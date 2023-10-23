-- Overview Assignment 1
-- Dataset The Look Ecoomenerce
-- in Google BigQuery

-- Task
-- 1. user count comparison
SELECT
  gender AS jk,
  COUNT(gender) AS jumlah_jk
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY jk;

-- 2. user count comparison in 2023
SELECT
  t2.gender AS jk,
  COUNT(t2.gender) AS jumlah_jk
FROM `bigquery-public-data.thelook_ecommerce.order_items` AS t1
JOIN `bigquery-public-data.thelook_ecommerce.users` AS t2
ON t1.user_id = t2.id
WHERE EXTRACT(YEAR FROM t1.created_at) = 2023
GROUP BY jk;

-- 3. number of age comparisons 
SELECT
  gender AS jk,
  age AS umur,
  COUNT(age) AS j_umur  
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY age, gender
ORDER BY j_umur DESC;

-- 4. number of users enrolled by gender and country of origin in 2023
SELECT
  gender AS jk,
  country AS negara,
  COUNT(id) AS jumlah_user
FROM `bigquery-public-data.thelook_ecommerce.users`
WHERE EXTRACT(YEAR FROM created_at) = 2023
GROUP BY gender, negara
ORDER BY jumlah_user DESC;

-- 5. traffic sources and volumes
SELECT
  traffic_source AS sumber,
  COUNT(traffic_source) AS jumlah_sumber
FROM `bigquery-public-data.thelook_ecommerce.events`
WHERE EXTRACT(YEAR FROM created_at) = 2023
GROUP BY sumber
ORDER BY jumlah_sumber DESC;

-- 6. traffic sources and volumes by gender
SELECT
  t1.traffic_source AS sumber,
  COUNT(t1.traffic_source) AS jumlah_sumber,
  -- COUNT(t2.gender) AS jumlah_jk,
  t2.gender AS jk
FROM `bigquery-public-data.thelook_ecommerce.events` AS t1
JOIN `bigquery-public-data.thelook_ecommerce.users` AS t2
ON t1.user_id = t2.id
WHERE EXTRACT(YEAR FROM t1.created_at) = 2023
GROUP BY sumber, t2.gender
ORDER BY sumber, jk DESC;

-- 7. know the countries and number of countries that visited in 2023
SELECT
  state AS negara,
  COUNT(state) AS jumlah_user
FROM `bigquery-public-data.thelook_ecommerce.events`
WHERE EXTRACT(YEAR FROM created_at) = 2023
GROUP BY negara
ORDER BY jumlah_user DESC
LIMIT 100;

-- 8. find out the highest item sales and average purchase per 100 people with the highest purchase in 2023
SELECT
  user_id AS user,
  SUM(num_of_item) AS total_pembelian,
  ROUND(AVG(num_of_item)) AS rata_rata_pembelian
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE EXTRACT(YEAR FROM created_at) = 2023
GROUP BY user_id
ORDER BY total_pembelian DESC
LIMIT 100;

-- 9. know the details of goods and the selling price of goods in 2023
SELECT
  sku,
  name AS nama_item,
  COUNT(order_id) AS jumlah_penjualan,
  ROUND(AVG(retail_price), 2) AS harga_per_item,
  ROUND(SUM(retail_price), 2) AS jumlah_harga_penjualan
FROM `bigquery-public-data.thelook_ecommerce.products` AS t1
JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS t2
ON t1.id = t2.product_id
WHERE EXTRACT(YEAR FROM created_at) = 2023
GROUP BY sku, nama_item
ORDER BY jumlah_harga_penjualan DESC -- it can change to harga_per_item
LIMIT 100;

-- 10. find out the age range, gender, category of goods purchased, and total sales above 500 pcs in 2023.
SELECT
  CASE
    WHEN t1.age BETWEEN 11 AND 20 THEN '11-20'
    WHEN t1.age BETWEEN 21 AND 30 THEN '21-30'
    WHEN t1.age BETWEEN 31 AND 40 THEN '31-40'
    WHEN t1.age BETWEEN 41 AND 50 THEN '41-50'
    WHEN t1.age BETWEEN 51 AND 60 THEN '51-60'
    ELSE 'Lainnya'
  END AS rentang_umur,

  t1.gender AS jk,
  t3.category AS kategori,
  COUNT(t3.category) AS jumlah_penjualan

FROM `bigquery-public-data.thelook_ecommerce.users` AS t1
JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS t2
ON t1.id = t2.user_id
JOIN `bigquery-public-data.thelook_ecommerce.products` AS t3
ON t2.product_id = t3.id
WHERE EXTRACT(YEAR FROM t2.created_at) = 2023
GROUP BY rentang_umur, jk, kategori
HAVING jumlah_penjualan > 500
ORDER BY rentang_umur ASC, jumlah_penjualan DESC;

-- 11. from the previous case no, it is known that men buy more goods than women, here the analyst wants to know the trend of goods purchased by both men and women for the 5 most frequently purchased items, price variants, and also the preferences of goods purchased by both men and women in 2023.
WITH PembeliWanita AS (
  SELECT
    t1.gender AS jk,
    t3.category AS kategori,
    COUNT(t3.name) AS j_barang,
    t3.name AS nama_barang,
    ROUND(t3.retail_price, 2) AS harga_barang
  FROM `bigquery-public-data.thelook_ecommerce.users` AS t1
  JOIN`bigquery-public-data.thelook_ecommerce.order_items` AS t2
  ON t1.id = t2.user_id
  JOIN `bigquery-public-data.thelook_ecommerce.products` AS t3
  ON t2.product_id = t3.id
  WHERE
    EXTRACT(YEAR FROM t2.created_at) = 2023
    AND t1.gender LIKE 'F%'
  GROUP BY jk, kategori, t3.name, nama_barang, harga_barang
  ORDER BY j_barang DESC
  LIMIT 5
),

PembeliPria AS (
  SELECT
    t1.gender AS jk,
    t3.category AS kategori,
    COUNT(t3.name) AS j_barang,
    t3.name AS nama_barang,
    ROUND(t3.retail_price, 2) AS harga_barang
  FROM `bigquery-public-data.thelook_ecommerce.users` AS t1
  JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS t2
  ON t1.id = t2.user_id
  JOIN `bigquery-public-data.thelook_ecommerce.products` AS t3
  ON t2.product_id = t3.id
  WHERE
    EXTRACT(YEAR FROM t2.created_at) = 2023
    AND t1.gender LIKE 'M%'
  GROUP BY jk, kategori, t3.name, nama_barang, harga_barang
  ORDER BY j_barang DESC
  LIMIT 5
)

SELECT * FROM PembeliWanita
UNION ALL
SELECT * FROM PembeliPria
ORDER BY j_barang DESC, jk;
