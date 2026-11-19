create table distributor (id String, name String) Engine = MergeTree() order by id;
create table product (id String, name String) Engine = MergeTree() order by id;
create table sales (
    id String,
    distributor String,
    product String,
    amount Float32
) Engine = MergeTree() order by id;
SELECT * FROM
  view(
    SELECT * FROM sales
    LEFT JOIN distributor ON distributor.id = sales.distributor
  ) AS newSales
LEFT JOIN product ON product.id = newSales.product SETTINGS enable_analyzer=1;
