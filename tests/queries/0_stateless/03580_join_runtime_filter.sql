CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;
CREATE TABLE orders(o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2)) ENGINE MergeTree ORDER BY o_orderkey;

INSERT INTO nation VALUES (0,'ALGERIA'),(1,'ARGENTINA'),(2,'BRAZIL'),(3,'CANADA'),(4,'EGYPT'),(5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY'),(8,'INDIA'),(9,'INDONESIA'),(10,'IRAN'),(11,'IRAQ'),(12,'JAPAN'),(13,'JORDAN'),(14,'KENYA'),(15,'MOROCCO'),(16,'MOZAMBIQUE'),(17,'PERU'),(18,'CHINA'),(19,'ROMANIA'),(20,'SAUDI ARABIA'),(21,'VIETNAM'),(22,'UNITED KINGDOM'),(23,'UNITED STATES');

INSERT INTO customer SELECT number, number%23 FROM numbers(1000);

INSERT INTO orders SELECT number, number/1000, number%1000 FROM numbers(1000000);

SELECT avg(o_totalprice)
FROM orders, customer, nation
WHERE c_custkey = o_custkey AND c_nationkey=n_nationkey AND n_name = 'FRANCE'
SETTINGS enable_join_runtime_filters=1;
