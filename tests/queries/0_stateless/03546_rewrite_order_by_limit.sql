-- { echo ON }
drop table if exists a;
drop table if exists b;
-- create table
CREATE TABLE a (UserID UInt32, EventDate Date, Amount Float64, Device String) ENGINE = MergeTree() ORDER BY (UserID, EventDate) PARTITION BY toYYYYMM(EventDate);
CREATE TABLE b (OrderID UInt64, UserID UInt32, Product String, Quantity UInt16, OrderTime DateTime) ENGINE = MergeTree() ORDER BY (OrderID, UserID) PARTITION BY toYYYYMM(OrderTime);

-- prepare data
INSERT INTO a VALUES (1, '2023-01-01', 100.5, 'Mobile'), (2, '2023-01-02', 200.3, 'Desktop'), (3, '2023-01-03', 50.7, 'Tablet'), (4, '2023-01-04', 300.0, 'Mobile'), (5, '2023-01-05', 150.2, 'Desktop'), (6, '2023-02-10', 88.9, 'Mobile'), (7, '2023-02-15', 420.8, 'Tablet'), (8, '2023-03-01', 75.0, 'Desktop'), (9, '2023-03-05', 999.9, 'Mobile'), (10, '2023-03-10', 240.6, 'Tablet');
INSERT INTO b VALUES (1001, 1, 'Laptop', 2, '2023-01-01 10:00:00'), (1002, 2, 'Phone', 1, '2023-01-02 11:00:00'), (1003, 3, 'Monitor', 3, '2023-01-03 12:00:00'), (1004, 4, 'Keyboard', 4, '2023-01-04 13:00:00'), (1005, 5, 'Mouse', 2, '2023-01-05 14:00:00'), (1006, 6, 'Printer', 1, '2023-02-10 15:00:00'), (1007, 7, 'SSD', 5, '2023-02-15 16:00:00'), (1008, 8, 'RAM', 3, '2023-03-01 17:00:00'), (1009, 9, 'GPU', 2, '2023-03-05 18:00:00'), (1010, 10, 'CPU', 4, '2023-03-10 19:00:00');

-- 1. test simple order by limit(has rewrite)
SELECT * FROM a ORDER BY UserID DESC LIMIT 3 settings query_plan_rewrite_order_by_limit=1;
-- explain already rewritten
explain syntax run_query_tree_passes = 1 SELECT * FROM a ORDER BY Amount DESC LIMIT 3 settings query_plan_rewrite_order_by_limit=1;
-- explain no rewritten
explain syntax run_query_tree_passes = 1 SELECT * FROM a ORDER BY Amount DESC LIMIT 3 settings query_plan_rewrite_order_by_limit=0;

-- 2. test order by limit in join left table(has rewrite)
SELECT * FROM (SELECT * FROM a ORDER BY Amount DESC LIMIT 5) AS a LEFT JOIN b ON a.UserID = b.UserID ORDER BY a.UserID settings query_plan_rewrite_order_by_limit=1;
explain syntax run_query_tree_passes = 1 SELECT * FROM (SELECT * FROM a ORDER BY Amount DESC LIMIT 5) AS a LEFT JOIN b ON a.UserID = b.UserID settings query_plan_rewrite_order_by_limit=1;
explain syntax run_query_tree_passes = 1 SELECT * FROM (SELECT * FROM a ORDER BY Amount DESC LIMIT 5) AS a LEFT JOIN b ON a.UserID = b.UserID settings query_plan_rewrite_order_by_limit=0;

-- 3. test order by limit in join right table(has rewrite)
SELECT * FROM a LEFT JOIN (SELECT * FROM b ORDER BY Quantity DESC LIMIT 5) AS b ON a.UserID = b.UserID ORDER BY a.UserID settings query_plan_rewrite_order_by_limit=1;
explain syntax run_query_tree_passes = 1 SELECT * FROM a LEFT JOIN (SELECT * FROM b ORDER BY Quantity DESC LIMIT 5) AS b ON a.UserID = b.UserID settings query_plan_rewrite_order_by_limit=1;
explain syntax run_query_tree_passes = 1 SELECT * FROM a LEFT JOIN (SELECT * FROM b ORDER BY Quantity DESC LIMIT 5) AS b ON a.UserID = b.UserID settings query_plan_rewrite_order_by_limit=0;

-- 4. test order by limit in the entire join table(no rewrite)
SELECT * FROM a LEFT JOIN b ON a.UserID = b.UserID ORDER BY a.UserID DESC LIMIT 5 settings query_plan_rewrite_order_by_limit=1;
explain syntax run_query_tree_passes = 1 SELECT * FROM a LEFT JOIN b ON a.UserID = b.UserID ORDER BY a.Amount DESC LIMIT 5 settings query_plan_rewrite_order_by_limit=1;
explain syntax run_query_tree_passes = 1 SELECT * FROM a LEFT JOIN b ON a.UserID = b.UserID ORDER BY a.Amount DESC LIMIT 5 settings query_plan_rewrite_order_by_limit=0;

-- 5. test order by limit with prewhere
SELECT * FROM a PREWHERE UserID>1 ORDER BY UserID DESC LIMIT 3 settings query_plan_rewrite_order_by_limit=1;
explain syntax SELECT * FROM a PREWHERE UserID>1 ORDER BY Amount DESC LIMIT 3 settings query_plan_rewrite_order_by_limit=1;
explain syntax SELECT * FROM a PREWHERE UserID>1 ORDER BY Amount DESC LIMIT 3 settings query_plan_rewrite_order_by_limit=0;

drop table a;
drop table b;
