-- failover_test.sql

-- Create a local table on each node
CREATE TABLE local_table ON CLUSTER my_cluster
(
    column1 Int32,
    column2 String,
    column3 String
) ENGINE = MergeTree()
ORDER BY column1;

-- Create a distributed table that uses the local tables
CREATE TABLE distributed_table ON CLUSTER my_cluster
(
    column1 Int32,
    column2 String,
    column3 String
) ENGINE = Distributed(my_cluster, 'default', 'local_table', rand());

-- Insert some test data into the local table
INSERT INTO local_table VALUES (1, 'data1', 'more_data1');
INSERT INTO local_table VALUES (2, 'data2', 'more_data2');
INSERT INTO local_table VALUES (3, 'data3', 'more_data3');

-- Query data from the distributed table
SELECT * FROM distributed_table ORDER BY column1;

-- Simulate server unavailability
-- This should be done manually or via a separate script/command
-- E.g., stopping a ClickHouse server node

-- Perform another query to trigger the failover mechanism
SELECT * FROM distributed_table ORDER BY column1;
