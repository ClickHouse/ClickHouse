-- Ensure that all nodes are running before starting this test

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
INSERT INTO distributed_table VALUES (1, 'data1', 'more_data1');
INSERT INTO distributed_table VALUES (2, 'data2', 'more_data2');
INSERT INTO distributed_table VALUES (3, 'data3', 'more_data3');

-- Query data from the distributed table before simulating failover
SELECT * FROM distributed_table ORDER BY column1;

-- Simulate server unavailability by stopping a ClickHouse server node
-- Note: This part is commented out and should be executed manually or via a script during testing
-- sudo systemctl stop clickhouse-server

-- Perform another query to trigger the failover mechanism
-- This query should trigger the failover if the node is down
SELECT * FROM distributed_table ORDER BY column1;

-- Restore server availability
-- Note: This part is commented out and should be executed manually or via a script during testing
-- sudo systemctl start clickhouse-server

-- Query data again to ensure the failover mechanism worked
SELECT * FROM distributed_table ORDER BY column1;
