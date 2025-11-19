-- Tags: no-ordinary-database, no-fasttest, no-encrypted-storage
DROP TABLE IF EXISTS landing_to_target;
DROP TABLE IF EXISTS target;
DROP TABLE IF EXISTS landing;

CREATE TABLE landing (n Int64) engine=MergeTree order by n;
CREATE TABLE target  (n Int64) engine=MergeTree order by n;
CREATE MATERIALIZED VIEW landing_to_target TO target AS
    SELECT n + throwIf(n == 3333) AS n
    FROM landing;

-- There is no gurantee what is inserted in case if exception
-- That initial check is meaninglessness
-- We only sure that if the internal mv's exception is ignored then all the data is inserted to the table landing
INSERT INTO landing SELECT * FROM numbers(10000) SETTINGS materialized_views_ignore_errors=1;
SELECT 'no_transaction_landing', count() = 10000 FROM landing;
SELECT 'no_transaction_target', count() < 10000 FROM target;

TRUNCATE TABLE landing;
TRUNCATE TABLE target;


BEGIN TRANSACTION;
INSERT INTO landing SELECT * FROM numbers(10000); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
ROLLBACK;
SELECT 'after_transaction_landing', count() FROM landing;
SELECT 'after_transaction_target', count() FROM target;

-- Same but using implicit_transaction
INSERT INTO landing SETTINGS implicit_transaction=True SELECT * FROM numbers(10000); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'after_implicit_txn_in_query_settings_landing', count() FROM landing;
SELECT 'after_implicit_txn_in_query_settings_target', count() FROM target;

-- Same but using implicit_transaction in a session
SET implicit_transaction=True;
INSERT INTO landing SELECT * FROM numbers(10000); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SET implicit_transaction=False;
SELECT 'after_implicit_txn_in_session_landing', count() FROM landing;
SELECT 'after_implicit_txn_in_session_target', count() FROM target;

-- Reading from incompatible sources with implicit_transaction works the same way as with normal transactions:
-- Currently reading from system tables inside a transaction is Not implemented:
SELECT name, value, changed FROM system.settings where name = 'implicit_transaction' SETTINGS implicit_transaction=True; -- { serverError NOT_IMPLEMENTED }


-- Verify that you don't have to manually close transactions with implicit_transaction
SET implicit_transaction=True;
SELECT throwIf(number == 0) FROM numbers(100); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(number == 0) FROM numbers(100); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(number == 0) FROM numbers(100); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(number == 0) FROM numbers(100); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SET implicit_transaction=False;

-- implicit_transaction is ignored when inside a transaction (no recursive transaction error)
BEGIN TRANSACTION;
SELECT 'inside_txn_and_implicit', 1 SETTINGS implicit_transaction=True;
SELECT throwIf(number == 0) FROM numbers(100) SETTINGS implicit_transaction=True; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
ROLLBACK;

SELECT 'inside_txn_and_implicit', 1 SETTINGS implicit_transaction=True;

-- You can work with transactions even if `implicit_transaction=True` is set
SET implicit_transaction=True;
BEGIN TRANSACTION;
INSERT INTO target SELECT * FROM numbers(10000);
SELECT 'in_transaction', count() FROM target;
ROLLBACK;
SELECT 'out_transaction', count() FROM target;
SET implicit_transaction=False;


-- Verify that the transaction_id column is populated correctly
SELECT 'Looking_at_transaction_id_True' FORMAT Null SETTINGS implicit_transaction=1;
-- Verify that the transaction_id column is NOT populated without transaction
SELECT 'Looking_at_transaction_id_False' FORMAT Null SETTINGS implicit_transaction=0;
SYSTEM FLUSH LOGS query_log;

SELECT
    'implicit_True',
    count() as all,
    transaction_id = (0,0,'00000000-0000-0000-0000-000000000000') as is_empty
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    event_date >= yesterday() AND
    query LIKE '-- Verify that the transaction_id column is populated correctly%'
GROUP BY transaction_id
FORMAT JSONEachRow;

SELECT
    'implicit_False',
    count() as all,
    transaction_id = (0,0,'00000000-0000-0000-0000-000000000000') as is_empty
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    event_date >= yesterday() AND
    query LIKE '-- Verify that the transaction_id column is NOT populated without transaction%'
GROUP BY transaction_id
FORMAT JSONEachRow;

SET implicit_transaction=1;
SET throw_on_unsupported_query_inside_transaction=1;
SELECT * FROM system.one;
SELECT * FROM cluster('test_cluster_interserver_secret', system, one);  -- { serverError NOT_IMPLEMENTED }
SELECT * FROM cluster('test_cluster_two_shards', system, one);  -- { serverError NOT_IMPLEMENTED }
SET throw_on_unsupported_query_inside_transaction=0;
-- there's not session in the interserver mode
SELECT * FROM cluster('test_cluster_interserver_secret', system, one) FORMAT Null;  -- { serverError INVALID_TRANSACTION }
SELECT * FROM cluster('test_cluster_two_shards', system, one);
