-- Tags: no-fasttest

SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', NULL, 'my_collection', 'test_user', 'password', 'x Int32');  -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', NULL, 'test_user', 'password', 'x Int32');  -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', NULL, 'password', 'x Int32');  -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', 'test_user', NULL, 'x Int32');  -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', 'test_user', 'password', NULL); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', 'test_user', 'password', materialize(1) + 1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', 'test_user', 'password', 'x Int32', NULL); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', 'test_user', 'password', NULL, 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('mongodb://some-cluster:27017/?retryWrites=false', 'test', 'my_collection', 'test_user', 'password', NULL, 'x Int32'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb(NULL, 'test', 'my_collection', 'test_user', 'password', 'x Int32');  -- { serverError BAD_ARGUMENTS }

CREATE TABLE IF NOT EXISTS store_version ( `_id` String ) ENGINE = MongoDB(`localhost:27017`, mongodb, storeinfo, adminUser, adminUser); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }
