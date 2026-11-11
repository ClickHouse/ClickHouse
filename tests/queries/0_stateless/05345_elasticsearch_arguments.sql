-- The storage opens no connection at creation time, so every check below runs without an Elasticsearch server.

SET enable_elasticsearch = 1;

SELECT * FROM elasticsearch('ftp://localhost:1', 'idx'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://user:pw@localhost:1', 'idx'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1/?x=1', 'idx'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', ''); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'a b'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', '_internal'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM elasticsearch('http://localhost:1', 'idx', password = 'x'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'a:b', 'pw'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', ' a', 'pw'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'pw', api_key = 'k'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', bearer_token = 't', api_key = 'k'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM elasticsearch('http://localhost:1', 'idx', query('not json')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', query('[1, 2]')); -- { serverError BAD_ARGUMENTS }

SELECT * FROM elasticsearch('http://localhost:1', 'idx', unknown_key = 'x'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', index = 42); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', index = 'a', index = 'b'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', index = 'other'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'pw', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM elasticsearch(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM elasticsearch('http://localhost:1'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM elasticsearch(no_such_collection); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

CREATE TABLE es_two_columns (a JSON, b JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE es_not_json (a String) ENGINE = Elasticsearch('http://localhost:1', 'idx'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE es_reserved_name (_id String) ENGINE = Elasticsearch('http://localhost:1', 'idx'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE es_alias_column (a JSON, b String ALIAS 'x') ENGINE = Elasticsearch('http://localhost:1', 'idx'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', structure = 'a String'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE es_zero_page (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx') SETTINGS page_size = 0; -- { serverError SETTING_CONSTRAINT_VIOLATION }
CREATE TABLE es_zero_keep_alive (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx') SETTINGS keep_alive = 0; -- { serverError SETTING_CONSTRAINT_VIOLATION }
CREATE TABLE es_unknown_setting (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx') SETTINGS no_such_setting = 1; -- { serverError UNKNOWN_SETTING }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', SETTINGS page_size = 0); -- { serverError SETTING_CONSTRAINT_VIOLATION }

DROP TABLE IF EXISTS es_args_ok;
CREATE TABLE es_args_ok (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', 'u', 'pw') SETTINGS page_size = 5;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 'es_args_ok';
DROP TABLE es_args_ok;
