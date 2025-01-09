-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on OpenSSL
-- Tag no-parallel: Messes with internal cache

-- Test for issue #64136

SYSTEM DROP QUERY CACHE;

DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS db2;

CREATE DATABASE db1;
CREATE DATABASE db2;

CREATE TABLE db1.tab(a UInt64, PRIMARY KEY a);
CREATE TABLE db2.tab(a UInt64, PRIMARY KEY a);

INSERT INTO db1.tab values(1);
INSERT INTO db2.tab values(2);

USE db1;
SELECT * FROM tab SETTINGS use_query_cache=1;

USE db2;
SELECT * FROM tab SETTINGS use_query_cache=1;

DROP DATABASE db1;
DROP DATABASE db2;

SYSTEM DROP QUERY CACHE;
