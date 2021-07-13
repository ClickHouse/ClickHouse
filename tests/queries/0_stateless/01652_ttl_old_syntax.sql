DROP TABLE IF EXISTS ttl_old_syntax;

CREATE TABLE ttl_old_syntax (d Date, i Int) ENGINE = MergeTree(d, i, 8291);
ALTER TABLE ttl_old_syntax MODIFY TTL toDate('2020-01-01'); -- { serverError 36 }

DROP TABLE ttl_old_syntax;
