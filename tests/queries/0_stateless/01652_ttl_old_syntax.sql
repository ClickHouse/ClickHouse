DROP TABLE IF EXISTS ttl_old_syntax;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE ttl_old_syntax (d Date, i Int) ENGINE = MergeTree(d, i, 8291);
ALTER TABLE ttl_old_syntax MODIFY TTL toDate('2020-01-01'); -- { serverError BAD_ARGUMENTS }

DROP TABLE ttl_old_syntax;
