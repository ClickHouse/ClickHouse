--- Tests for Github issue 88306

CREATE TABLE tab(col Int) ENGINE = MergeTree() ORDER BY tuple() TTL greater(materialize(2), 1); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab(col Int TTL (1 AS alias)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab(col Int TTL (1 AS alias)) ENGINE = ReplacingMergeTree ORDER BY col; -- { serverError BAD_ARGUMENTS }
