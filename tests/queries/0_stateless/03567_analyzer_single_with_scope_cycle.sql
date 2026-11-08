create table tab (x String, y UInt8) engine = MergeTree order by tuple();
insert into tab select 'rue', 1;

WITH
    a as b
SELECT 1 FROM (
    WITH b as c 
    SELECT 1 FROM (
        WITH c as d
        SELECT 1 FROM (
            SELECT 1 FROM tab WHERE e = 'true'
        )
    )
)
SETTINGS allow_experimental_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }

SET enable_scopes_for_with_statement = 0;

WITH
    a as b
SELECT 1 FROM (
    WITH b as c 
    SELECT 1 FROM (
        WITH c as d
        SELECT 1 FROM (
            SELECT 1 FROM tab WHERE e = 'true'
        )
    )
)
SETTINGS allow_experimental_analyzer = 1; -- { serverError UNKNOWN_IDENTIFIER }
