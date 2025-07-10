-- Tags: no-parallel, no-ordinary-database
-- Tag no-parallel: static UUID
-- Tag no-ordinary-database: requires UUID

CREATE TABLE no_physical (a Int EPHEMERAL) Engine=Memory; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }
CREATE TABLE no_physical (a Int ALIAS 1) Engine=Memory; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }

CREATE TABLE no_insertable (a Int MATERIALIZED 1) Engine=Memory; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }
ATTACH TABLE no_insertable UUID '00000000-0000-0000-0000-000000000001' (a Int MATERIALIZED 1) Engine=Memory;

CREATE TABLE insertable (a Int EPHEMERAL, b Int MATERIALIZED 1) Engine=Memory;
ALTER TABLE insertable DROP COLUMN a; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }
