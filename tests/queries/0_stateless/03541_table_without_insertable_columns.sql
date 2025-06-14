CREATE TABLE no_physical (a Int EPHEMERAL) Engine=Memory; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }
CREATE TABLE no_physical (a Int ALIAS 1) Engine=Memory; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }

CREATE TABLE no_insertable (a Int MATERIALIZED 1) Engine=Memory; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }

CREATE TABLE no_insertable (a Int EPHEMERAL, b Int MATERIALIZED a+1) Engine=Memory;
