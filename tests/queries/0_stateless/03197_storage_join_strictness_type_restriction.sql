DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(SEMI, ALL, a); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(SEMI, INNER, a); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(SEMI, OUTER, a); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(ANTI, ALL, a); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(ANTI, INNER, a); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(ANTI, OUTER, a); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1
(
    a Int64,
    b Int64
) Engine = Join(ANY, OUTER, a); -- { serverError BAD_ARGUMENTS }
