EXPLAIN SYNTAX CREATE TABLE t (x varchar(255) COLLATE binary NOT NULL) ENGINE=Memory;

EXPLAIN SYNTAX CREATE TABLE t (x varchar(255) COLLATE NOT NULL) ENGINE=Memory; -- {clientError SYNTAX_ERROR}
EXPLAIN SYNTAX CREATE TABLE t (x varchar(255) COLLATE NULL) ENGINE=Memory; -- {clientError SYNTAX_ERROR}
EXPLAIN SYNTAX CREATE TABLE t (x varchar(255) COLLATE something_else NOT NULL) ENGINE=Memory; -- {clientError SYNTAX_ERROR}

SET compatibility_ignore_collation_in_create_table=false;
CREATE TABLE t_02267_collation (x varchar(255) COLLATE utf8_unicode_ci NOT NULL) ENGINE = Memory; -- {serverError NOT_IMPLEMENTED}

SET compatibility_ignore_collation_in_create_table=true;
CREATE TABLE t_02267_collation (x varchar(255) COLLATE utf8_unicode_ci NOT NULL) ENGINE = Memory;

DROP TABLE t_02267_collation;
