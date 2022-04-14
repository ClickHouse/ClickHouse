SET compatibility_ignore_collation_in_create_table=false;
CREATE TABLE t_02267_collation (x varchar(255) COLLATION utf8_unicode_ci NOT NULL) ENGINE = Memory; --{serverError 49}

SET compatibility_ignore_collation_in_create_table=false;
CREATE TABLE t_02267_collation (x varchar(255) COLLATION utf8_unicode_ci NOT NULL) ENGINE = Memory;

DROP TABLE t_02267_collation;
