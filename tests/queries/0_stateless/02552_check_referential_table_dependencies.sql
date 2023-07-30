DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (x UInt8) ENGINE = Memory;
CREATE TABLE dst (x UInt8) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM src;

SET check_referential_table_dependencies = 1;

-- Can't drop because of referential dependencies
DROP TABLE src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dst; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- Ok to drop in the correct order
DROP TABLE mv;
DROP TABLE src;
DROP TABLE dst;

-- Check again with check_referential_table_dependencies = 0
CREATE TABLE src (x UInt8) ENGINE = Memory;
CREATE TABLE dst (x UInt8) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM src;

SET check_referential_table_dependencies = 0;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE mv;
