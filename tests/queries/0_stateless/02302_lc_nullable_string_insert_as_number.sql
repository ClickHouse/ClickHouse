DROP TABLE IF EXISTS lc_nullable_string;

CREATE TABLE lc_nullable_string(`c1` LowCardinality(Nullable(String)) DEFAULT CAST(NULL, 'LowCardinality(Nullable(String))')) 
ENGINE = Memory;

INSERT INTO lc_nullable_string (c1) FORMAT Values (0);

SELECT * FROM lc_nullable_string;

DROP TABLE lc_nullable_string;
