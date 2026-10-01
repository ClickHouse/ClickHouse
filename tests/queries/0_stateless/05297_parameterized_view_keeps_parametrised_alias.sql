-- A parametrised alias is formatted, so a parameterized view keeps it in its stored definition.
DROP VIEW IF EXISTS v_parametrised_alias;
CREATE VIEW v_parametrised_alias AS SELECT number AS {name:Identifier} FROM numbers(3) WHERE number < {lim:UInt8};
SELECT * FROM v_parametrised_alias(name = 'abc', lim = 2) FORMAT TSVWithNames;
SELECT replaceRegexpOne(create_table_query, '^CREATE VIEW \\S+ ', 'CREATE VIEW ') FROM system.tables WHERE database = currentDatabase() AND name = 'v_parametrised_alias';
DROP VIEW v_parametrised_alias;
