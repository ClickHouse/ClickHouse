-- `UUID` and `UUID2` are the same logical guid type, so a typed Kusto `guid` parameter accepts either of them,
-- including a column declared as `UUID` under `uuid_type_version = 2`, which is stored as `UUID2`.

DROP TABLE IF EXISTS kql_guid_uuid2;
CREATE TABLE kql_guid_uuid2 (g UUID, gn Nullable(UUID), glc LowCardinality(UUID), u UUID1) ENGINE = Memory SETTINGS uuid_type_version = 2;
INSERT INTO kql_guid_uuid2 VALUES ('74be27de-1e4e-49d9-b579-fe0b331d3642', '74be27de-1e4e-49d9-b579-fe0b331d3642', '74be27de-1e4e-49d9-b579-fe0b331d3642', '74be27de-1e4e-49d9-b579-fe0b331d3642');
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'kql_guid_uuid2' ORDER BY name;

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- An explicit `UUID2` argument passes through and keeps its flavor: no cast between the two layouts is inserted.
let F = (g:guid) { g }; print F(toUUID2('74be27de-1e4e-49d9-b579-fe0b331d3642'));
let F = (g:guid) { toTypeName(g) }; print F(toUUID2('74be27de-1e4e-49d9-b579-fe0b331d3642'));
let F = (g:guid) { toTypeName(g) }; print F(guid(74be27de-1e4e-49d9-b579-fe0b331d3642));

-- `Nullable` and `LowCardinality` wrappers around `UUID2` are accepted the same way as around `UUID`.
let F = (g:guid) { g }; print F(toNullable(toUUID2('74be27de-1e4e-49d9-b579-fe0b331d3642')));
let F = (g:guid) { toTypeName(g) }; print F(toNullable(toUUID2('74be27de-1e4e-49d9-b579-fe0b331d3642')));
let F = (g:guid) { isnull(g) }; print F(toUUID2OrNull('not a guid'));
let F = (g:guid) { g }; print F(toLowCardinality(toUUID2('74be27de-1e4e-49d9-b579-fe0b331d3642')));

-- The columns of a table created under `uuid_type_version = 2` are accepted, whichever flavor they are stored as.
let F = (g:guid) { g }; kql_guid_uuid2 | project F(g), F(gn), F(glc), F(u);
let F = (g:guid) { toTypeName(g) }; kql_guid_uuid2 | project F(g), F(gn), F(glc), F(u);

-- A non-guid argument is still rejected.
let F = (g:guid) { g }; print F('74be27de-1e4e-49d9-b579-fe0b331d3642'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (g:guid) { g }; print F(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET dialect = 'clickhouse';
DROP TABLE kql_guid_uuid2;
