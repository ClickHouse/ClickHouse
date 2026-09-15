# Databricks UniForm Iceberg v3 + Delta `.bin` deletion vectors

Customer-provided table (`altinity_dv_puffin_repro_v3.zip`). Databricks UniForm writes Iceberg
metadata under `_iceberg/metadata/` and data / deletion vectors at the table root.

- Schema: `Id String`, `Name String`, `UpdatedAt timestamptz`
- 10000 rows (`Id` `"1"` .. `"10000"`)
- `DELETE WHERE cast(Id as int) <= 1000` → 1000 positions in `deletion_vector_*.bin`
- Delete-manifest entry: `file_format = PUFFIN`, `content_offset = 1`, `content_size_in_bytes = 251`
- Object header is Delta version `0x01`, not Puffin `PFA1`

The integration test copies this tree, flattens `_iceberg/metadata` to `metadata/`, and rewrites
Databricks `s3://` URIs onto the ClickHouse test warehouse path.
