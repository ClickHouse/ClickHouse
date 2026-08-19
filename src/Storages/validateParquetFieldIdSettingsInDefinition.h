#pragma once

#include <Formats/FormatSettings.h>
#include <Storages/StorageFactory.h>

namespace DB
{

/** Table engines that write files (`File`, `URL`, `S3` and the other object-storage engines)
  * freeze their `FormatSettings` from the `CREATE TABLE ... SETTINGS` clause, so an invalid
  * `output_format_parquet_column_field_ids` map supplied in the definition would be accepted at
  * `CREATE` time and then fail every later `INSERT`, leaving a table that can never be written.
  * This validates the Parquet `field_id` settings up front when the definition itself supplies
  * them (an ambient session or profile value never reaches a table definition — see
  * `getFormatSettingsForTableDefinition`), the definition is a fresh one — a `CREATE`, or a
  * full-definition `ATTACH` — and the format is Parquet. Replaying
  * an already-accepted definition (server startup, replicated or `ON CLUSTER` DDL replay,
  * `RESTORE` from backup, a short `ATTACH TABLE t`) is exempt, so existing tables always load.
  *
  * When the definition declares its columns and `definition_columns_match_writer_header` is true,
  * the header-dependent checks (unknown columns, full coverage of the schema, ambiguous dotted
  * paths) run as well. Pass `definition_columns_match_writer_header = false` when the engine may
  * write a header that differs from the declared column list — an object-storage table with a
  * `PARTITION BY` clause, whose `hive` partition strategy keeps the partition columns out of the
  * data file unless `partition_columns_in_data_file` is enabled. A definition relying on schema
  * inference likewise gets only the header-independent checks at this point. In both cases the
  * engine reruns the full validation through `validateParquetFieldIdSettingsWithResolvedHeader`
  * once the real writer header is known.
  *
  * Throws `BAD_ARGUMENTS` on an invalid definition. No-op when Parquet support is compiled out.
  */
void validateParquetFieldIdSettingsInDefinition(
    const StorageFactory::Arguments & args,
    const String & format_name,
    const FormatSettings & format_settings,
    bool definition_columns_match_writer_header = true);

/** Second phase of the validation above, run once the engine knows the header the Parquet writer
  * will actually receive. A definition without a column list (or with `format = 'auto'`) passes
  * the first phase with only the header-independent checks, because the engine infers the real
  * schema later during `CREATE` (`StorageFile::setStorageMetadata`, the `IStorageURLBase`
  * constructor, `StorageObjectStorage::resolveSchemaAndFormat`); a partitioned object-storage
  * definition passes it the same way, because its partition strategy may drop the partition
  * columns from the written file (`HiveStylePartitionStrategy::getFormatHeader` with
  * `partition_columns_in_data_file = 0`). Once the writer header is known — the resolved physical
  * columns, or the partition strategy's format header — the header-dependent checks (unknown
  * columns, full coverage, ambiguous dotted paths) must run against it, or the definition would
  * be accepted and only fail on the first `INSERT`. The same definition-supplied /
 * fresh-definition / Parquet gates apply, so replayed definitions and ambient values are exempt
 * exactly as in the first phase. Set `validate_secondary_create` for a node-local source whose
 * resolved header can differ on a distributed DDL worker: unlike a metadata replay, that worker
 * must validate its own header before accepting the table. Call it only when the first phase ran
 * without the final writer
  * header, i.e. when the definition's column list was empty, its format was still `auto`, or the
  * table is partitioned.
  */
void validateParquetFieldIdSettingsWithResolvedHeader(
    const StorageFactory::Arguments & args,
    const String & resolved_format_name,
    const NamesAndTypesList & writer_header_columns,
    const FormatSettings & format_settings,
    bool validate_secondary_create = false);

}
