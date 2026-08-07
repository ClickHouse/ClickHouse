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
  * them (ambient session or profile values are left to the write-time checks), the definition is
  * a fresh one — a `CREATE`, or a full-definition `ATTACH` — and the format is Parquet. Replaying
  * an already-accepted definition (server startup, replicated or `ON CLUSTER` DDL replay,
  * `RESTORE` from backup, a short `ATTACH TABLE t`) is exempt, so existing tables always load.
  *
  * When the definition declares its columns, the header-dependent checks (unknown columns, full
  * coverage of the schema, ambiguous dotted paths) run as well; a definition relying on schema
  * inference gets only the header-independent checks at this point — the engine then reruns the
  * full validation through `validateParquetFieldIdSettingsAfterSchemaInference` once the schema
  * and format are resolved.
  *
  * Throws `BAD_ARGUMENTS` on an invalid definition. No-op when Parquet support is compiled out.
  */
void validateParquetFieldIdSettingsInDefinition(
    const StorageFactory::Arguments & args, const String & format_name, const FormatSettings & format_settings);

/** Second phase of the validation above, run after the engine has resolved its schema and format.
  * A definition without a column list (or with `format = 'auto'`) passes the first phase with only
  * the header-independent checks, because the engine infers the real Parquet header later during
  * `CREATE` (`StorageFile::setStorageMetadata`, the `IStorageURLBase` constructor,
  * `StorageObjectStorage::resolveSchemaAndFormat`). Once that header is known — it is the one the
  * engine freezes and every write will use — the header-dependent checks (unknown columns, full
  * coverage, ambiguous dotted paths) must run against it, or the definition would be accepted and
  * only fail on the first `INSERT`. The same definition-supplied / fresh-definition / Parquet
  * gates apply, so replayed definitions and ambient values are exempt exactly as in the first
  * phase. Call it only when the first phase ran without the final schema, i.e. when the
  * definition's column list was empty or its format was still `auto`.
  */
void validateParquetFieldIdSettingsAfterSchemaInference(
    const StorageFactory::Arguments & args,
    const String & resolved_format_name,
    const NamesAndTypesList & resolved_columns,
    const FormatSettings & format_settings);

}
