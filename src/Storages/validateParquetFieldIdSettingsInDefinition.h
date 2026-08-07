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
  * inference gets only the header-independent checks.
  *
  * Throws `BAD_ARGUMENTS` on an invalid definition. No-op when Parquet support is compiled out.
  */
void validateParquetFieldIdSettingsInDefinition(
    const StorageFactory::Arguments & args, const String & format_name, const FormatSettings & format_settings);

}
