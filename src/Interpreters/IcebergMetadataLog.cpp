#include <Access/ContextAccess.h>
#include <Common/SystemTableDocumentation.h>
#include <Core/Settings.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IcebergMetadataLog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Common/DateLUTImpl.h>
#include <Common/ErrnoException.h>
#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace Setting
{
extern const SettingsIcebergMetadataLogLevel iceberg_metadata_log_level;
}

namespace ErrorCodes
{
extern const int CANNOT_CLOCK_GETTIME;
extern const int BAD_ARGUMENTS;
}

namespace
{

const DataTypePtr rowType = makeNullable(std::make_shared<DataTypeUInt64>());

auto iceberg_pruning_status_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
    {"NotPruned", static_cast<Int8>(Iceberg::PruningReturnStatus::NOT_PRUNED)},
    {"PartitionPruned", static_cast<Int8>(Iceberg::PruningReturnStatus::PARTITION_PRUNED)},
    {"MinMaxIndexPruned", static_cast<Int8>(Iceberg::PruningReturnStatus::MIN_MAX_INDEX_PRUNED)}});

const DataTypePtr iceberg_pruning_status_datatype_nullable = makeNullable(iceberg_pruning_status_datatype);
}

ColumnsDescription IcebergMetadataLogElement::getColumnsDescription()
{
    auto iceberg_metadata_log_entry_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"None", static_cast<Int8>(IcebergMetadataLogLevel::None)},
        {"Metadata", static_cast<Int8>(IcebergMetadataLogLevel::Metadata)},
        {"ManifestListMetadata", static_cast<Int8>(IcebergMetadataLogLevel::ManifestListMetadata)},
        {"ManifestListEntry", static_cast<Int8>(IcebergMetadataLogLevel::ManifestListEntry)},
        {"ManifestFileMetadata", static_cast<Int8>(IcebergMetadataLogLevel::ManifestFileMetadata)},
        {"ManifestFileEntry", static_cast<Int8>(IcebergMetadataLogLevel::ManifestFileEntry)}});

    return ColumnsDescription{
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"query_id", std::make_shared<DataTypeString>(), "Query id."},
        {"content_type", std::move(iceberg_metadata_log_entry_datatype), "Content type."},
        {"table_path", std::make_shared<DataTypeString>(), "Table path."},
        {"file_path", std::make_shared<DataTypeString>(), "File path."},
        {"content", std::make_shared<DataTypeString>(), "Content in a JSON format (json file content, avro metadata or avro entry)."},
        {"row_in_file", rowType, "Row in file."},
        {"pruning_status", iceberg_pruning_status_datatype_nullable, "Status of partition pruning or min-max index pruning for the file."}};
}

void IcebergMetadataLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_index = 0;
    columns[column_index++]->insert(getFQDNOrHostName());
    columns[column_index++]->insert(DateLUT::instance().toDayNum(current_time).toUnderType());
    columns[column_index++]->insert(current_time);
    columns[column_index++]->insert(query_id);
    columns[column_index++]->insert(content_type);
    columns[column_index++]->insert(table_path);
    columns[column_index++]->insert(file_path);
    columns[column_index++]->insert(metadata_content);
    columns[column_index++]->insert(row_in_file ? *row_in_file : rowType->getDefault());
    columns[column_index++]->insert(pruning_status ? *pruning_status : iceberg_pruning_status_datatype_nullable->getDefault());
}

IcebergMetadataLogLevel getIcebergMetadataLogLevel(const ContextPtr & local_context)
{
    return local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;
}

void insertRowToLogTableImpl(
    const ContextPtr & local_context,
    String row,
    IcebergMetadataLogLevel row_log_level,
    const String & table_path,
    const Iceberg::IcebergPathFromMetadata & file_path,
    std::optional<UInt64> row_in_file,
    std::optional<Iceberg::PruningReturnStatus> pruning_status)
{
    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    auto iceberg_metadata_log = Context::getGlobalContextInstance()->getIcebergMetadataLog();

    if (!iceberg_metadata_log)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg metadata log table is not configured");
    }

    iceberg_metadata_log->add([&](DB::IcebergMetadataLogElement & element)
    {
        element = DB::IcebergMetadataLogElement{
            .current_time = spec.tv_sec,
            .query_id = local_context->getCurrentQueryId(),
            .content_type = row_log_level,
            .table_path = table_path,
            .file_path = file_path.serialize(),
            .metadata_content = row,
            .row_in_file = row_in_file,
            .pruning_status = pruning_status};
    });
}
}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "iceberg_metadata_log",
    .description = R"DOCS_MD(
The `system.iceberg_metadata_log` table records metadata access and parsing events for Iceberg tables read by ClickHouse. It provides detailed information about each metadata file or entry processed, which is useful for debugging, auditing, and understanding Iceberg table structure evolution.

This table logs every metadata file and entry read from Iceberg tables, including root metadata files, manifest lists, and manifest entries. It helps users trace how ClickHouse interprets Iceberg table metadata and diagnose issues related to schema evolution, file resolution, or query planning.

<Note>
This table is primarily intended for debugging purposes.
</Note>

### Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`iceberg_metadata_log_level`](/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'manifest_file_entry';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

To log only the root metadata JSON file used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'metadata';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

See more information in the description of the [`iceberg_metadata_log_level`](/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level) setting.

### Good To Know {#good-to-know}

- Use `iceberg_metadata_log_level` at the query level only when you need to investigate your Iceberg table in detail. Otherwise, you may populate the log table with excessive metadata and experience performance degradation.
- The table contains duplicate entries, as it is intended primarily for debugging and does not guarantee uniqueness per entity. Separate rows store content and pruning status because they are collected at different moments in a program. Content is collected when the metadata is read, pruning status is collected when the metadata is checked for pruning. **Never rely on the table itself for deduplication.**
- If you use a `content_type` more verbose than `ManifestListMetadata`, the Iceberg metadata cache is disabled for manifest lists.
- Similarly, if you use a `content_type` more verbose than `ManifestFileMetadata`, the Iceberg metadata cache is disabled for manifest files.
- If the SELECT query was cancelled or failed, the log table may still contain entries for metadata processed before the failure but will not contain information about metadata entities that were not processed.
)DOCS_MD",
    .get_columns = IcebergMetadataLogElement::getColumnsDescription,
    .columns_notes = R"DOCS_MD(
### `content_type` values {#content-type-values}

- `None`: No content.
- `Metadata`: Root metadata file.
- `ManifestListMetadata`: Manifest list metadata.
- `ManifestListEntry`: Entry in a manifest list.
- `ManifestFileMetadata`: Manifest file metadata.
- `ManifestFileEntry`: Entry in a manifest file.
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Iceberg Table Engine](/reference/engines/table-engines/integrations/iceberg)
- [Iceberg Table Function](/reference/functions/table-functions/iceberg)
- [system.iceberg_history](/reference/system-tables/iceberg_history)
)DOCS_MD")

}
