#include <Storages/HivePartitioningUtils.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/impl/DuplicateKeyFoundException.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/FormatFactory.h>
#include <Processors/Chunk.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
}

namespace HivePartitioningUtils
{

static auto makeExtractor()
{
    return KeyValuePairExtractorBuilder().withItemDelimiters({'/'}).withKeyValueDelimiter('=').buildWithReferenceMap();
}

HivePartitioningKeysAndValues parseHivePartitioningKeysAndValues(const String & path)
{
    static auto extractor = makeExtractor();

    HivePartitioningKeysAndValues key_values;

    // cutting the filename to prevent malformed filenames that contain key-value-pairs from being extracted
    // not sure if we actually need to do that, but just in case. Plus, the previous regex impl took care of it
    const auto last_slash_pos = path.find_last_of('/');

    if (last_slash_pos == std::string::npos)
    {
        // nothing to extract, there is no path, just a filename
        return key_values;
    }

    std::string_view path_without_filename(path.data(), last_slash_pos);

    try
    {
        extractor.extract(path_without_filename, key_values);
    }
    catch (const extractKV::DuplicateKeyFoundException & ex)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Path '{}' to file with enabled hive-style partitioning contains duplicated partition key {} with different values, only unique keys are allowed", path, ex.key);
    }

    return key_values;
}
NamesAndTypesList extractHivePartitionColumnsFromPath(
    const ColumnsDescription & storage_columns,
    const std::string & sample_path,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;

    const auto hive_map = parseHivePartitioningKeysAndValues(sample_path);

    for (const auto & item : hive_map)
    {
        const std::string key(item.first);
        const std::string value(item.second);

        // if we know the type from the schema, use it.
        if (storage_columns.has(key))
        {
            hive_partition_columns_to_read_from_file_path.emplace_back(key, storage_columns.get(key).type);
        }
        else
        {
            if (const auto type = tryInferDataTypeByEscapingRule(value, format_settings ? *format_settings : getFormatSettings(context), FormatSettings::EscapingRule::Raw))
            {
                if (type->canBeInsideLowCardinality() && isStringOrFixedString(type))
                {
                    hive_partition_columns_to_read_from_file_path.emplace_back(key, std::make_shared<DataTypeLowCardinality>(type));
                }
                else
                {
                    hive_partition_columns_to_read_from_file_path.emplace_back(key, type);
                }
            }
            else
            {
                hive_partition_columns_to_read_from_file_path.emplace_back(key, std::make_shared<DataTypeString>());
            }
        }
    }

    return hive_partition_columns_to_read_from_file_path;
}

void addPartitionColumnsToChunk(
    Chunk & chunk,
    const NamesAndTypesList & hive_partition_columns_to_read_from_file_path,
    const std::string & path)
{
    const auto hive_map = parseHivePartitioningKeysAndValues(path);

    for (const auto & column : hive_partition_columns_to_read_from_file_path)
    {
        const std::string column_name = column.getNameInStorage();
        const auto it = hive_map.find(column_name);

        if (it == hive_map.end())
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Expected to find hive partitioning column {} in the path {}."
                "Try it with hive partitioning disabled (partition_strategy='wildcard' and/or use_hive_partitioning=0",
                column_name,
                path);
        }

        auto chunk_column = column.type->createColumnConst(chunk.getNumRows(), convertFieldToType(Field(it->second), *column.type))->convertToFullColumnIfConst();
        chunk.addColumn(std::move(chunk_column));
    }
}

void sanityCheckSchemaAndHivePartitionColumns(const NamesAndTypesList & hive_partition_columns_to_read_from_file_path, const ColumnsDescription & storage_columns)
{
    for (const auto & column : hive_partition_columns_to_read_from_file_path)
    {
        if (!storage_columns.has(column.name))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "All hive partitioning columns must be present in the schema. Missing column: {}. "
                "If you do not want to use hive partitioning, try `use_hive_partitioning=0` and/or `partition_strategy != hive`",
                column.name);
        }
    }

    if (storage_columns.size() == hive_partition_columns_to_read_from_file_path.size())
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "A hive partitioned file can't contain only partition columns. "
            "Try reading it with `use_hive_partitioning=0` and/or `partition_strategy != hive`");
    }
}

void extractPartitionColumnsFromPathAndEnrichStorageColumns(
    ColumnsDescription & storage_columns,
    NamesAndTypesList & hive_partition_columns_to_read_from_file_path,
    const std::string & path,
    bool inferred_schema,
    std::optional<FormatSettings> format_settings,
    ContextPtr context)
{
    hive_partition_columns_to_read_from_file_path = extractHivePartitionColumnsFromPath(storage_columns, path, format_settings, context);

    /// If the structure was inferred (not present in `columns_`), then we might need to enrich the schema with partition columns
    /// Because they might not be present in the data and exist only in the path
    if (inferred_schema)
    {
        for (const auto & [name, type]: hive_partition_columns_to_read_from_file_path)
        {
            if (!storage_columns.has(name))
            {
                storage_columns.add({name, type});
            }
        }
    }
}

HivePartitionColumnsWithFileColumnsPair setupHivePartitioningForObjectStorage(
    ColumnsDescription & columns,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::string & sample_path,
    bool inferred_schema,
    std::optional<FormatSettings> format_settings,
    ContextPtr context)
{
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
    NamesAndTypesList file_columns;

    /*
     * If `partition_strategy=hive`, the partition columns shall be extracted from the `PARTITION BY` expression.
     * There is no need to read from the file's path.
     *
     * Otherwise, in case `use_hive_partitioning=1`, we can keep the old behavior of extracting it from the sample path.
     * And if the schema was inferred (not specified in the table definition), we need to enrich it with the path partition columns
     */
     if (configuration->partition_strategy && configuration->partition_strategy_type == PartitionStrategyFactory::StrategyType::HIVE)
     {
         hive_partition_columns_to_read_from_file_path = configuration->partition_strategy->getPartitionColumns();
     }
     else if (context->getSettingsRef()[Setting::use_hive_partitioning])
     {
        extractPartitionColumnsFromPathAndEnrichStorageColumns(
            columns,
            hive_partition_columns_to_read_from_file_path,
            sample_path,
            inferred_schema,
            format_settings,
            context);
     }

     sanityCheckSchemaAndHivePartitionColumns(hive_partition_columns_to_read_from_file_path, columns);

     if (configuration->partition_columns_in_data_file)
     {
         file_columns = columns.getAllPhysical();
     }
     else
     {
         std::unordered_set<String> hive_partition_columns_to_read_from_file_path_set;

         for (const auto & [name, type] : hive_partition_columns_to_read_from_file_path)
         {
             hive_partition_columns_to_read_from_file_path_set.insert(name);
         }

         for (const auto & [name, type] : columns.getAllPhysical())
         {
             if (!hive_partition_columns_to_read_from_file_path_set.contains(name))
             {
                file_columns.emplace_back(name, type);
             }
         }
     }

     return {hive_partition_columns_to_read_from_file_path, file_columns};
}

HivePartitionColumnsWithFileColumnsPair setupHivePartitioningForFileURLLikeStorage(
    ColumnsDescription & columns,
    const std::string & sample_path,
    bool inferred_schema,
    std::optional<FormatSettings> format_settings,
    ContextPtr context)
{
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
    NamesAndTypesList file_columns;

    if (context->getSettingsRef()[Setting::use_hive_partitioning])
    {
        extractPartitionColumnsFromPathAndEnrichStorageColumns(
            columns,
            hive_partition_columns_to_read_from_file_path,
            sample_path,
            inferred_schema,
            format_settings,
            context);
    }

    sanityCheckSchemaAndHivePartitionColumns(hive_partition_columns_to_read_from_file_path, columns);

    /// Partition strategy is not implemented for File/URL storages,
    /// so there is no option to set whether hive partition columns are in the data file or not.
    file_columns = columns.getAllPhysical();

    return {hive_partition_columns_to_read_from_file_path, file_columns};
}

}

}
