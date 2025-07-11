#include <Storages/HivePartitioningUtils.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/impl/DuplicateKeyFoundException.h>

#include <Formats/EscapingRuleUtils.h>

#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
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

    const auto hive_map = HivePartitioningUtils::parseHivePartitioningKeysAndValues(sample_path);

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
                hive_partition_columns_to_read_from_file_path.emplace_back(key, type);
            }
            else
            {
                hive_partition_columns_to_read_from_file_path.emplace_back(key, std::make_shared<DataTypeString>());
            }
        }
    }

    return hive_partition_columns_to_read_from_file_path;
}

}

}
