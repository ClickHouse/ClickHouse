#include <Storages/System/StorageSystemGraphite.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>

#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace
{

using namespace Poco::Util;

struct Pattern
{
    struct Retention
    {
        UInt64 age;
        UInt64 precision;
    };

    std::string regexp;
    std::string function;
    std::vector<Retention> retentions;
    UInt16 priority;
    UInt8 is_default;
};

static Pattern readOnePattern(
    const AbstractConfiguration & config,
    const std::string & path)
{
    Pattern pattern;
    AbstractConfiguration::Keys keys;

    config.keys(path, keys);

    if (keys.empty())
        throw Exception("Empty pattern in Graphite rollup configuration", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;

        if (startsWith(key, "regexp"))
        {
            pattern.regexp = config.getString(key_path);
        }
        else if (startsWith(key, "function"))
        {
            pattern.function = config.getString(key_path);
        }
        else if (startsWith(key, "retention"))
        {
            pattern.retentions.push_back(Pattern::Retention{0, 0});
            pattern.retentions.back().age = config.getUInt64(key_path + ".age", 0);
            pattern.retentions.back().precision = config.getUInt64(key_path + ".precision", 0);
        }
    }

    return pattern;
}

static std::vector<Pattern> readPatterns(
    const AbstractConfiguration & config,
    const std::string & section)
{
    AbstractConfiguration::Keys keys;
    std::vector<Pattern> result;
    size_t count = 0;

    config.keys(section, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "pattern"))
        {
            Pattern pattern(readOnePattern(config, section + "." + key));
            pattern.is_default = false;
            pattern.priority = ++count;
            result.push_back(pattern);
        }
        else if (startsWith(key, "default"))
        {
            Pattern pattern(readOnePattern(config, section + "." + key));
            pattern.is_default = true;
            pattern.priority = std::numeric_limits<UInt16>::max();
            result.push_back(pattern);
        }
    }

    return result;
}

static Strings getAllGraphiteSections(const AbstractConfiguration & config)
{
    Strings result;

    AbstractConfiguration::Keys keys;
    config.keys(keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "graphite_"))
            result.push_back(key);
    }

    return result;
}

} // namespace

StorageSystemGraphite::StorageSystemGraphite(const std::string & name_)
    : name(name_)
{
    columns = NamesAndTypesList{
        {"config_name", std::make_shared<DataTypeString>()},
        {"regexp",      std::make_shared<DataTypeString>()},
        {"function",    std::make_shared<DataTypeString>()},
        {"age",         std::make_shared<DataTypeUInt64>()},
        {"precision",   std::make_shared<DataTypeUInt64>()},
        {"priority",    std::make_shared<DataTypeUInt16>()},
        {"is_default",  std::make_shared<DataTypeUInt8>()}
    };
}


BlockInputStreams StorageSystemGraphite::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    const auto & config = context.getConfigRef();

    Strings sections = getAllGraphiteSections(config);
    for (const auto & section : sections)
    {
        const auto patterns = readPatterns(config, section);
        for (const auto & pattern : patterns)
        {
            for (const auto & ret : pattern.retentions)
            {
                res_columns[0]->insert(Field(section));
                res_columns[1]->insert(Field(pattern.regexp));
                res_columns[2]->insert(Field(pattern.function));
                res_columns[3]->insert(nearestFieldType(ret.age));
                res_columns[4]->insert(nearestFieldType(ret.precision));
                res_columns[5]->insert(nearestFieldType(pattern.priority));
                res_columns[6]->insert(nearestFieldType(pattern.is_default));
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}

}
