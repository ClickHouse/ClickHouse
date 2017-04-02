#include <Storages/System/StorageSystemGraphite.h>

#include <Core/Field.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Poco/Util/Application.h>

namespace DB
{

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

    for (const auto & key : keys) {
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

static std::vector<Pattern> readPatterns(const std::string & section)
{
    const AbstractConfiguration & config = Application::instance().config();
    AbstractConfiguration::Keys keys;
    std::vector<Pattern> result;
    size_t count = 0;

    config.keys(section, keys);

    for (const auto & key : keys) {
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

static Strings getAllGraphiteSections()
{
    const AbstractConfiguration & config = Application::instance().config();
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
    , columns {
        {"config_name", std::make_shared<DataTypeString>()},
        {"regexp",      std::make_shared<DataTypeString>()},
        {"function",    std::make_shared<DataTypeString>()},
        {"age",         std::make_shared<DataTypeUInt64>()},
        {"precision",   std::make_shared<DataTypeUInt64>()},
        {"priority",    std::make_shared<DataTypeUInt16>()},
        {"is_default",  std::make_shared<DataTypeUInt8>()}}
{
}

StoragePtr StorageSystemGraphite::create(const std::string & name_)
{
    return make_shared(name_);
}

BlockInputStreams StorageSystemGraphite::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block;

    ColumnWithTypeAndName col_conf_name;
    col_conf_name.name = "config_name";
    col_conf_name.type = std::make_shared<DataTypeString>();
    col_conf_name.column = std::make_shared<ColumnString>();
    block.insert(col_conf_name);

    ColumnWithTypeAndName col_regexp;
    col_regexp.name = "regexp";
    col_regexp.type = std::make_shared<DataTypeString>();
    col_regexp.column = std::make_shared<ColumnString>();
    block.insert(col_regexp);

    ColumnWithTypeAndName col_function;
    col_function.name = "function";
    col_function.type = std::make_shared<DataTypeString>();
    col_function.column = std::make_shared<ColumnString>();
    block.insert(col_function);

    ColumnWithTypeAndName col_age;
    col_age.name = "age";
    col_age.type = std::make_shared<DataTypeUInt64>();
    col_age.column = std::make_shared<ColumnUInt64>();
    block.insert(col_age);

    ColumnWithTypeAndName col_precision;
    col_precision.name = "precision";
    col_precision.type = std::make_shared<DataTypeUInt64>();
    col_precision.column = std::make_shared<ColumnUInt64>();
    block.insert(col_precision);

    ColumnWithTypeAndName col_priority;
    col_priority.name = "priority";
    col_priority.type = std::make_shared<DataTypeUInt16>();
    col_priority.column = std::make_shared<ColumnUInt16>();
    block.insert(col_priority);

    ColumnWithTypeAndName col_is_default;
    col_is_default.name = "is_default";
    col_is_default.type = std::make_shared<DataTypeUInt8>();
    col_is_default.column = std::make_shared<ColumnUInt8>();
    block.insert(col_is_default);

    Strings sections = getAllGraphiteSections();
    for (const auto & section : sections)
    {
        const auto patterns = readPatterns(section);
        for (const auto & pattern : patterns)
        {
            for (const auto & ret : pattern.retentions)
            {
                col_conf_name.column->insert(Field(section));
                col_regexp.column->insert(Field(pattern.regexp));
                col_function.column->insert(Field(pattern.function));
                col_age.column->insert(nearestFieldType(ret.age));
                col_precision.column->insert(nearestFieldType(ret.precision));
                col_priority.column->insert(nearestFieldType(pattern.priority));
                col_is_default.column->insert(nearestFieldType(pattern.is_default));
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}

}
