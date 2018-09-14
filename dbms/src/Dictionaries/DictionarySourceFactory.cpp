#include <Dictionaries/DictionarySourceFactory.h>

#include <Core/Block.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/FileDictionarySource.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/ExecutableDictionarySource.h>
#include <Dictionaries/HTTPDictionarySource.h>
#include <Dictionaries/LibraryDictionarySource.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <Common/FieldVisitors.h>
#include <Columns/ColumnsNumber.h>
#include <IO/HTTPCommon.h>
#include <memory>
#include <mutex>

#include <Common/config.h>
#if USE_POCO_MONGODB
    #include <Dictionaries/MongoDBDictionarySource.h>
#endif
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
    #include <Poco/Data/ODBC/Connector.h>
    #include <Dictionaries/ODBCDictionarySource.h>
#endif
#if USE_MYSQL
    #include <Dictionaries/MySQLDictionarySource.h>
#endif

#include <Poco/Logger.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

Block createSampleBlock(const DictionaryStructure & dict_struct)
{
    Block block;

    if (dict_struct.id)
        block.insert(ColumnWithTypeAndName{ColumnUInt64::create(1, 0), std::make_shared<DataTypeUInt64>(), dict_struct.id->name});

    if (dict_struct.key)
    {
        for (const auto & attribute : *dict_struct.key)
        {
            auto column = attribute.type->createColumn();
            column->insertDefault();

            block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
        }
    }

    if (dict_struct.range_min)
        for (const auto & attribute : { dict_struct.range_min, dict_struct.range_max })
            block.insert(ColumnWithTypeAndName{
                ColumnUInt16::create(1, 0), std::make_shared<DataTypeDate>(), attribute->name});

    for (const auto & attribute : dict_struct.attributes)
    {
        auto column = attribute.type->createColumn();
        column->insert(attribute.null_value);

        block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
    }

    return block;
}

}


DictionarySourceFactory::DictionarySourceFactory()
    : log(&Poco::Logger::get("DictionarySourceFactory"))
{
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
    Poco::Data::ODBC::Connector::registerConnector();
#endif
}

void DictionarySourceFactory::registerSource(const std::string & source_type, Creator create_source)
{
    LOG_DEBUG(log, "Register dictionary source type `" + source_type + "`");
    if (!registered_sources.emplace(source_type, std::move(create_source)).second)
        throw Exception("DictionarySourceFactory: the source name '" + source_type + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

DictionarySourcePtr DictionarySourceFactory::create(
    const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const DictionaryStructure & dict_struct, Context & context) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    if (keys.size() != 1)
        throw Exception{name +": element dictionary.source should have exactly one child element", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

    auto sample_block = createSampleBlock(dict_struct);

    const auto & source_type = keys.front();

    if ("file" == source_type)
    {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `file` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        const auto filename = config.getString(config_prefix + ".file.path");
        const auto format = config.getString(config_prefix + ".file.format");
        return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);
    }
    else if ("mysql" == source_type)
    {
#if USE_MYSQL
        return std::make_unique<MySQLDictionarySource>(dict_struct, config, config_prefix + ".mysql", sample_block);
#else
        throw Exception{"Dictionary source of type `mysql` is disabled because ClickHouse was built without mysql support.",
            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else if ("clickhouse" == source_type)
    {
        return std::make_unique<ClickHouseDictionarySource>(dict_struct, config, config_prefix + ".clickhouse",
            sample_block, context);
    }
    else if ("mongodb" == source_type)
    {
#if USE_POCO_MONGODB
        return std::make_unique<MongoDBDictionarySource>(dict_struct, config, config_prefix + ".mongodb", sample_block);
#else
        throw Exception{"Dictionary source of type `mongodb` is disabled because poco library was built without mongodb support.",
            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else if ("odbc" == source_type)
    {
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
        return std::make_unique<ODBCDictionarySource>(dict_struct, config, config_prefix + ".odbc", sample_block, context);
#else
        throw Exception{"Dictionary source of type `odbc` is disabled because poco library was built without ODBC support.",
            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else if ("executable" == source_type)
    {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `executable` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<ExecutableDictionarySource>(dict_struct, config, config_prefix + ".executable", sample_block, context);
    }
    else if ("http" == source_type)
    {

        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `http` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<HTTPDictionarySource>(dict_struct, config, config_prefix + ".http", sample_block, context);
    }
    else if ("library" == source_type)
    {
        return std::make_unique<LibraryDictionarySource>(dict_struct, config, config_prefix + ".library", sample_block, context);
    }
    else
    {
        const auto found = registered_sources.find(source_type);
        if (found != registered_sources.end())
        {
            const auto & create_source = found->second;
            return create_source(dict_struct, config, config_prefix, sample_block, context);
        }
    }

    throw Exception{name + ": unknown dictionary source type: " + source_type, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
}

}
