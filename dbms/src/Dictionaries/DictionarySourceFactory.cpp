#include <Dictionaries/DictionarySourceFactory.h>

#include <Core/Block.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/FileDictionarySource.h>
#include <Dictionaries/MySQLDictionarySource.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/ExecutableDictionarySource.h>
#include <Dictionaries/HTTPDictionarySource.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <Core/FieldVisitors.h>
#include <Columns/ColumnsNumber.h>
#include <IO/HTTPCommon.h>
#include <memory>
#include <mutex>

#include <Common/config.h>
#if Poco_MongoDB_FOUND
    #include <Dictionaries/MongoDBDictionarySource.h>
#endif
#if Poco_DataODBC_FOUND
    #include <Poco/Data/ODBC/Connector.h>
    #include <Dictionaries/ODBCDictionarySource.h>
#endif
#if USE_MYSQL
    #include <Dictionaries/MySQLDictionarySource.h>
#endif



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
        block.insert(ColumnWithTypeAndName{
            std::make_shared<ColumnUInt64>(1), std::make_shared<DataTypeUInt64>(), dict_struct.id.value().name});

    if (dict_struct.key)
    {
        for (const auto & attribute : *dict_struct.key)
        {
            auto column = attribute.type->createColumn();
            column->insertDefault();

            block.insert(ColumnWithTypeAndName{column, attribute.type, attribute.name});
        }
    }

    if (dict_struct.range_min)
        for (const auto & attribute : { dict_struct.range_min, dict_struct.range_max })
            block.insert(ColumnWithTypeAndName{
                std::make_shared<ColumnUInt16>(1), std::make_shared<DataTypeDate>(), attribute.value().name});

    for (const auto & attribute : dict_struct.attributes)
    {
        auto column = attribute.type->createColumn();
        column->insert(attribute.null_value);

        block.insert(ColumnWithTypeAndName{column, attribute.type, attribute.name});
    }

    return block;
}

}


DictionarySourceFactory::DictionarySourceFactory()
{
#if Poco_DataODBC_FOUND
    Poco::Data::ODBC::Connector::registerConnector();
#endif
}


DictionarySourcePtr DictionarySourceFactory::create(
    const std::string & name, Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const DictionaryStructure & dict_struct, Context & context) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    if (keys.size() != 1)
        throw Exception{
            name +": element dictionary.source should have exactly one child element",
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG
        };

    auto sample_block = createSampleBlock(dict_struct);

    const auto & source_type = keys.front();

    if ("file" == source_type)
    {
        if (dict_struct.has_expressions)
            throw Exception{
                "Dictionary source of type `file` does not support attribute expressions",
                ErrorCodes::LOGICAL_ERROR};

        const auto filename = config.getString(config_prefix + ".file.path");
        const auto format = config.getString(config_prefix + ".file.format");
        return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);
    }
    else if ("mysql" == source_type)
    {
#if USE_MYSQL
        return std::make_unique<MySQLDictionarySource>(dict_struct, config, config_prefix + ".mysql", sample_block);
#else
        throw Exception{"Dictionary source of type `mysql` disabled because ClickHouse built without mysql support.",
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
#if Poco_MongoDB_FOUND
        return std::make_unique<MongoDBDictionarySource>(dict_struct, config, config_prefix + ".mongodb", sample_block);
#else
        throw Exception{"Dictionary source of type `mongodb` disabled because poco library built without mongodb support.",
            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else if ("odbc" == source_type)
    {
#if Poco_DataODBC_FOUND
        return std::make_unique<ODBCDictionarySource>(dict_struct, config, config_prefix + ".odbc", sample_block);
#else
        throw Exception{"Dictionary source of type `odbc` disabled because poco library built without ODBC support.",
            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else if ("executable" == source_type)
    {
        if (dict_struct.has_expressions)
            throw Exception{
                "Dictionary source of type `executable` does not support attribute expressions",
                ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<ExecutableDictionarySource>(dict_struct, config, config_prefix + ".executable", sample_block, context);
    }
    else if ("http" == source_type)
    {

        if (dict_struct.has_expressions)
            throw Exception{
                "Dictionary source of type `http` does not support attribute expressions",
                ErrorCodes::LOGICAL_ERROR};

#if Poco_NetSSL_FOUND
        // Used for https queries
        std::call_once(ssl_init_once, SSLInit);
#endif

        return std::make_unique<HTTPDictionarySource>(dict_struct, config, config_prefix + ".http", sample_block, context);
    }

    throw Exception{
        name + ": unknown dictionary source type: " + source_type,
        ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
}

}
