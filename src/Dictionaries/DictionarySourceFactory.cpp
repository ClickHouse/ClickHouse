#include "DictionarySourceFactory.h"

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include "DictionaryStructure.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

namespace
{
    Block createSampleBlock(const DictionaryStructure & dict_struct)
    {
        Block block;

        if (dict_struct.id)
        {
            block.insert(ColumnWithTypeAndName{ColumnUInt64::create(1, 0), std::make_shared<DataTypeUInt64>(), dict_struct.id->name});
        }
        else if (dict_struct.key)
        {
            for (const auto & attribute : *dict_struct.key)
            {
                auto column = attribute.type->createColumn();
                column->insertDefault();

                block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
            }
        }

        if (dict_struct.range_min)
        {
            for (const auto & attribute : {dict_struct.range_min, dict_struct.range_max})
            {
                const auto & type = std::make_shared<DataTypeNullable>(attribute->type);
                auto column = type->createColumn();
                column->insertDefault();

                block.insert(ColumnWithTypeAndName{std::move(column), type, attribute->name});
            }
        }

        for (const auto & attribute : dict_struct.attributes)
        {
            auto column = attribute.type->createColumn();
            column->insert(attribute.null_value);

            block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
        }

        return block;
    }

}


DictionarySourceFactory::DictionarySourceFactory() : log(getLogger("DictionarySourceFactory"))
{
}

void DictionarySourceFactory::registerSource(const std::string & source_type, Creator create_source)
{
    if (!registered_sources.emplace(source_type, std::move(create_source)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DictionarySourceFactory: the source name '{}' is not unique", source_type);
}

DictionarySourcePtr DictionarySourceFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DictionaryStructure & dict_struct,
    ContextPtr global_context,
    const std::string & default_database,
    bool check_config) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    if (keys.empty() || keys.size() > 2)
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
            "{}: element dictionary.source should have one or two child elements",
            name);

    const std::string & source_type = keys.front() == "settings" ? keys.back() : keys.front();

    const auto found = registered_sources.find(source_type);
    if (found != registered_sources.end())
    {
        const auto & create_source = found->second;
        auto sample_block = createSampleBlock(dict_struct);
        return create_source(dict_struct, config, config_prefix, sample_block, global_context, default_database, check_config);
    }

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
        "{}: unknown dictionary source type: {}",
        name,
        source_type);
}

void DictionarySourceFactory::checkSourceAvailable(const std::string & source_type, const std::string & dictionary_name, const ContextPtr & /* context */) const
{
    const auto found = registered_sources.find(source_type);
    if (found == registered_sources.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
            "{}: unknown dictionary source type: {}",
            dictionary_name,
            source_type);
    }
}

DictionarySourceFactory & DictionarySourceFactory::instance()
{
    static DictionarySourceFactory instance;
    return instance;
}

}
