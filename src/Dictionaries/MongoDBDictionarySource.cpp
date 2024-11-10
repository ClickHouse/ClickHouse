#include "config.h"

#include "DictionarySourceFactory.h"
#if USE_MONGODB
#include "MongoDBDictionarySource.h"
#include "DictionaryStructure.h"

#include <Common/logger_useful.h>
#include <Processors/Sources/MongoDBSource.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <bsoncxx/builder/basic/array.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::basic::array;
#endif

namespace DB
{

namespace ErrorCodes
{
    #if USE_MONGODB
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    #else
    extern const int SUPPORT_IS_DISABLED;
    #endif
}

void registerDictionarySourceMongoDB(DictionarySourceFactory & factory)
{
    #if USE_MONGODB
    auto create_dictionary_source = [](
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & root_config_prefix,
        Block & sample_block,
        ContextPtr context,
        const std::string & /* default_database */,
        bool /* created_from_ddl */)
    {
        const auto config_prefix = root_config_prefix + ".mongodb";
        auto configuration = std::make_shared<MongoDBConfiguration>();
        if (auto named_collection = tryGetNamedCollectionWithOverrides(config, config_prefix, context))
        {
            if (named_collection->has("uri"))
            {
                validateNamedCollection(*named_collection, {"collection"}, {});
                configuration->uri = std::make_unique<mongocxx::uri>(named_collection->get<String>("uri"));
            }
            else
            {
                validateNamedCollection(*named_collection, {"host", "db", "collection"}, {"port", "user", "password", "options"});
                String user = named_collection->get<String>("user");
                String auth_string;
                if (!user.empty())
                    auth_string = fmt::format("{}:{}@", user, named_collection->get<String>("password"));
                configuration->uri = std::make_unique<mongocxx::uri>(fmt::format("mongodb://{}{}:{}/{}?{}",
                                                                                 auth_string,
                                                                                 named_collection->get<String>("host"),
                                                                                 named_collection->getOrDefault<String>("port", "27017"),
                                                                                 named_collection->get<String>("db"),
                                                                                 named_collection->getOrDefault<String>("options", "")));
            }
            configuration->collection = named_collection->get<String>("collection");
        }
        else
        {
            configuration->collection = config.getString(config_prefix + ".collection");
            auto uri_str = config.getString(config_prefix + ".uri", "");
            if (!uri_str.empty())
                configuration->uri = std::make_unique<mongocxx::uri>(uri_str);
            else
            {
                String user = config.getString(config_prefix + ".user", "");
                String auth_string;
                if (!user.empty())
                    auth_string = fmt::format("{}:{}@", user, config.getString(config_prefix + ".password", ""));
                configuration->uri = std::make_unique<mongocxx::uri>(fmt::format("mongodb://{}{}:{}/{}?{}",
                                                                                 auth_string,
                                                                                 config.getString(config_prefix + ".host"),
                                                                                 config.getString(config_prefix + ".port", "27017"),
                                                                                 config.getString(config_prefix + ".db"),
                                                                                 config.getString(config_prefix + ".options", "")));
            }
        }

        configuration->checkHosts(context);

        return std::make_unique<MongoDBDictionarySource>(dict_struct, std::move(configuration), std::move(sample_block));
    };
    #else
    auto create_dictionary_source = [](
        const DictionaryStructure & /* dict_struct */,
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* root_config_prefix */,
        Block & /* sample_block */,
        ContextPtr /* context */,
        const std::string & /* default_database */,
        bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "Dictionary source of type `mongodb` is disabled because ClickHouse was built without mongodb support.");
    };
    #endif

    factory.registerSource("mongodb", create_dictionary_source);
}

#if USE_MONGODB
static const UInt64 max_block_size = 8192;


MongoDBDictionarySource::MongoDBDictionarySource(
    const DictionaryStructure & dict_struct_,
    std::shared_ptr<MongoDBConfiguration> configuration_,
    Block sample_block_)
    : dict_struct{dict_struct_}
    , configuration{configuration_}
    , sample_block{sample_block_}
{
}

MongoDBDictionarySource::MongoDBDictionarySource(const MongoDBDictionarySource & other)
    : MongoDBDictionarySource{other.dict_struct, other.configuration, other.sample_block}
{
}

MongoDBDictionarySource::~MongoDBDictionarySource() = default;

QueryPipeline MongoDBDictionarySource::loadAll()
{
    return QueryPipeline(std::make_shared<MongoDBSource>(*configuration->uri, configuration->collection, make_document(), mongocxx::options::find(), sample_block, max_block_size));
}

QueryPipeline MongoDBDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    if (!dict_struct.id)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is required for selective loading");

    auto ids_array = array();
    for (const auto & id : ids)
        ids_array.append(static_cast<Int64>(id));

    return QueryPipeline(std::make_shared<MongoDBSource>(*configuration->uri, configuration->collection, make_document(kvp(dict_struct.id->name, make_document(kvp("$in", ids_array)))), mongocxx::options::find(), sample_block, max_block_size));
}


QueryPipeline MongoDBDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    if (!dict_struct.key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is required for selective loading");

    if (key_columns.size() != dict_struct.key->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of key_columns does not equal to the size of dictionary key");

    auto keys = array();
    for (const auto & row : requested_rows)
    {
        auto key = array();
        for (size_t i = 0; i < key_columns.size(); i++)
        {
            const auto & dict_key = dict_struct.key->at(i);
            WhichDataType type(dict_key.type);

            if (isBool(dict_key.type))
                key.append(make_document(kvp(dict_key.name, key_columns[i]->getBool(row))));
            else if (type.isUInt())
                key.append(make_document(kvp(dict_key.name, static_cast<Int64>(key_columns[i]->getUInt(row)))));
            else if (type.isFloat64())
                key.append(make_document(kvp(dict_key.name, key_columns[i]->getFloat64(row))));
            else if (type.isInt())
                key.append(make_document(kvp(dict_key.name, key_columns[i]->getInt(row))));
            else if (type.isString())
                key.append(make_document(kvp(dict_key.name, key_columns[i]->getDataAt(row).toString())));
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type '{}' of key in MongoDB dictionary", dict_key.type->getName());
        }
        keys.append(make_document(kvp("$and", key)));
    }

    return QueryPipeline(std::make_shared<MongoDBSource>(*configuration->uri, configuration->collection, make_document(kvp("$or", keys)), mongocxx::options::find(), sample_block, max_block_size));
}

std::string MongoDBDictionarySource::toString() const
{
    return fmt::format("MongoDB: {}", configuration->uri->to_string());
}
#endif

}
