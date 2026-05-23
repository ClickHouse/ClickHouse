#include <Access/Common/AccessType.h>
#include <Core/Types.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/Elasticsearch/ElasticsearchQueueSettings.h>
#include <Storages/Elasticsearch/ElasticsearchQueue_fwd.h>
#include <Storages/Elasticsearch/StorageElasticsearchQueue.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <cctype>

namespace DB
{

namespace ElasticsearchQueueSetting
{
    extern const ElasticsearchQueueSettingsString elasticsearch_auth_type;
    extern const ElasticsearchQueueSettingsString elasticsearch_cursor_field;
    extern const ElasticsearchQueueSettingsString elasticsearch_index;
    extern const ElasticsearchQueueSettingsString elasticsearch_keeper_checkpoint_name;
    extern const ElasticsearchQueueSettingsString elasticsearch_keeper_path;
    extern const ElasticsearchQueueSettingsUInt64 elasticsearch_max_block_size;
    extern const ElasticsearchQueueSettingsString elasticsearch_pit_keep_alive;
    extern const ElasticsearchQueueSettingsUInt64 elasticsearch_poll_max_batch_size;
    extern const ElasticsearchQueueSettingsString elasticsearch_url;
    extern const ElasticsearchQueueSettingsBool elasticsearch_use_point_in_time;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
String toLowerASCII(String value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}
}

void registerStorageElasticsearchQueue(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args) -> std::shared_ptr<IStorage>
    {
        ASTs & engine_args = args.engine_args;
        const size_t args_count = engine_args.size();
        const bool has_settings = args.storage_def->settings;

        auto settings = std::make_unique<ElasticsearchQueueSettings>();
        String collection_name;
        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
        {
            settings->loadFromNamedCollection(named_collection);
            collection_name = assert_cast<const ASTIdentifier *>(args.engine_args[0].get())->name();
        }

        if (has_settings)
            settings->loadFromQuery(*args.storage_def);

#define CHECK_ELASTICSEARCH_QUEUE_ARGUMENT(ARG_NUM, PAR_NAME) \
        if (args_count < (ARG_NUM) && !(*settings)[ElasticsearchQueueSetting::PAR_NAME].changed) \
        { \
            throw Exception( \
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, \
                "Required parameter '{}' for storage ElasticsearchQueue not specified", \
                #PAR_NAME); \
        } \
        if (args_count >= (ARG_NUM)) \
        { \
            if (has_settings && (*settings)[ElasticsearchQueueSetting::PAR_NAME].changed) \
                throw Exception( \
                    ErrorCodes::BAD_ARGUMENTS, \
                    "The argument {} of storage ElasticsearchQueue and the parameter '{}' in SETTINGS cannot be specified at the same time", \
                    #ARG_NUM, \
                    #PAR_NAME); \
            engine_args[(ARG_NUM) - 1] = evaluateConstantExpressionAsLiteral(engine_args[(ARG_NUM) - 1], args.getLocalContext()); \
            (*settings)[ElasticsearchQueueSetting::PAR_NAME] = checkAndGetLiteralArgument<String>(engine_args[(ARG_NUM) - 1], #PAR_NAME); \
        }

        if (collection_name.empty())
        {
            if (args_count > 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage ElasticsearchQueue requires 3 arguments: url, index, cursor_field");

            CHECK_ELASTICSEARCH_QUEUE_ARGUMENT(1, elasticsearch_url)
            CHECK_ELASTICSEARCH_QUEUE_ARGUMENT(2, elasticsearch_index)
            CHECK_ELASTICSEARCH_QUEUE_ARGUMENT(3, elasticsearch_cursor_field)
        }

#undef CHECK_ELASTICSEARCH_QUEUE_ARGUMENT

        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_url].value.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_url cannot be empty");
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_index].value.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_index cannot be empty");
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_cursor_field].value.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_cursor_field cannot be empty");
        const auto auth_type = toLowerASCII((*settings)[ElasticsearchQueueSetting::elasticsearch_auth_type].value);
        if (auth_type != "auto" && auth_type != "none" && auth_type != "basic" && auth_type != "api_key" && auth_type != "bearer")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unsupported value '{}' for elasticsearch_auth_type. Supported values are: auto, none, basic, api_key, bearer",
                auth_type);
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_use_point_in_time].value
            && (*settings)[ElasticsearchQueueSetting::elasticsearch_pit_keep_alive].value.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_pit_keep_alive cannot be empty when elasticsearch_use_point_in_time is enabled");
        }
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_path].changed)
        {
            const auto & keeper_path = (*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_path].value;
            if (!keeper_path.empty() && (!keeper_path.starts_with('/') || keeper_path == "/"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_keeper_path must be an absolute non-root Keeper path");
        }
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_checkpoint_name].value.empty()
            || (*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_checkpoint_name].value.find('/') != String::npos)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_keeper_checkpoint_name must be a non-empty Keeper node name");
        }
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_checkpoint_name].value == "lock")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_keeper_checkpoint_name cannot be 'lock'");
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_max_block_size].changed
            && (*settings)[ElasticsearchQueueSetting::elasticsearch_max_block_size].value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_max_block_size cannot be lower than 1");
        }
        if ((*settings)[ElasticsearchQueueSetting::elasticsearch_poll_max_batch_size].changed
            && (*settings)[ElasticsearchQueueSetting::elasticsearch_poll_max_batch_size].value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_poll_max_batch_size cannot be lower than 1");
        }

        return std::make_shared<StorageElasticsearchQueue>(
            args.table_id,
            args.getContext(),
            args.columns,
            args.comment,
            std::move(settings),
            args.relative_data_path,
            args.mode,
            collection_name);
    };

    factory.registerStorage(
        ElasticsearchQueue::TABLE_ENGINE_NAME,
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .source_access_type = AccessTypeObjects::Source::ELASTICSEARCH,
            .has_builtin_setting_fn = ElasticsearchQueueSettings::hasBuiltin,
        });
}

}
