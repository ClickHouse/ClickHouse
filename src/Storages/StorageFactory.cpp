#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/StorageID.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int ENGINE_REQUIRED;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int BAD_ARGUMENTS;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_TABLES;
}


/// Some types are only for intermediate values of expressions and cannot be used in tables.
static void checkAllTypesAreAllowedInTable(const NamesAndTypesList & names_and_types)
{
    for (const auto & elem : names_and_types)
        if (elem.type->cannotBeStoredInTables())
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_TABLES, "Data type {} cannot be used in tables", elem.type->getName());
}


ContextMutablePtr StorageFactory::Arguments::getContext() const
{
    auto ptr = context.lock();
    if (!ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");
    return ptr;
}

ContextMutablePtr StorageFactory::Arguments::getLocalContext() const
{
    auto ptr = local_context.lock();
    if (!ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");
    return ptr;
}


void StorageFactory::registerStorage(const std::string & name, CreatorFn creator_fn, StorageFeatures features)
{
    if (!storages.emplace(name, Creator{std::move(creator_fn), features}).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TableFunctionFactory: the table function name '{}' is not unique", name);
}


StoragePtr StorageFactory::get(
    const ASTCreateQuery & query,
    const String & relative_data_path,
    ContextMutablePtr local_context,
    ContextMutablePtr context,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    LoadingStrictnessLevel mode) const
{
    String name, comment;

    ASTStorage * storage_def = query.storage;

    bool has_engine_args = false;

    if (query.is_ordinary_view)
    {
        if (query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a View");

        name = "View";
    }
    else if (query.is_live_view)
    {
        if (query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a LiveView");

        name = "LiveView";
    }
    else if (query.is_dictionary)
    {
        if (query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a Dictionary");

        name = "Dictionary";
    }
    else
    {
        /// Check for some special types, that are not allowed to be stored in tables. Example: NULL data type.
        /// Exception: any type is allowed in View, because plain (non-materialized) View does not store anything itself.
        checkAllTypesAreAllowedInTable(columns.getAll());

        if (query.is_materialized_view)
        {
            name = "MaterializedView";
        }
        else if (query.is_window_view)
        {
            name = "WindowView";
        }
        else
        {
            if (!query.storage)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect CREATE query: storage required");

            if (!storage_def->engine)
                throw Exception(ErrorCodes::ENGINE_REQUIRED, "Incorrect CREATE query: ENGINE required");

            const ASTFunction & engine_def = *storage_def->engine;

            if (engine_def.parameters)
                throw Exception(ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS, "Engine definition cannot take the form of a parametric function");

            if (engine_def.arguments)
                has_engine_args = true;

            name = engine_def.name;

            if (name == "View")
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Direct creation of tables with ENGINE View is not supported, use CREATE VIEW statement");
            }
            else if (name == "MaterializedView")
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                                "Direct creation of tables with ENGINE MaterializedView "
                                "is not supported, use CREATE MATERIALIZED VIEW statement");
            }
            else if (name == "LiveView")
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                                "Direct creation of tables with ENGINE LiveView "
                                "is not supported, use CREATE LIVE VIEW statement");
            }
            else if (name == "WindowView")
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                                "Direct creation of tables with ENGINE WindowView "
                                "is not supported, use CREATE WINDOW VIEW statement");
            }

            auto it = storages.find(name);
            if (it == storages.end())
            {
                auto hints = getHints(name);
                if (!hints.empty())
                    throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown table engine {}. Maybe you meant: {}", name, toString(hints));
                else
                    throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown table engine {}", name);
            }

            auto check_feature = [&](String feature_description, FeatureMatcherFn feature_matcher_fn)
            {
                if (!feature_matcher_fn(it->second.features))
                {
                    String msg;
                    auto supporting_engines = getAllRegisteredNamesByFeatureMatcherFn(feature_matcher_fn);
                    for (size_t index = 0; index < supporting_engines.size(); ++index)
                    {
                        if (index)
                            msg += ", ";
                        msg += supporting_engines[index];
                    }
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine {} doesn't support {}. "
                                    "Currently only the following engines have support for the feature: [{}]",
                                    name, feature_description, msg);
                }
            };

            if (storage_def->settings)
                check_feature(
                    "SETTINGS clause",
                    [](StorageFeatures features) { return features.supports_settings; });

            if (storage_def->partition_by || storage_def->primary_key || storage_def->order_by || storage_def->sample_by)
                check_feature(
                    "PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses",
                    [](StorageFeatures features) { return features.supports_sort_order; });

            if (storage_def->ttl_table || !columns.getColumnTTLs().empty())
                check_feature(
                    "TTL clause",
                    [](StorageFeatures features) { return features.supports_ttl; });

            if (query.columns_list && query.columns_list->indices && !query.columns_list->indices->children.empty())
                check_feature(
                    "skipping indices",
                    [](StorageFeatures features) { return features.supports_skipping_indices; });

            if (query.columns_list && query.columns_list->projections && !query.columns_list->projections->children.empty())
                check_feature(
                    "projections",
                    [](StorageFeatures features) { return features.supports_projections; });
        }
    }

    if (query.comment)
        comment = query.comment->as<ASTLiteral &>().value.safeGet<String>();

    ASTs empty_engine_args;
    Arguments arguments{
        .engine_name = name,
        .engine_args = has_engine_args ? storage_def->engine->arguments->children : empty_engine_args,
        .storage_def = storage_def,
        .query = query,
        .relative_data_path = relative_data_path,
        .table_id = StorageID(query.getDatabase(), query.getTable(), query.uuid),
        .local_context = local_context,
        .context = context,
        .columns = columns,
        .constraints = constraints,
        .mode = mode,
        .comment = comment};

    assert(arguments.getContext() == arguments.getContext()->getGlobalContext());

    auto res = storages.at(name).creator_fn(arguments);
    if (!empty_engine_args.empty())
    {
        /// Storage creator modified empty arguments list, so we should modify the query
        assert(storage_def && storage_def->engine && !storage_def->engine->arguments);
        storage_def->engine->arguments = std::make_shared<ASTExpressionList>();
        storage_def->engine->children.push_back(storage_def->engine->arguments);
        storage_def->engine->arguments->children = empty_engine_args;
    }

    if (local_context->hasQueryContext() && local_context->getSettingsRef()[Setting::log_queries])
        local_context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Storage, name);

    return res;
}

StorageFactory & StorageFactory::instance()
{
    static StorageFactory ret;
    return ret;
}


AccessType StorageFactory::getSourceAccessType(const String & table_engine) const
{
    auto it = storages.find(table_engine);
    if (it == storages.end())
        return AccessType::NONE;
    return it->second.features.source_access_type;
}


const StorageFactory::StorageFeatures & StorageFactory::getStorageFeatures(const String & storage_name) const
{
    auto it = storages.find(storage_name);
    if (it == storages.end())
        throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown table engine {}", storage_name);
    return it->second.features;
}

}
