#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/StorageID.h>

namespace DB
{

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
            throw Exception("Data type " + elem.type->getName() + " cannot be used in tables", ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_TABLES);
}


void StorageFactory::registerStorage(const std::string & name, CreatorFn creator_fn, StorageFeatures features)
{
    if (!storages.emplace(name, Creator{std::move(creator_fn), features}).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}


StoragePtr StorageFactory::get(
    const ASTCreateQuery & query,
    const String & relative_data_path,
    Context & local_context,
    Context & context,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    bool has_force_restore_data_flag) const
{
    String name;
    ASTStorage * storage_def = query.storage;

    bool has_engine_args = false;

    if (query.is_view)
    {
        if (query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        name = "View";
    }
    else if (query.is_live_view)
    {

        if (query.storage)
            throw Exception("Specifying ENGINE is not allowed for a LiveView", ErrorCodes::INCORRECT_QUERY);

        name = "LiveView";
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
        else
        {
            if (!storage_def)
                throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

            const ASTFunction & engine_def = *storage_def->engine;

            if (engine_def.parameters)
                throw Exception(
                    "Engine definition cannot take the form of a parametric function", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

            if (engine_def.arguments)
                has_engine_args = true;

            name = engine_def.name;

            if (name == "View")
            {
                throw Exception(
                    "Direct creation of tables with ENGINE View is not supported, use CREATE VIEW statement",
                    ErrorCodes::INCORRECT_QUERY);
            }
            else if (name == "MaterializedView")
            {
                throw Exception(
                    "Direct creation of tables with ENGINE MaterializedView is not supported, use CREATE MATERIALIZED VIEW statement",
                    ErrorCodes::INCORRECT_QUERY);
            }
            else if (name == "LiveView")
            {
                throw Exception(
                    "Direct creation of tables with ENGINE LiveView is not supported, use CREATE LIVE VIEW statement",
                    ErrorCodes::INCORRECT_QUERY);
            }

            auto it = storages.find(name);
            if (it == storages.end())
            {
                auto hints = getHints(name);
                if (!hints.empty())
                    throw Exception("Unknown table engine " + name + ". Maybe you meant: " + toString(hints), ErrorCodes::UNKNOWN_STORAGE);
                else
                    throw Exception("Unknown table engine " + name, ErrorCodes::UNKNOWN_STORAGE);
            }

            auto check_feature = [&](String feature_description, FeatureMatcherFn feature_matcher_fn)
            {
                if (!feature_matcher_fn(it->second.features))
                {
                    String msg = "Engine " + name + " doesn't support " + feature_description + ". "
                        "Currently only the following engines have support for the feature: [";
                    auto supporting_engines = getAllRegisteredNamesByFeatureMatcherFn(feature_matcher_fn);
                    for (size_t index = 0; index < supporting_engines.size(); ++index)
                    {
                        if (index)
                            msg += ", ";
                        msg += supporting_engines[index];
                    }
                    msg += "]";
                    throw Exception(msg, ErrorCodes::BAD_ARGUMENTS);
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
        }
    }

    ASTs empty_engine_args;
    Arguments arguments
    {
        .engine_name = name,
        .engine_args = has_engine_args ? storage_def->engine->arguments->children : empty_engine_args,
        .storage_def = storage_def,
        .query = query,
        .relative_data_path = relative_data_path,
        .table_id = StorageID(query.database, query.table, query.uuid),
        .local_context = local_context,
        .context = context,
        .columns = columns,
        .constraints = constraints,
        .attach = query.attach,
        .has_force_restore_data_flag = has_force_restore_data_flag
    };

    auto res = storages.at(name).creator_fn(arguments);
    if (!empty_engine_args.empty())
    {
        /// Storage creator modified empty arguments list, so we should modify the query
        assert(storage_def && storage_def->engine && !storage_def->engine->arguments);
        storage_def->engine->arguments = std::make_shared<ASTExpressionList>();
        storage_def->engine->children.push_back(storage_def->engine->arguments);
        storage_def->engine->arguments->children = empty_engine_args;
    }
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

}
