#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>


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


void StorageFactory::registerStorage(const std::string & name, Creator creator)
{
    if (!storages.emplace(name, std::move(creator)).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}


StoragePtr StorageFactory::get(
    ASTCreateQuery & query,
    const String & data_path,
    const String & table_name,
    const String & database_name,
    Context & local_context,
    Context & context,
    const ColumnsDescription & columns,
    bool attach,
    bool has_force_restore_data_flag) const
{
    String name;
    ASTs args;
    ASTStorage * storage_def = query.storage;

    if (query.is_view)
    {
        if (query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        name = "View";
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
                args = engine_def.arguments->children;

            name = engine_def.name;

            if ((storage_def->partition_by || storage_def->order_by || storage_def->sample_by || storage_def->settings)
                && !endsWith(name, "MergeTree"))
            {
                throw Exception(
                    "Engine " + name + " doesn't support PARTITION BY, ORDER BY, SAMPLE BY or SETTINGS clauses. "
                    "Currently only the MergeTree family of engines supports them", ErrorCodes::BAD_ARGUMENTS);
            }

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
        }
    }

    auto it = storages.find(name);
    if (it == storages.end())
        throw Exception("Unknown table engine " + name, ErrorCodes::UNKNOWN_STORAGE);

    Arguments arguments
    {
        .engine_name = name,
        .engine_args = args,
        .storage_def = storage_def,
        .query = query,
        .data_path = data_path,
        .table_name = table_name,
        .database_name = database_name,
        .local_context = local_context,
        .context = context,
        .columns = columns,
        .attach = attach,
        .has_force_restore_data_flag = has_force_restore_data_flag
    };

    return it->second(arguments);
}

}
