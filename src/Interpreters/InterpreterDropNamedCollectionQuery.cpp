#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/quoteString.h>

#include <fmt/ranges.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool check_named_collection_dependencies;
}

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_IS_USED;
}

BlockIO InterpreterDropNamedCollectionQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTDropNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION, query.collection_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    if (current_context->getSettingsRef()[Setting::check_named_collection_dependencies])
    {
        auto dependents = NamedCollectionFactory::instance().getDependents(query.collection_name);

        /// Filter out tables that no longer exist (e.g. from failed CREATE TABLE).
        /// Dependencies are registered during table configuration parsing, before the table
        /// is fully created. If CREATE TABLE fails after that point, a stale dependency remains.
        std::vector<String> dependent_names;
        dependent_names.reserve(dependents.size());
        for (const auto & dep : dependents)
        {
            if (DatabaseCatalog::instance().isTableExist(dep, current_context))
                dependent_names.push_back(dep.getFullTableName());
            else
                NamedCollectionFactory::instance().removeDependencies(dep);
        }

        /// The engine of a database registers the dependency before the database is attached, so a failed
        /// CREATE DATABASE leaves a stale one too. It is skipped, not removed: the database may be being created now.
        std::vector<String> dependent_database_names;
        for (const auto & database_name : NamedCollectionFactory::instance().getDependentDatabases(query.collection_name))
        {
            if (DatabaseCatalog::instance().isDatabaseExist(database_name))
                dependent_database_names.push_back(backQuoteIfNeed(database_name));
        }

        std::vector<String> users;
        if (!dependent_names.empty())
            users.push_back(fmt::format("tables: {}", fmt::join(dependent_names, ", ")));
        if (!dependent_database_names.empty())
            users.push_back(fmt::format("databases: {}", fmt::join(dependent_database_names, ", ")));

        if (!users.empty())
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_IS_USED,
                "Named collection `{}` is used by {}",
                query.collection_name,
                fmt::join(users, "; "));
        }
    }

    NamedCollectionFactory::instance().removeFromSQL(query);

    return {};
}

void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory);
void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropNamedCollectionQuery", create_fn);
}

}
