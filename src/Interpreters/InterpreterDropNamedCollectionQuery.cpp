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
        if (!dependents.empty())
        {
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

            if (!dependent_names.empty())
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_IS_USED,
                    "Named collection `{}` is used by tables: {}",
                    query.collection_name,
                    fmt::join(dependent_names, ", "));
            }
        }
    }

    NamedCollectionFactory::instance().removeFromSQL(query);

    return {};
}

void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropNamedCollectionQuery", create_fn);
}

}
