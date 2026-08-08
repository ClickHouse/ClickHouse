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

        /// A detached table is not in `DatabaseCatalog`, but the metadata it is attached from still
        /// references the collection: if the collection is dropped, the `ATTACH` replayed at the next
        /// server start throws `NAMED_COLLECTION_DOESNT_EXIST` and the server does not start.
        auto detached_dependents = NamedCollectionFactory::instance().getDetachedDependents(query.collection_name);
        if (!detached_dependents.empty())
        {
            std::vector<String> detached_names;
            detached_names.reserve(detached_dependents.size());
            for (const auto & dep : detached_dependents)
            {
                /// The entry is not removed when the dependencies of the table are registered again:
                /// that happens while the engine arguments are resolved, and the `ATTACH` can still
                /// fail after that, leaving the table detached with the entry as its only protection.
                /// A table that exists in the catalog proves the attach went through - such an entry
                /// only lingered here, and the dependency of the attached table was checked above.
                if (DatabaseCatalog::instance().isTableExist(dep, current_context))
                    NamedCollectionFactory::instance().removeDetachedDependencies(dep);
                else
                    detached_names.push_back(dep.getFullTableName());
            }

            if (!detached_names.empty())
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_IS_USED,
                    "Named collection `{}` may still be used by detached tables: {}",
                    query.collection_name,
                    fmt::join(detached_names, ", "));
            }
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
