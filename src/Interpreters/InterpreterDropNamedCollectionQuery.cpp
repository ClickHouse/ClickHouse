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
                {
                    dependent_names.push_back(dep.getFullTableName());
                    continue;
                }

                /// The table is also absent while the `CREATE`/`ATTACH` that registered the dependency
                /// is still in flight: the registration happens while the engine arguments are resolved,
                /// before the table is committed to the catalog. Classifying such an entry as stale would
                /// let the drop proceed while the create later succeeds, leaving metadata that references
                /// a collection that no longer exists. The creating query holds the `DDLGuard` of the
                /// table name for the whole window between the registration and the commit, so re-check
                /// under that guard: once it is acquired, no create is in flight, and the table's absence
                /// proves the earlier create failed and left a stale entry.
                /// An empty database name identifies a dictionary defined in the configuration files; it
                /// is not created through DDL, so there is no guard to synchronize with.
                if (!dep.database_name.empty())
                {
                    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(dep.database_name, dep.table_name, nullptr);
                    if (DatabaseCatalog::instance().isTableExist(dep, current_context))
                    {
                        dependent_names.push_back(dep.getFullTableName());
                        continue;
                    }
                }

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
        /// Every entry blocks the drop, even when a table with that name exists in `DatabaseCatalog`
        /// again. The entry cannot be pruned here based on the table's existence: a concurrent `ATTACH`
        /// registers its live dependency after the check above already ran, and a concurrent `DETACH`
        /// records the entry while the table is still in the catalog - in both windows the table exists,
        /// yet nothing in this query has validated the live dependency. Entries are removed only by the
        /// events that prove the metadata under the recorded name is gone or harmless: `DROP TABLE`,
        /// `DETACH TABLE ... PERMANENTLY`, `RENAME` of the table, and `DROP DATABASE`.
        auto detached_dependents = NamedCollectionFactory::instance().getDetachedDependents(query.collection_name);
        if (!detached_dependents.empty())
        {
            std::vector<String> detached_names;
            detached_names.reserve(detached_dependents.size());
            for (const auto & dep : detached_dependents)
                detached_names.push_back(dep.getFullTableName());

            throw Exception(
                ErrorCodes::NAMED_COLLECTION_IS_USED,
                "Named collection `{}` may still be used by detached tables: {}",
                query.collection_name,
                fmt::join(detached_names, ", "));
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
