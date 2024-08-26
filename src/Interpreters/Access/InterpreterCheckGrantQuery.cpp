#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCheckGrantQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <Interpreters/DatabaseCatalog.h>
#include "Databases/IDatabase.h"
#include "Storages/IStorage.h"

namespace DB
{

namespace
{
    /// Extracts access rights elements which are going to check grant
    void collectAccessRightsElementsToGrantOrRevoke(
        const ASTCheckGrantQuery & query,
        AccessRightsElements & elements_to_check_grant)
    {
        elements_to_check_grant.clear();
        /// GRANT
        elements_to_check_grant = query.access_rights_elements;
    }
}


BlockIO InterpreterCheckGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTCheckGrantQuery &>();
    query.access_rights_elements.eraseNonGrantable();

    auto current_user_access = getContext()->getAccess();

    /// Collect access rights elements which will be checked.
    AccessRightsElements elements_to_check_grant;
    collectAccessRightsElementsToGrantOrRevoke(query, elements_to_check_grant);


    /// Replacing empty database with the default. This step must be done before replication to avoid privilege escalation.
    String current_database = getContext()->getCurrentDatabase();
    elements_to_check_grant.replaceEmptyDatabase(current_database);
    query.access_rights_elements.replaceEmptyDatabase(current_database);
    auto *logger = &::Poco::Logger::get("CheckGrantQuery");

    /// Check If Table/Columns exist.
    for (const auto & elem : elements_to_check_grant)
    {
        try
        {
            DatabasePtr database;
            database = DatabaseCatalog::instance().getDatabase(elem.database);
            if (!database->isTableExist(elem.table, getContext()))
            {
                /// Table not found.
                return executeQuery("SELECT 0 AS CHECK_GRANT", getContext(), QueryFlags{.internal = true}).second;
            }
            auto table = database->getTable(elem.table, getContext());

            auto column_name_with_sizes = table->getColumnSizes();
            for (const auto & elem_col : elem.columns)
            {
                bool founded = false;
                for (const auto & col_in_table : column_name_with_sizes)
                {
                    if (col_in_table.first == elem_col)
                    {
                        founded = true;
                        break;
                    }
                }
                if (!founded)
                {
                    /// Column not found.
                    return executeQuery("SELECT 0 AS CHECK_GRANT", getContext(), QueryFlags{.internal = true}).second;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(logger);
            return executeQuery("SELECT 0 AS CHECK_GRANT", getContext(), QueryFlags{.internal = true}).second;
        }
    }
    bool user_is_granted = current_user_access->isGranted(elements_to_check_grant);
    if (!user_is_granted)
    {
        return executeQuery("SELECT 0 AS CHECK_GRANT", getContext(), QueryFlags{.internal = true}).second;
    }

    return executeQuery("SELECT 1 AS CHECK_GRANT", getContext(), QueryFlags{.internal = true}).second;
}

void registerInterpreterCheckGrantQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCheckGrantQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCheckGrantQuery", create_fn);
}

}
