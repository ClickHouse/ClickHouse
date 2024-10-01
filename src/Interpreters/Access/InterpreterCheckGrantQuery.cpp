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
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include "Storages/IStorage.h"

namespace DB
{

BlockIO InterpreterCheckGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTCheckGrantQuery &>();
    query.access_rights_elements.eraseNonGrantable();

    auto current_user_access = getContext()->getAccess();

    /// Collect access rights elements which will be checked.
    AccessRightsElements & elements_to_check_grant = query.access_rights_elements;

    /// Replacing empty database with the default. This step must be done before replication to avoid privilege escalation.
    String current_database = getContext()->getCurrentDatabase();
    elements_to_check_grant.replaceEmptyDatabase(current_database);
    query.access_rights_elements.replaceEmptyDatabase(current_database);
    bool user_is_granted = current_user_access->isGranted(elements_to_check_grant);
    BlockIO res;
    res.pipeline = QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(Block{{ColumnUInt8::create(1, user_is_granted), std::make_shared<DataTypeUInt8>(), "result"}}));
    return res;
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
