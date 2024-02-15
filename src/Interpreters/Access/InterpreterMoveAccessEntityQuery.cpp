#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/executeDDLQueryOnCluster.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ACCESS_ENTITY_NOT_FOUND;
}


BlockIO InterpreterMoveAccessEntityQuery::execute()
{
    auto & query = query_ptr->as<ASTMoveAccessEntityQuery &>();
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    std::vector<UUID> ids;
    if (query.type == AccessEntityType::ROW_POLICY)
        ids = access_control.getIDs(query.type, query.row_policy_names->toStrings());
    else
        ids = access_control.getIDs(query.type, query.names);

    /// Validate that all entities are from the same storage.
    const auto source_storage = access_control.findStorage(ids.front());
    if (!source_storage->exists(ids))
        throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "All access entities must be from the same storage in order to be moved");

    access_control.moveAccessEntities(ids, source_storage->getStorageName(), query.storage_name);
    return {};
}


AccessRightsElements InterpreterMoveAccessEntityQuery::getRequiredAccess() const
{
    const auto & query = query_ptr->as<const ASTMoveAccessEntityQuery &>();
    AccessRightsElements res;
    switch (query.type)
    {
        case AccessEntityType::USER:
        {
            res.emplace_back(AccessType::DROP_USER);
            res.emplace_back(AccessType::CREATE_USER);
            return res;
        }
        case AccessEntityType::ROLE:
        {
            res.emplace_back(AccessType::DROP_ROLE);
            res.emplace_back(AccessType::CREATE_ROLE);
            return res;
        }
        case AccessEntityType::SETTINGS_PROFILE:
        {
            res.emplace_back(AccessType::DROP_SETTINGS_PROFILE);
            res.emplace_back(AccessType::CREATE_SETTINGS_PROFILE);
            return res;
        }
        case AccessEntityType::ROW_POLICY:
        {
            if (query.row_policy_names)
            {
                for (const auto & row_policy_name : query.row_policy_names->full_names)
                {
                    res.emplace_back(AccessType::DROP_ROW_POLICY, row_policy_name.database, row_policy_name.table_name);
                    res.emplace_back(AccessType::CREATE_ROW_POLICY, row_policy_name.database, row_policy_name.table_name);
                }
            }
            return res;
        }
        case AccessEntityType::QUOTA:
        {
            res.emplace_back(AccessType::DROP_QUOTA);
            res.emplace_back(AccessType::CREATE_QUOTA);
            return res;
        }
        case AccessEntityType::MAX:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: type is not supported by DROP query", toString(query.type));
}

void registerInterpreterMoveAccessEntityQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterMoveAccessEntityQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterMoveAccessEntityQuery", create_fn);
}

}
