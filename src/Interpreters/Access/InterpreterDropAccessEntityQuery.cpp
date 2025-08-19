#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterDropAccessEntityQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


BlockIO InterpreterDropAccessEntityQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTDropAccessEntityQuery &>();

    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    auto do_drop = [&](const Strings & names, const String & storage_name)
    {
        IAccessStorage * storage = &access_control;
        MultipleAccessStorage::StoragePtr storage_ptr;
        if (!storage_name.empty())
        {
            storage_ptr = access_control.getStorageByName(storage_name);
            storage = storage_ptr.get();
        }

        if (query.if_exists)
            storage->tryRemove(storage->find(query.type, names));
        else
            storage->remove(storage->getIDs(query.type, names));
    };

    if (query.type == AccessEntityType::ROW_POLICY)
        do_drop(query.row_policy_names->toStrings(), query.storage_name);
    else
        do_drop(query.names, query.storage_name);

    return {};
}


AccessRightsElements InterpreterDropAccessEntityQuery::getRequiredAccess() const
{
    const auto & query = query_ptr->as<const ASTDropAccessEntityQuery &>();
    AccessRightsElements res;
    switch (query.type)
    {
        case AccessEntityType::USER:
        {
            res.emplace_back(AccessType::DROP_USER);
            return res;
        }
        case AccessEntityType::ROLE:
        {
            res.emplace_back(AccessType::DROP_ROLE);
            return res;
        }
        case AccessEntityType::SETTINGS_PROFILE:
        {
            res.emplace_back(AccessType::DROP_SETTINGS_PROFILE);
            return res;
        }
        case AccessEntityType::ROW_POLICY:
        {
            if (query.row_policy_names)
            {
                for (const auto & row_policy_name : query.row_policy_names->full_names)
                    res.emplace_back(AccessType::DROP_ROW_POLICY, row_policy_name.database, row_policy_name.table_name);
            }
            return res;
        }
        case AccessEntityType::QUOTA:
        {
            res.emplace_back(AccessType::DROP_QUOTA);
            return res;
        }
        case AccessEntityType::MAX:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: type is not supported by DROP query", toString(query.type));
}

void registerInterpreterDropAccessEntityQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropAccessEntityQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropAccessEntityQuery", create_fn);
}

}
