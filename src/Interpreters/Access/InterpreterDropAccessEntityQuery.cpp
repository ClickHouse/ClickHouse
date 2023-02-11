#include <Interpreters/Access/InterpreterDropAccessEntityQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


BlockIO InterpreterDropAccessEntityQuery::execute()
{
    auto & query = query_ptr->as<ASTDropAccessEntityQuery &>();
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    auto do_drop = [&](const Strings & names)
    {
        if (query.if_exists)
            access_control.tryRemove(access_control.find(query.type, names));
        else
            access_control.remove(access_control.getIDs(query.type, names));
    };

    if (query.type == AccessEntityType::ROW_POLICY)
        do_drop(query.row_policy_names->toStrings());
    else
        do_drop(query.names);

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
    throw Exception(
        toString(query.type) + ": type is not supported by DROP query", ErrorCodes::NOT_IMPLEMENTED);
}

}
