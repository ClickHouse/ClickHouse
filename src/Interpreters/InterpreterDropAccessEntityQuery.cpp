#include <Interpreters/InterpreterDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using EntityType = IAccessEntity::Type;


BlockIO InterpreterDropAccessEntityQuery::execute()
{
    auto & query = query_ptr->as<ASTDropAccessEntityQuery &>();
    auto & access_control = context.getAccessControlManager();
    context.checkAccess(getRequiredAccess());

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context);

    query.replaceEmptyDatabaseWithCurrent(context.getCurrentDatabase());

    auto do_drop = [&](const Strings & names)
    {
        if (query.if_exists)
            access_control.tryRemove(access_control.find(query.type, names));
        else
            access_control.remove(access_control.getIDs(query.type, names));
    };

    if (query.type == EntityType::ROW_POLICY)
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
        case EntityType::USER: res.emplace_back(AccessType::DROP_USER); return res;
        case EntityType::ROLE: res.emplace_back(AccessType::DROP_ROLE); return res;
        case EntityType::SETTINGS_PROFILE: res.emplace_back(AccessType::DROP_SETTINGS_PROFILE); return res;
        case EntityType::ROW_POLICY: res.emplace_back(AccessType::DROP_ROW_POLICY); return res;
        case EntityType::QUOTA: res.emplace_back(AccessType::DROP_QUOTA); return res;
        case EntityType::MAX: break;
    }
    throw Exception(
        toString(query.type) + ": type is not supported by DROP query", ErrorCodes::NOT_IMPLEMENTED);
}

}
