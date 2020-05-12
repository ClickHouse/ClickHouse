#include <Interpreters/InterpreterDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>
#include <boost/range/algorithm/transform.hpp>


namespace DB
{
namespace
{
    using Kind = ASTDropAccessEntityQuery::Kind;

    std::type_index getType(Kind kind)
    {
        switch (kind)
        {
            case Kind::USER: return typeid(User);
            case Kind::ROLE: return typeid(Role);
            case Kind::QUOTA: return typeid(Quota);
            case Kind::ROW_POLICY: return typeid(RowPolicy);
            case Kind::SETTINGS_PROFILE: return typeid(SettingsProfile);
        }
        __builtin_unreachable();
    }

    AccessType getRequiredAccessType(Kind kind)
    {
        switch (kind)
        {
            case Kind::USER: return AccessType::DROP_USER;
            case Kind::ROLE: return AccessType::DROP_ROLE;
            case Kind::QUOTA: return AccessType::DROP_QUOTA;
            case Kind::ROW_POLICY: return AccessType::DROP_ROW_POLICY;
            case Kind::SETTINGS_PROFILE: return AccessType::DROP_SETTINGS_PROFILE;
        }
        __builtin_unreachable();
    }
}

BlockIO InterpreterDropAccessEntityQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropAccessEntityQuery &>();
    auto & access_control = context.getAccessControlManager();

    std::type_index type = getType(query.kind);
    context.checkAccess(getRequiredAccessType(query.kind));

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context);

    if (query.kind == Kind::ROW_POLICY)
    {
        Strings full_names;
        boost::range::transform(
            query.row_policies_names, std::back_inserter(full_names),
            [this](const RowPolicy::FullNameParts & row_policy_name) { return row_policy_name.getFullName(context); });
        if (query.if_exists)
            access_control.tryRemove(access_control.find<RowPolicy>(full_names));
        else
            access_control.remove(access_control.getIDs<RowPolicy>(full_names));
        return {};
    }

    if (query.if_exists)
        access_control.tryRemove(access_control.find(type, query.names));
    else
        access_control.remove(access_control.getIDs(type, query.names));
    return {};
}

}
