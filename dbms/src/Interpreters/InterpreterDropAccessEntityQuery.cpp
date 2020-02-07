#include <Interpreters/InterpreterDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <boost/range/algorithm/transform.hpp>


namespace DB
{
BlockIO InterpreterDropAccessEntityQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropAccessEntityQuery &>();
    auto & access_control = context.getAccessControlManager();
    using Kind = ASTDropAccessEntityQuery::Kind;

    switch (query.kind)
    {
        case Kind::USER:
        {
            context.checkAccess(AccessType::DROP_USER);
            if (query.if_exists)
                access_control.tryRemove(access_control.find<User>(query.names));
            else
                access_control.remove(access_control.getIDs<User>(query.names));
            return {};
        }

        case Kind::QUOTA:
        {
            context.checkAccess(AccessType::DROP_QUOTA);
            if (query.if_exists)
                access_control.tryRemove(access_control.find<Quota>(query.names));
            else
                access_control.remove(access_control.getIDs<Quota>(query.names));
            return {};
        }

        case Kind::ROW_POLICY:
        {
            context.checkAccess(AccessType::DROP_POLICY);
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
    }

    __builtin_unreachable();
}
}
