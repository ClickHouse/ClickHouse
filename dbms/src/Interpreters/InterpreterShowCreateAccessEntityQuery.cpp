#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsageInfo.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Common/StringUtils/StringUtils.h>
#include <ext/range.h>
#include <sstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    ASTPtr getCreateQueryImpl(
        const User & user,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        auto query = std::make_shared<ASTCreateUserQuery>();
        query->name = user.getName();
        query->attach = attach_mode;

        if (user.allowed_client_hosts != AllowedClientHosts::AnyHostTag{})
            query->hosts = user.allowed_client_hosts;

        if (!user.profile.empty())
            query->profile = user.profile;

        if (user.default_roles != ExtendedRoleSet::AllTag{})
        {
            if (attach_mode)
                query->default_roles = ExtendedRoleSet{user.default_roles}.toAST();
            else
                query->default_roles = ExtendedRoleSet{user.default_roles}.toASTWithNames(*manager);
        }

        if (attach_mode && (user.authentication.getType() != Authentication::NO_PASSWORD))
        {
            /// We don't show password unless it's an ATTACH statement.
            query->authentication = user.authentication;
        }
        return query;
    }


    ASTPtr getCreateQueryImpl(const Role & role, const AccessControlManager *, bool attach_mode = false)
    {
        auto query = std::make_shared<ASTCreateRoleQuery>();
        query->name = role.getName();
        query->attach = attach_mode;
        return query;
    }


    ASTPtr getCreateQueryImpl(
        const Quota & quota,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        auto query = std::make_shared<ASTCreateQuotaQuery>();
        query->name = quota.getName();
        query->attach = attach_mode;

        query->key_type = quota.key_type;
        query->all_limits.reserve(quota.all_limits.size());

        for (const auto & limits : quota.all_limits)
        {
            ASTCreateQuotaQuery::Limits create_query_limits;
            create_query_limits.duration = limits.duration;
            create_query_limits.randomize_interval = limits.randomize_interval;
            for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
                if (limits.max[resource_type])
                    create_query_limits.max[resource_type] = limits.max[resource_type];
            query->all_limits.push_back(create_query_limits);
        }

        if (!quota.to_roles.empty())
        {
            if (attach_mode)
                query->roles = quota.to_roles.toAST();
            else
                query->roles = quota.to_roles.toASTWithNames(*manager);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(
        const RowPolicy & policy,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        auto query = std::make_shared<ASTCreateRowPolicyQuery>();
        query->name_parts = RowPolicy::FullNameParts{policy.getDatabase(), policy.getTableName(), policy.getName()};
        query->attach = attach_mode;

        if (policy.isRestrictive())
            query->is_restrictive = policy.isRestrictive();

        for (auto index : ext::range_with_static_cast<RowPolicy::ConditionType>(RowPolicy::MAX_CONDITION_TYPE))
        {
            const auto & condition = policy.conditions[index];
            if (!condition.empty())
            {
                ParserExpression parser;
                ASTPtr expr = parseQuery(parser, condition, 0);
                query->conditions.push_back(std::pair{index, expr});
            }
        }

        if (!policy.to_roles.empty())
        {
            if (attach_mode)
                query->roles = policy.to_roles.toAST();
            else
                query->roles = policy.to_roles.toASTWithNames(*manager);
        }

        return query;
    }

    ASTPtr getCreateQueryImpl(
        const IAccessEntity & entity,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getCreateQueryImpl(*user, manager, attach_mode);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getCreateQueryImpl(*role, manager, attach_mode);
        if (const RowPolicy * policy = typeid_cast<const RowPolicy *>(&entity))
            return getCreateQueryImpl(*policy, manager, attach_mode);
        if (const Quota * quota = typeid_cast<const Quota *>(&entity))
            return getCreateQueryImpl(*quota, manager, attach_mode);
        throw Exception("Unexpected type of access entity: " + entity.getTypeName(), ErrorCodes::LOGICAL_ERROR);
    }
}


BlockIO InterpreterShowCreateAccessEntityQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowCreateAccessEntityQuery::executeImpl()
{
    const auto & show_query = query_ptr->as<ASTShowCreateAccessEntityQuery &>();

    /// Build a create query.
    ASTPtr create_query = getCreateQuery(show_query);

    /// Build the result column.
    std::stringstream create_query_ss;
    formatAST(*create_query, create_query_ss, false, true);
    String create_query_str = create_query_ss.str();
    MutableColumnPtr column = ColumnString::create();
    column->insert(create_query_str);

    /// Prepare description of the result column.
    std::stringstream desc_ss;
    formatAST(show_query, desc_ss, false, true);
    String desc = desc_ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateQuery(const ASTShowCreateAccessEntityQuery & show_query) const
{
    const auto & access_control = context.getAccessControlManager();
    using Kind = ASTShowCreateAccessEntityQuery::Kind;
    switch (show_query.kind)
    {
        case Kind::USER:
        {
            UserPtr user;
            if (show_query.current_user)
                user = context.getUser();
            else
                user = access_control.read<User>(show_query.name);
            return getCreateQueryImpl(*user, &access_control);
        }

        case Kind::QUOTA:
        {
            QuotaPtr quota;
            if (show_query.current_quota)
                quota = access_control.read<Quota>(context.getQuota()->getUsageInfo().quota_id);
            else
                quota = access_control.read<Quota>(show_query.name);
            return getCreateQueryImpl(*quota, &access_control);
        }

        case Kind::ROW_POLICY:
        {
            RowPolicyPtr policy = access_control.read<RowPolicy>(show_query.row_policy_name.getFullName(context));
            return getCreateQueryImpl(*policy, &access_control);
        }
    }
    __builtin_unreachable();
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getAttachQuery(const IAccessEntity & entity)
{
    return getCreateQueryImpl(entity, nullptr, true);
}

}
