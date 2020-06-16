#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Defines.h>
#include <ext/range.h>
#include <sstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    ASTPtr getCreateQueryImpl(
        const User & user,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateUserQuery>();
        query->name = user.getName();
        query->attach = attach_mode;

        if (user.allowed_client_hosts != AllowedClientHosts::AnyHostTag{})
            query->hosts = user.allowed_client_hosts;

        if (user.default_roles != ExtendedRoleSet::AllTag{})
        {
            if (attach_mode)
                query->default_roles = user.default_roles.toAST();
            else
                query->default_roles = user.default_roles.toASTWithNames(*manager);
        }

        if (user.authentication.getType() != Authentication::NO_PASSWORD)
        {
            query->authentication = user.authentication;
            query->show_password = attach_mode; /// We don't show password unless it's an ATTACH statement.
        }

        if (!user.settings.empty())
        {
            if (attach_mode)
                query->settings = user.settings.toAST();
            else
                query->settings = user.settings.toASTWithNames(*manager);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(const Role & role, const AccessControlManager * manager, bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateRoleQuery>();
        query->name = role.getName();
        query->attach = attach_mode;

        if (!role.settings.empty())
        {
            if (attach_mode)
                query->settings = role.settings.toAST();
            else
                query->settings = role.settings.toASTWithNames(*manager);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(const SettingsProfile & profile, const AccessControlManager * manager, bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateSettingsProfileQuery>();
        query->name = profile.getName();
        query->attach = attach_mode;

        if (!profile.elements.empty())
        {
            if (attach_mode)
                query->settings = profile.elements.toAST();
            else
                query->settings = profile.elements.toASTWithNames(*manager);
            if (query->settings)
                query->settings->setUseInheritKeyword(true);
        }

        if (!profile.to_roles.empty())
        {
            if (attach_mode)
                query->to_roles = profile.to_roles.toAST();
            else
                query->to_roles = profile.to_roles.toASTWithNames(*manager);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(
        const Quota & quota,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode)
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
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateRowPolicyQuery>();
        query->name_parts = policy.getNameParts();
        query->attach = attach_mode;

        if (policy.isRestrictive())
            query->is_restrictive = policy.isRestrictive();

        for (auto type : ext::range(RowPolicy::MAX_CONDITION_TYPE))
        {
            const auto & condition = policy.conditions[static_cast<size_t>(type)];
            if (!condition.empty())
            {
                ParserExpression parser;
                ASTPtr expr = parseQuery(parser, condition, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                query->conditions[static_cast<size_t>(type)] = expr;
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
        bool attach_mode)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getCreateQueryImpl(*user, manager, attach_mode);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getCreateQueryImpl(*role, manager, attach_mode);
        if (const RowPolicy * policy = typeid_cast<const RowPolicy *>(&entity))
            return getCreateQueryImpl(*policy, manager, attach_mode);
        if (const Quota * quota = typeid_cast<const Quota *>(&entity))
            return getCreateQueryImpl(*quota, manager, attach_mode);
        if (const SettingsProfile * profile = typeid_cast<const SettingsProfile *>(&entity))
            return getCreateQueryImpl(*profile, manager, attach_mode);
        throw Exception(entity.outputTypeAndName() + ": type is not supported by SHOW CREATE query", ErrorCodes::NOT_IMPLEMENTED);
    }

    using EntityType = IAccessEntity::Type;
}


InterpreterShowCreateAccessEntityQuery::InterpreterShowCreateAccessEntityQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_), ignore_quota(query_ptr->as<ASTShowCreateAccessEntityQuery &>().type == EntityType::QUOTA)
{
}


BlockIO InterpreterShowCreateAccessEntityQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowCreateAccessEntityQuery::executeImpl()
{
    auto & show_query = query_ptr->as<ASTShowCreateAccessEntityQuery &>();

    /// Build a create query.
    ASTPtr create_query = getCreateQuery(show_query);

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    if (create_query)
    {
        std::stringstream create_query_ss;
        formatAST(*create_query, create_query_ss, false, true);
        String create_query_str = create_query_ss.str();
        column->insert(create_query_str);
    }

    /// Prepare description of the result column.
    std::stringstream desc_ss;
    formatAST(show_query, desc_ss, false, true);
    String desc = desc_ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateQuery(ASTShowCreateAccessEntityQuery & show_query) const
{
    const auto & access_control = context.getAccessControlManager();
    context.checkAccess(getRequiredAccess());

    if (show_query.current_user)
    {
        auto user = context.getUser();
        if (!user)
            return nullptr;
        return getCreateQueryImpl(*user, &access_control, false);
    }

    if (show_query.current_quota)
    {
        auto usage = context.getQuotaUsage();
        if (!usage)
            return nullptr;
        auto quota = access_control.read<Quota>(usage->quota_id);
        return getCreateQueryImpl(*quota, &access_control, false);
    }

    if (show_query.type == EntityType::ROW_POLICY)
    {
        if (show_query.row_policy_name_parts.database.empty())
            show_query.row_policy_name_parts.database = context.getCurrentDatabase();
        RowPolicyPtr policy = access_control.read<RowPolicy>(show_query.row_policy_name_parts.getName());
        return getCreateQueryImpl(*policy, &access_control, false);
    }

    auto entity = access_control.read(access_control.getID(show_query.type, show_query.name));
    return getCreateQueryImpl(*entity, &access_control, false);
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getAttachQuery(const IAccessEntity & entity)
{
    return getCreateQueryImpl(entity, nullptr, true);
}


AccessRightsElements InterpreterShowCreateAccessEntityQuery::getRequiredAccess() const
{
    const auto & show_query = query_ptr->as<const ASTShowCreateAccessEntityQuery &>();
    AccessRightsElements res;
    switch (show_query.type)
    {
        case EntityType::USER: res.emplace_back(AccessType::SHOW_USERS); return res;
        case EntityType::ROLE: res.emplace_back(AccessType::SHOW_ROLES); return res;
        case EntityType::SETTINGS_PROFILE: res.emplace_back(AccessType::SHOW_SETTINGS_PROFILES); return res;
        case EntityType::ROW_POLICY: res.emplace_back(AccessType::SHOW_ROW_POLICIES); return res;
        case EntityType::QUOTA: res.emplace_back(AccessType::SHOW_QUOTAS); return res;
        case EntityType::MAX: break;
    }
    throw Exception(toString(show_query.type) + ": type is not supported by SHOW CREATE query", ErrorCodes::NOT_IMPLEMENTED);
}
}
