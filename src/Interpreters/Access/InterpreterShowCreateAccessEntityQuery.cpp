#include <Interpreters/Access/InterpreterShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Access/AccessControl.h>
#include <Access/EnabledQuota.h>
#include <Access/Quota.h>
#include <Access/QuotaUsage.h>
#include <Access/Role.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>
#include <Access/User.h>
#include <Columns/ColumnString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <base/range.h>
#include <base/sort.h>


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
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateUserQuery>();
        query->names = std::make_shared<ASTUserNamesWithHost>();
        query->names->push_back(user.getName());
        query->attach = attach_mode;

        if (user.allowed_client_hosts != AllowedClientHosts::AnyHostTag{})
            query->hosts = user.allowed_client_hosts;

        if (user.default_roles != RolesOrUsersSet::AllTag{})
        {
            if (attach_mode)
                query->default_roles = user.default_roles.toAST();
            else
                query->default_roles = user.default_roles.toASTWithNames(*access_control);
        }

        if (user.auth_data.getType() != AuthenticationType::NO_PASSWORD)
        {
            query->auth_data = user.auth_data;
            query->show_password = attach_mode; /// We don't show password unless it's an ATTACH statement.
        }

        if (!user.settings.empty())
        {
            if (attach_mode)
                query->settings = user.settings.toAST();
            else
                query->settings = user.settings.toASTWithNames(*access_control);
        }

        if (user.grantees != RolesOrUsersSet::AllTag{})
        {
            if (attach_mode)
                query->grantees = user.grantees.toAST();
            else
                query->grantees = user.grantees.toASTWithNames(*access_control);
            query->grantees->use_keyword_any = true;
        }

        if (!user.default_database.empty())
        {
            auto ast = std::make_shared<ASTDatabaseOrNone>();
            ast->database_name = user.default_database;
            query->default_database = ast;
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(const Role & role, const AccessControl * access_control, bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateRoleQuery>();
        query->names.emplace_back(role.getName());
        query->attach = attach_mode;

        if (!role.settings.empty())
        {
            if (attach_mode)
                query->settings = role.settings.toAST();
            else
                query->settings = role.settings.toASTWithNames(*access_control);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(const SettingsProfile & profile, const AccessControl * access_control, bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateSettingsProfileQuery>();
        query->names.emplace_back(profile.getName());
        query->attach = attach_mode;

        if (!profile.elements.empty())
        {
            if (attach_mode)
                query->settings = profile.elements.toAST();
            else
                query->settings = profile.elements.toASTWithNames(*access_control);
            if (query->settings)
                query->settings->setUseInheritKeyword(true);
        }

        if (!profile.to_roles.empty())
        {
            if (attach_mode)
                query->to_roles = profile.to_roles.toAST();
            else
                query->to_roles = profile.to_roles.toASTWithNames(*access_control);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(
        const Quota & quota,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateQuotaQuery>();
        query->names.emplace_back(quota.getName());
        query->attach = attach_mode;

        if (quota.key_type != QuotaKeyType::NONE)
            query->key_type = quota.key_type;

        query->all_limits.reserve(quota.all_limits.size());

        for (const auto & limits : quota.all_limits)
        {
            ASTCreateQuotaQuery::Limits create_query_limits;
            create_query_limits.duration = limits.duration;
            create_query_limits.randomize_interval = limits.randomize_interval;
            for (auto quota_type : collections::range(QuotaType::MAX))
            {
                auto quota_type_i = static_cast<size_t>(quota_type);
                create_query_limits.max[quota_type_i] = limits.max[quota_type_i];
            }
            query->all_limits.push_back(create_query_limits);
        }

        if (!quota.to_roles.empty())
        {
            if (attach_mode)
                query->roles = quota.to_roles.toAST();
            else
                query->roles = quota.to_roles.toASTWithNames(*access_control);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(
        const RowPolicy & policy,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateRowPolicyQuery>();
        query->names = std::make_shared<ASTRowPolicyNames>();
        query->names->full_names.emplace_back(policy.getFullName());
        query->attach = attach_mode;

        if (policy.isRestrictive())
            query->is_restrictive = policy.isRestrictive();

        for (auto type : collections::range(RowPolicyFilterType::MAX))
        {
            const auto & filter = policy.filters[static_cast<size_t>(type)];
            if (!filter.empty())
            {
                ParserExpression parser;
                ASTPtr expr = parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                query->filters.emplace_back(type, std::move(expr));
            }
        }

        if (!policy.to_roles.empty())
        {
            if (attach_mode)
                query->roles = policy.to_roles.toAST();
            else
                query->roles = policy.to_roles.toASTWithNames(*access_control);
        }

        return query;
    }

    ASTPtr getCreateQueryImpl(
        const IAccessEntity & entity,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getCreateQueryImpl(*user, access_control, attach_mode);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getCreateQueryImpl(*role, access_control, attach_mode);
        if (const RowPolicy * policy = typeid_cast<const RowPolicy *>(&entity))
            return getCreateQueryImpl(*policy, access_control, attach_mode);
        if (const Quota * quota = typeid_cast<const Quota *>(&entity))
            return getCreateQueryImpl(*quota, access_control, attach_mode);
        if (const SettingsProfile * profile = typeid_cast<const SettingsProfile *>(&entity))
            return getCreateQueryImpl(*profile, access_control, attach_mode);
        throw Exception(entity.formatTypeWithName() + ": type is not supported by SHOW CREATE query", ErrorCodes::NOT_IMPLEMENTED);
    }
}


InterpreterShowCreateAccessEntityQuery::InterpreterShowCreateAccessEntityQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterShowCreateAccessEntityQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowCreateAccessEntityQuery::executeImpl()
{
    /// Build a create queries.
    ASTs create_queries = getCreateQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    WriteBufferFromOwnString create_query_buf;
    for (const auto & create_query : create_queries)
    {
        formatAST(*create_query, create_query_buf, false, true);
        column->insert(create_query_buf.str());
        create_query_buf.restart();
    }

    /// Prepare description of the result column.
    WriteBufferFromOwnString desc_buf;
    const auto & show_query = query_ptr->as<const ASTShowCreateAccessEntityQuery &>();
    formatAST(show_query, desc_buf, false, true);
    String desc = desc_buf.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}}));
}


std::vector<AccessEntityPtr> InterpreterShowCreateAccessEntityQuery::getEntities() const
{
    auto & show_query = query_ptr->as<ASTShowCreateAccessEntityQuery &>();
    const auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());
    show_query.replaceEmptyDatabase(getContext()->getCurrentDatabase());
    std::vector<AccessEntityPtr> entities;

    if (show_query.all)
    {
        auto ids = access_control.findAll(show_query.type);
        for (const auto & id : ids)
        {
            if (auto entity = access_control.tryRead(id))
                entities.push_back(entity);
        }
    }
    else if (show_query.current_user)
    {
        entities.push_back(getContext()->getUser());
    }
    else if (show_query.current_quota)
    {
        auto usage = getContext()->getQuotaUsage();
        if (usage)
            entities.push_back(access_control.read<Quota>(usage->quota_id));
    }
    else if (show_query.type == AccessEntityType::ROW_POLICY)
    {
        auto ids = access_control.findAll<RowPolicy>();
        if (show_query.row_policy_names)
        {
            for (const String & name : show_query.row_policy_names->toStrings())
                entities.push_back(access_control.read<RowPolicy>(name));
        }
        else
        {
            for (const auto & id : ids)
            {
                auto policy = access_control.tryRead<RowPolicy>(id);
                if (!policy)
                    continue;
                if (!show_query.short_name.empty() && (policy->getShortName() != show_query.short_name))
                    continue;
                if (show_query.database_and_table_name)
                {
                    const String & database = show_query.database_and_table_name->first;
                    const String & table_name = show_query.database_and_table_name->second;
                    if (!database.empty() && (policy->getDatabase() != database))
                        continue;
                    if (!table_name.empty() && (policy->getTableName() != table_name))
                        continue;
                }
                entities.push_back(policy);
            }
        }
    }
    else
    {
        for (const String & name : show_query.names)
            entities.push_back(access_control.read(access_control.getID(show_query.type, name)));
    }

    ::sort(entities.begin(), entities.end(), IAccessEntity::LessByName{});
    return entities;
}


ASTs InterpreterShowCreateAccessEntityQuery::getCreateQueries() const
{
    auto entities = getEntities();

    ASTs list;
    const auto & access_control = getContext()->getAccessControl();
    for (const auto & entity : entities)
        list.push_back(getCreateQuery(*entity, access_control));

    return list;
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateQuery(const IAccessEntity & entity, const AccessControl & access_control)
{
    return getCreateQueryImpl(entity, &access_control, false);
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
        case AccessEntityType::USER:
        {
            res.emplace_back(AccessType::SHOW_USERS);
            return res;
        }
        case AccessEntityType::ROLE:
        {
            res.emplace_back(AccessType::SHOW_ROLES);
            return res;
        }
        case AccessEntityType::SETTINGS_PROFILE:
        {
            res.emplace_back(AccessType::SHOW_SETTINGS_PROFILES);
            return res;
        }
        case AccessEntityType::ROW_POLICY:
        {
            if (show_query.row_policy_names)
            {
                for (const auto & row_policy_name : show_query.row_policy_names->full_names)
                    res.emplace_back(AccessType::SHOW_ROW_POLICIES, row_policy_name.database, row_policy_name.table_name);
            }
            else if (show_query.database_and_table_name)
            {
                if (show_query.database_and_table_name->second.empty())
                    res.emplace_back(AccessType::SHOW_ROW_POLICIES, show_query.database_and_table_name->first);
                else
                    res.emplace_back(AccessType::SHOW_ROW_POLICIES, show_query.database_and_table_name->first, show_query.database_and_table_name->second);
            }
            else
            {
                res.emplace_back(AccessType::SHOW_ROW_POLICIES);
            }
            return res;
        }
        case AccessEntityType::QUOTA:
        {
            res.emplace_back(AccessType::SHOW_QUOTAS);
            return res;
        }
        case AccessEntityType::MAX:
            break;
    }
    throw Exception(toString(show_query.type) + ": type is not supported by SHOW CREATE query", ErrorCodes::NOT_IMPLEMENTED);
}
}
