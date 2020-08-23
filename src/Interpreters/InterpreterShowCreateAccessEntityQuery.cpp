#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTUserNameWithHost.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Access/AccessControlManager.h>
#include <Access/VisibleAccessEntities.h>
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
#include <boost/range/algorithm/sort.hpp>
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
        const VisibleAccessEntities * visible_entities /* not used if attach_mode == true */,
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
                query->default_roles = user.default_roles.toASTWithNames(*visible_entities);
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
                query->settings = user.settings.toASTWithNames(visible_entities->getAccessControlManager());
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(const Role & role, const VisibleAccessEntities * visible_entities, bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateRoleQuery>();
        query->names.emplace_back(role.getName());
        query->attach = attach_mode;

        if (!role.settings.empty())
        {
            if (attach_mode)
                query->settings = role.settings.toAST();
            else
                query->settings = role.settings.toASTWithNames(*visible_entities);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(const SettingsProfile & profile, const VisibleAccessEntities * visible_entities, bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateSettingsProfileQuery>();
        query->names.emplace_back(profile.getName());
        query->attach = attach_mode;

        if (!profile.elements.empty())
        {
            if (attach_mode)
                query->settings = profile.elements.toAST();
            else
                query->settings = profile.elements.toASTWithNames(visible_entities->getAccessControlManager());
            if (query->settings)
                query->settings->setUseInheritKeyword(true);
        }

        if (!profile.to_roles.empty())
        {
            if (attach_mode)
                query->to_roles = profile.to_roles.toAST();
            else
                query->to_roles = profile.to_roles.toASTWithNames(*visible_entities);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(
        const Quota & quota,
        const VisibleAccessEntities * visible_entities /* not used if attach_mode == true */,
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateQuotaQuery>();
        query->names.emplace_back(quota.getName());
        query->attach = attach_mode;

        if (quota.key_type != Quota::KeyType::NONE)
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
                query->roles = quota.to_roles.toASTWithNames(*visible_entities);
        }

        return query;
    }


    ASTPtr getCreateQueryImpl(
        const RowPolicy & policy,
        const VisibleAccessEntities * visible_entities /* not used if attach_mode == true */,
        bool attach_mode)
    {
        auto query = std::make_shared<ASTCreateRowPolicyQuery>();
        query->names = std::make_shared<ASTRowPolicyNames>();
        query->names->name_parts.emplace_back(policy.getNameParts());
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
                query->conditions.emplace_back(type, std::move(expr));
            }
        }

        if (!policy.to_roles.empty())
        {
            if (attach_mode)
                query->roles = policy.to_roles.toAST();
            else
                query->roles = policy.to_roles.toASTWithNames(*visible_entities);
        }

        return query;
    }

    ASTPtr getCreateQueryImpl(
        const IAccessEntity & entity,
        const VisibleAccessEntities * visible_entities /* not used if attach_mode == true */,
        bool attach_mode)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getCreateQueryImpl(*user, visible_entities, attach_mode);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getCreateQueryImpl(*role, visible_entities, attach_mode);
        if (const RowPolicy * policy = typeid_cast<const RowPolicy *>(&entity))
            return getCreateQueryImpl(*policy, visible_entities, attach_mode);
        if (const Quota * quota = typeid_cast<const Quota *>(&entity))
            return getCreateQueryImpl(*quota, visible_entities, attach_mode);
        if (const SettingsProfile * profile = typeid_cast<const SettingsProfile *>(&entity))
            return getCreateQueryImpl(*profile, visible_entities, attach_mode);
        throw Exception(entity.outputTypeAndName() + ": type is not supported by SHOW CREATE query", ErrorCodes::NOT_IMPLEMENTED);
    }

    using EntityType = IAccessEntity::Type;
}


InterpreterShowCreateAccessEntityQuery::InterpreterShowCreateAccessEntityQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
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
    /// Build a create queries.
    ASTs create_queries = getCreateQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    std::stringstream create_query_ss;
    for (const auto & create_query : create_queries)
    {
        formatAST(*create_query, create_query_ss, false, true);
        column->insert(create_query_ss.str());
        create_query_ss.str("");
    }

    /// Prepare description of the result column.
    std::stringstream desc_ss;
    const auto & show_query = query_ptr->as<const ASTShowCreateAccessEntityQuery &>();
    formatAST(show_query, desc_ss, false, true);
    String desc = desc_ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


std::vector<AccessEntityPtr> InterpreterShowCreateAccessEntityQuery::getEntities() const
{
    auto & show_query = query_ptr->as<ASTShowCreateAccessEntityQuery &>();
    const auto & access_control = context.getAccessControlManager();
    VisibleAccessEntities visible_entities{context.getAccess()};
    show_query.replaceEmptyDatabaseWithCurrent(context.getCurrentDatabase());
    std::vector<AccessEntityPtr> entities;

    if (show_query.all)
    {
        auto ids = visible_entities.findAll(show_query.type);
        for (const auto & id : ids)
        {
            if (auto entity = access_control.tryRead(id))
                entities.push_back(entity);
        }
    }
    else if (show_query.current_user)
    {
        if (auto user = context.getUser())
            entities.push_back(user);
    }
    else if (show_query.current_quota)
    {
        auto usage = context.getQuotaUsage();
        if (usage)
            entities.push_back(access_control.read<Quota>(usage->quota_id));
    }
    else if (show_query.type == EntityType::ROW_POLICY)
    {
        if (show_query.row_policy_names)
        {
            for (const String & name : show_query.row_policy_names->toStrings())
                entities.push_back(visible_entities.read<RowPolicy>(name));
        }
        else
        {
            auto ids = visible_entities.findAll<RowPolicy>();
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
            entities.push_back(access_control.read(visible_entities.getID(show_query.type, name)));
    }

    boost::range::sort(entities, IAccessEntity::LessByName{});
    return entities;
}


ASTs InterpreterShowCreateAccessEntityQuery::getCreateQueries() const
{
    auto entities = getEntities();

    ASTs list;
    VisibleAccessEntities visible_entities{context.getAccess()};
    for (const auto & entity : entities)
        list.push_back(getCreateQueryImpl(*entity, &visible_entities, false));

    return list;
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateQuery(const IAccessEntity & entity, const Context & context)
{
    VisibleAccessEntities visible_entities{context.getAccess()};
    return getCreateQueryImpl(entity, &visible_entities, false);
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getAttachQuery(const IAccessEntity & entity)
{
    return getCreateQueryImpl(entity, nullptr, true);
}

}
