#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/formatAST.h>
#include <Access/AccessControl.h>
#include <Access/AccessRights.h>
#include <Access/CachedAccessChecking.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <base/sort.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    void getGrantsFromAccess(
        ASTs & res,
        const AccessRights & access,
        const std::shared_ptr<ASTRolesOrUsersSet> grantees,
        const AccessControl * access_control,
        bool attach_mode = false,
        bool with_implicit = false)
    {
        std::shared_ptr<ASTGrantQuery> current_query = nullptr;

        AccessRightsElements elements;
        if (with_implicit)
            elements = ContextAccess::addImplicitAccessRights(access, *access_control).getElements();
        else
            elements = access.getElements();

        for (auto & element : elements)
        {
            if (element.empty())
                continue;

            if (current_query)
            {
                const auto & prev_element = current_query->access_rights_elements.back();
                bool continue_with_current_query = element.sameDatabaseAndTableAndParameter(prev_element) && element.sameOptions(prev_element);
                if (!continue_with_current_query)
                    current_query = nullptr;
            }

            if (!current_query)
            {
                current_query = std::make_shared<ASTGrantQuery>();
                current_query->grantees = grantees;
                current_query->attach_mode = attach_mode;
                if (element.is_partial_revoke)
                    current_query->is_revoke = true;
                res.push_back(current_query);
            }

            current_query->access_rights_elements.emplace_back(std::move(element));
        }
    }

    template <typename T>
    void unionAccessFromRoles(AccessRights & res, const T & entity, const AccessControl * access_control)
    {
        std::vector granted_roles(entity.granted_roles.getGranted().begin(), entity.granted_roles.getGranted().end());
        std::vector granted_roles_with_admin_option(entity.granted_roles.getGrantedWithAdminOption().begin(), entity.granted_roles.getGrantedWithAdminOption().end());
        auto roles_info = access_control->getEnabledRolesInfo(granted_roles, granted_roles_with_admin_option);

        res.makeUnion(roles_info->access);
    }

    template <typename T>
    ASTs getGrantQueriesImpl(
        const T & grantee,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode = false,
        bool with_implicit = false,
        bool final = false)
    {
        ASTs res;

        std::shared_ptr<ASTRolesOrUsersSet> grantees = std::make_shared<ASTRolesOrUsersSet>();
        grantees->names.push_back(grantee.getName());

        AccessRights access = grantee.access;
        if (final)
            unionAccessFromRoles(access, grantee, access_control);

        getGrantsFromAccess(res, access, grantees, access_control, attach_mode, with_implicit);

        if (!final)
        {
            for (const auto & element : grantee.granted_roles.getElements())
            {
                if (element.empty())
                    continue;

                auto grant_query = std::make_shared<ASTGrantQuery>();
                grant_query->grantees = grantees;
                grant_query->admin_option = element.admin_option;
                grant_query->attach_mode = attach_mode;
                if (attach_mode)
                    grant_query->roles = RolesOrUsersSet{element.ids}.toAST();
                else
                    grant_query->roles = RolesOrUsersSet{element.ids}.toASTWithNames(*access_control);
                res.push_back(std::move(grant_query));
            }
        }

        return res;
    }

    ASTs getGrantQueriesImpl(
        const IAccessEntity & entity,
        const AccessControl * access_control /* not used if attach_mode == true */,
        bool attach_mode = false,
        bool with_implicit = false,
        bool final = false)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getGrantQueriesImpl(*user, access_control, attach_mode, with_implicit, final);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getGrantQueriesImpl(*role, access_control, attach_mode, with_implicit, final);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is expected to be user or role", entity.formatTypeWithName());
    }

}


BlockIO InterpreterShowGrantsQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowGrantsQuery::executeImpl()
{
    /// Build a create query.
    ASTs grant_queries = getGrantQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    WriteBufferFromOwnString grant_buf;
    for (const auto & grant_query : grant_queries)
    {
        grant_buf.restart();
        formatAST(*grant_query, grant_buf, false, true);
        column->insert(grant_buf.str());
    }

    /// Prepare description of the result column.
    WriteBufferFromOwnString desc_buf;
    const auto & show_query = query_ptr->as<const ASTShowGrantsQuery &>();
    formatAST(show_query, desc_buf, false, true);
    String desc = desc_buf.str();
    String prefix = "SHOW ";
    if (desc.starts_with(prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}}));
}


std::vector<AccessEntityPtr> InterpreterShowGrantsQuery::getEntities() const
{
    const auto & access = getContext()->getAccess();
    const auto & access_control = getContext()->getAccessControl();

    const auto & show_query = query_ptr->as<ASTShowGrantsQuery &>();
    auto ids = RolesOrUsersSet{*show_query.for_roles, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);

    CachedAccessChecking show_users(access, AccessType::SHOW_USERS);
    CachedAccessChecking show_roles(access, AccessType::SHOW_ROLES);
    bool throw_if_access_denied = !show_query.for_roles->all;

    auto current_user = access->getUser();
    auto roles_info = access->getRolesInfo();

    std::vector<AccessEntityPtr> entities;
    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        bool is_current_user = (id == access->getUserID());
        bool is_enabled_or_granted_role = entity->isTypeOf<Role>()
            && (current_user->granted_roles.isGranted(id) || roles_info->enabled_roles.contains(id));

        if ((is_current_user /* Any user can see his own grants */)
            || (is_enabled_or_granted_role /* and grants from the granted roles */)
            || (entity->isTypeOf<User>() && show_users.checkAccess(throw_if_access_denied))
            || (entity->isTypeOf<Role>() && show_roles.checkAccess(throw_if_access_denied)))
            entities.push_back(entity);
    }

    ::sort(entities.begin(), entities.end(), IAccessEntity::LessByTypeAndName{});
    return entities;
}


ASTs InterpreterShowGrantsQuery::getGrantQueries() const
{
    auto entities = getEntities();
    const auto & access_control = getContext()->getAccessControl();

    const auto & show_query = query_ptr->as<const ASTShowGrantsQuery &>();
    ASTs grant_queries;
    for (const auto & entity : entities)
        boost::range::push_back(grant_queries, getGrantQueries(*entity, access_control, show_query.with_implicit, show_query.final));

    return grant_queries;
}


ASTs InterpreterShowGrantsQuery::getGrantQueries(const IAccessEntity & user_or_role, const AccessControl & access_control, bool with_implicit, bool final)
{
    return getGrantQueriesImpl(user_or_role, &access_control, false, with_implicit, final);
}


ASTs InterpreterShowGrantsQuery::getAttachGrantQueries(const IAccessEntity & user_or_role)
{
    return getGrantQueriesImpl(user_or_role, nullptr, true, false);
}

void registerInterpreterShowGrantsQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowGrantsQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowGrantsQuery", create_fn);
}

}
