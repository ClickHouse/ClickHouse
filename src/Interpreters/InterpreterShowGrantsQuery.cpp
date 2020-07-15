#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    template <typename T>
    ASTs getGrantQueriesImpl(
        const T & grantee,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        ASTs res;

        std::shared_ptr<ASTRolesOrUsersSet> to_roles = std::make_shared<ASTRolesOrUsersSet>();
        to_roles->names.push_back(grantee.getName());

        std::shared_ptr<ASTGrantQuery> current_query = nullptr;

        auto elements = grantee.access.getElements();
        for (const auto & element : elements)
        {
            if (current_query)
            {
                const auto & prev_element = current_query->access_rights_elements.back();
                bool continue_using_current_query = (element.database == prev_element.database)
                    && (element.any_database == prev_element.any_database) && (element.table == prev_element.table)
                    && (element.any_table == prev_element.any_table) && (element.grant_option == current_query->grant_option)
                    && (element.kind == current_query->kind);
                if (!continue_using_current_query)
                    current_query = nullptr;
            }

            if (!current_query)
            {
                current_query = std::make_shared<ASTGrantQuery>();
                current_query->kind = element.kind;
                current_query->attach = attach_mode;
                current_query->grant_option = element.grant_option;
                current_query->to_roles = to_roles;
                res.push_back(current_query);
            }

            current_query->access_rights_elements.emplace_back(std::move(element));
        }

        auto grants_roles = grantee.granted_roles.getGrants();

        for (bool admin_option : {false, true})
        {
            const auto & roles = admin_option ? grants_roles.grants_with_admin_option : grants_roles.grants;
            if (roles.empty())
                continue;

            auto grant_query = std::make_shared<ASTGrantQuery>();
            using Kind = ASTGrantQuery::Kind;
            grant_query->kind = Kind::GRANT;
            grant_query->attach = attach_mode;
            grant_query->admin_option = admin_option;
            grant_query->to_roles = to_roles;
            if (attach_mode)
                grant_query->roles = RolesOrUsersSet{roles}.toAST();
            else
                grant_query->roles = RolesOrUsersSet{roles}.toASTWithNames(*manager);
            res.push_back(std::move(grant_query));
        }

        return res;
    }

    ASTs getGrantQueriesImpl(
        const IAccessEntity & entity,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        if (const User * user = typeid_cast<const User *>(&entity))
            return getGrantQueriesImpl(*user, manager, attach_mode);
        if (const Role * role = typeid_cast<const Role *>(&entity))
            return getGrantQueriesImpl(*role, manager, attach_mode);
        throw Exception(entity.outputTypeAndName() + " is expected to be user or role", ErrorCodes::LOGICAL_ERROR);
    }

}


BlockIO InterpreterShowGrantsQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowGrantsQuery::executeImpl()
{
    /// Build a create query.
    ASTs grant_queries = getGrantQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    std::stringstream grant_ss;
    for (const auto & grant_query : grant_queries)
    {
        grant_ss.str("");
        formatAST(*grant_query, grant_ss, false, true);
        column->insert(grant_ss.str());
    }

    /// Prepare description of the result column.
    std::stringstream desc_ss;
    const auto & show_query = query_ptr->as<const ASTShowGrantsQuery &>();
    formatAST(show_query, desc_ss, false, true);
    String desc = desc_ss.str();
    String prefix = "SHOW ";
    if (desc.starts_with(prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


std::vector<AccessEntityPtr> InterpreterShowGrantsQuery::getEntities() const
{
    const auto & show_query = query_ptr->as<ASTShowGrantsQuery &>();
    const auto & access_control = context.getAccessControlManager();
    auto ids = RolesOrUsersSet{*show_query.for_roles, access_control, context.getUserID()}.getMatchingIDs(access_control);

    std::vector<AccessEntityPtr> entities;
    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (entity)
            entities.push_back(entity);
    }

    boost::range::sort(entities, IAccessEntity::LessByTypeAndName{});
    return entities;
}


ASTs InterpreterShowGrantsQuery::getGrantQueries() const
{
    auto entities = getEntities();
    const auto & access_control = context.getAccessControlManager();

    ASTs grant_queries;
    for (const auto & entity : entities)
        boost::range::push_back(grant_queries, getGrantQueries(*entity, access_control));

    return grant_queries;
}


ASTs InterpreterShowGrantsQuery::getGrantQueries(const IAccessEntity & user_or_role, const AccessControlManager & access_control)
{
    return getGrantQueriesImpl(user_or_role, &access_control, false);
}


ASTs InterpreterShowGrantsQuery::getAttachGrantQueries(const IAccessEntity & user_or_role)
{
    return getGrantQueriesImpl(user_or_role, nullptr, true);
}

}
