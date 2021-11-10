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

        std::shared_ptr<ASTRolesOrUsersSet> grantees = std::make_shared<ASTRolesOrUsersSet>();
        grantees->names.push_back(grantee.getName());

        std::shared_ptr<ASTGrantQuery> current_query = nullptr;

        for (const auto & element : grantee.access.getElements())
        {
            if (element.empty())
                continue;

            if (current_query)
            {
                const auto & prev_element = current_query->access_rights_elements.back();
                bool continue_with_current_query = element.sameDatabaseAndTable(prev_element) && element.sameOptions(prev_element);
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
                grant_query->roles = RolesOrUsersSet{element.ids}.toASTWithNames(*manager);
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

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


std::vector<AccessEntityPtr> InterpreterShowGrantsQuery::getEntities() const
{
    const auto & show_query = query_ptr->as<ASTShowGrantsQuery &>();
    const auto & access_control = getContext()->getAccessControlManager();
    auto ids = RolesOrUsersSet{*show_query.for_roles, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);

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
    const auto & access_control = getContext()->getAccessControlManager();

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
