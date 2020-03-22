#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTGenericRoleSet.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    std::vector<AccessRightsElements> groupByTable(AccessRightsElements && elements)
    {
        using Key = std::tuple<String, bool, String, bool>;
        std::map<Key, AccessRightsElements> grouping_map;
        for (auto & element : elements)
        {
            Key key(element.database, element.any_database, element.table, element.any_table);
            grouping_map[key].emplace_back(std::move(element));
        }
        std::vector<AccessRightsElements> res;
        res.reserve(grouping_map.size());
        boost::range::copy(grouping_map | boost::adaptors::map_values, std::back_inserter(res));
        return res;
    }


    struct GroupedGrantsAndPartialRevokes
    {
        std::vector<AccessRightsElements> grants;
        std::vector<AccessRightsElements> partial_revokes;
    };

    GroupedGrantsAndPartialRevokes groupByTable(AccessRights::Elements && elements)
    {
        GroupedGrantsAndPartialRevokes res;
        res.grants = groupByTable(std::move(elements.grants));
        res.partial_revokes = groupByTable(std::move(elements.partial_revokes));
        return res;
    }


    template <typename T>
    ASTs getGrantQueriesImpl(
        const T & grantee,
        const AccessControlManager * manager /* not used if attach_mode == true */,
        bool attach_mode = false)
    {
        ASTs res;

        std::shared_ptr<ASTGenericRoleSet> to_roles = std::make_shared<ASTGenericRoleSet>();
        to_roles->names.push_back(grantee.getName());

        for (bool grant_option : {true, false})
        {
            if (!grant_option && (grantee.access == grantee.access_with_grant_option))
                continue;
            const auto & access_rights = grant_option ? grantee.access_with_grant_option : grantee.access;
            const auto grouped_elements = groupByTable(access_rights.getElements());

            using Kind = ASTGrantQuery::Kind;
            for (Kind kind : {Kind::GRANT, Kind::REVOKE})
            {
                for (const auto & elements : (kind == Kind::GRANT ? grouped_elements.grants : grouped_elements.partial_revokes))
                {
                    auto grant_query = std::make_shared<ASTGrantQuery>();
                    grant_query->kind = kind;
                    grant_query->attach = attach_mode;
                    grant_query->grant_option = grant_option;
                    grant_query->to_roles = to_roles;
                    grant_query->access_rights_elements = elements;
                    res.push_back(std::move(grant_query));
                }
            }
        }

        for (bool admin_option : {true, false})
        {
            if (!admin_option && (grantee.granted_roles == grantee.granted_roles_with_admin_option))
                continue;

            const auto & roles = admin_option ? grantee.granted_roles_with_admin_option : grantee.granted_roles;
            if (roles.empty())
                continue;

            auto grant_query = std::make_shared<ASTGrantQuery>();
            using Kind = ASTGrantQuery::Kind;
            grant_query->kind = Kind::GRANT;
            grant_query->attach = attach_mode;
            grant_query->admin_option = admin_option;
            grant_query->to_roles = to_roles;
            if (attach_mode)
                grant_query->roles = GenericRoleSet{roles}.toAST();
            else
                grant_query->roles = GenericRoleSet{roles}.toASTWithNames(*manager);
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
        throw Exception("Unexpected type of access entity: " + entity.getTypeName(), ErrorCodes::LOGICAL_ERROR);
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
    const auto & show_query = query_ptr->as<ASTShowGrantsQuery &>();

    /// Build a create query.
    ASTs grant_queries = getGrantQueries(show_query);

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
    formatAST(show_query, desc_ss, false, true);
    String desc = desc_ss.str();
    String prefix = "SHOW ";
    if (desc.starts_with(prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


ASTs InterpreterShowGrantsQuery::getGrantQueries(const ASTShowGrantsQuery & show_query) const
{
    const auto & access_control = context.getAccessControlManager();

    AccessEntityPtr user_or_role;
    if (show_query.current_user)
        user_or_role = context.getUser();
    else
    {
        user_or_role = access_control.tryRead<User>(show_query.name);
        if (!user_or_role)
            user_or_role = access_control.read<Role>(show_query.name);
    }

    return getGrantQueriesImpl(*user_or_role, &access_control);
}


ASTs InterpreterShowGrantsQuery::getAttachGrantQueries(const IAccessEntity & user_or_role)
{
    return getGrantQueriesImpl(user_or_role, nullptr, true);
}

}
