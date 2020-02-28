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
    UserPtr user;
    RolePtr role;
    if (show_query.current_user)
        user = context.getUser();
    else
    {
        user = access_control.tryRead<User>(show_query.name);
        if (!user)
            role = access_control.read<Role>(show_query.name);
    }

    const AccessRights * access = nullptr;
    const AccessRights * access_with_grant_option = nullptr;
    const boost::container::flat_set<UUID> * granted_roles = nullptr;
    const boost::container::flat_set<UUID> * granted_roles_with_admin_option = nullptr;
    if (user)
    {
        access = &user->access;
        access_with_grant_option = &user->access_with_grant_option;
        granted_roles = &user->granted_roles;
        granted_roles_with_admin_option = &user->granted_roles_with_admin_option;
    }
    else
    {
        access = &role->access;
        access_with_grant_option = &role->access_with_grant_option;
        granted_roles = &role->granted_roles;
        granted_roles_with_admin_option = &role->granted_roles_with_admin_option;
    }

    ASTs res;

    for (bool grant_option : {true, false})
    {
        if (!grant_option && (*access == *access_with_grant_option))
            continue;
        const auto & access_rights = grant_option ? *access_with_grant_option : *access;
        const auto grouped_elements = groupByTable(access_rights.getElements());

        using Kind = ASTGrantQuery::Kind;
        for (Kind kind : {Kind::GRANT, Kind::REVOKE})
        {
            for (const auto & elements : (kind == Kind::GRANT ? grouped_elements.grants : grouped_elements.partial_revokes))
            {
                auto grant_query = std::make_shared<ASTGrantQuery>();
                grant_query->kind = kind;
                grant_query->grant_option = grant_option;
                grant_query->to_roles = std::make_shared<ASTGenericRoleSet>();
                grant_query->to_roles->names.push_back(show_query.name);
                grant_query->access_rights_elements = elements;
                res.push_back(std::move(grant_query));
            }
        }
    }

    for (bool admin_option : {true, false})
    {
        if (!admin_option && (*granted_roles == *granted_roles_with_admin_option))
            continue;

        const auto & roles = admin_option ? *granted_roles_with_admin_option : *granted_roles;
        if (roles.empty())
            continue;

        auto grant_query = std::make_shared<ASTGrantQuery>();
        using Kind = ASTGrantQuery::Kind;
        grant_query->kind = Kind::GRANT;
        grant_query->admin_option = admin_option;
        grant_query->to_roles = std::make_shared<ASTGenericRoleSet>();
        grant_query->to_roles->names.push_back(show_query.name);
        grant_query->roles = GenericRoleSet{roles}.toAST(access_control);
        res.push_back(std::move(grant_query));
    }

    return res;
}
}
