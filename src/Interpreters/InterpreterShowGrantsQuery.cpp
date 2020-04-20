#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>


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

        std::shared_ptr<ASTExtendedRoleSet> to_roles = std::make_shared<ASTExtendedRoleSet>();
        to_roles->names.push_back(grantee.getName());

        auto grants_and_partial_revokes = grantee.access.getGrantsAndPartialRevokes();

        for (bool grant_option : {false, true})
        {
            using Kind = ASTGrantQuery::Kind;
            for (Kind kind : {Kind::GRANT, Kind::REVOKE})
            {
                AccessRightsElements * elements = nullptr;
                if (grant_option)
                    elements = (kind == Kind::GRANT) ? &grants_and_partial_revokes.grants_with_grant_option : &grants_and_partial_revokes.revokes_grant_option;
                else
                    elements = (kind == Kind::GRANT) ? &grants_and_partial_revokes.grants : &grants_and_partial_revokes.revokes;
                elements->normalize();

                std::shared_ptr<ASTGrantQuery> grant_query = nullptr;
                for (size_t i = 0; i != elements->size(); ++i)
                {
                    const auto & element = (*elements)[i];
                    bool prev_element_on_same_db_and_table = false;
                    if (grant_query)
                    {
                        const auto & prev_element = grant_query->access_rights_elements.back();
                        if ((element.database == prev_element.database) && (element.any_database == prev_element.any_database)
                            && (element.table == prev_element.table) && (element.any_table == prev_element.any_table))
                            prev_element_on_same_db_and_table = true;
                    }
                    if (!prev_element_on_same_db_and_table)
                    {
                        grant_query = std::make_shared<ASTGrantQuery>();
                        grant_query->kind = kind;
                        grant_query->attach = attach_mode;
                        grant_query->grant_option = grant_option;
                        grant_query->to_roles = to_roles;
                        res.push_back(grant_query);
                    }
                    grant_query->access_rights_elements.emplace_back(std::move(element));
                }
            }
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
                grant_query->roles = ExtendedRoleSet{roles}.toAST();
            else
                grant_query->roles = ExtendedRoleSet{roles}.toASTWithNames(*manager);
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
