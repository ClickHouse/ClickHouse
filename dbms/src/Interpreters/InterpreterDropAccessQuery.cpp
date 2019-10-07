#include <Interpreters/InterpreterDropAccessQuery.h>
#include <Parsers/ASTDropAccessQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>
#include <Access/User2.h>


namespace DB
{
BlockIO InterpreterDropAccessQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropAccessQuery &>();
    using Kind = ASTDropAccessQuery::Kind;
    switch (query.kind)
    {
        case Kind::ROLE:
        {
            if (query.if_exists)
                context.getAccessControlManager().tryRemove<Role>(query.names);
            else
                context.getAccessControlManager().remove<Role>(query.names);
            break;
        }
        case Kind::USER:
        {
            if (query.if_exists)
                context.getAccessControlManager().tryRemove<User2>(query.names);
            else
                context.getAccessControlManager().remove<User2>(query.names);
            break;
        }
#if 0
        case Kind::SETTINGS_PROFILE:
        {
            if (query.if_exists)
                context.getAccessControlManager().tryRemove<SettingsProfile>(query.names);
            else
                context.getAccessControlManager().remove<SettingsProfile>(query.names);
            break;
        }
        case Kind::QUOTA:
        {
            if (query.if_exists)
                context.getAccessControlManager().tryRemove<Quota2>(query.names);
            else
                context.getAccessControlManager().remove<Quota2>(query.names);
            break;
        }
        case Kind::ROW_POLICY:
        {
            if (query.if_exists)
                context.getAccessControlManager().tryRemove<RowPolicy>(query.names);
            else
                context.getAccessControlManager().remove<RowPolicy>(query.names);
            break;
        }
#endif
    }

    return {};
}
}
