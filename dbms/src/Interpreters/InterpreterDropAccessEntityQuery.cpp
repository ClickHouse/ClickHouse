#include <Interpreters/InterpreterDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/Quota.h>


namespace DB
{
BlockIO InterpreterDropAccessEntityQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropAccessEntityQuery &>();
    auto & access_control = context.getAccessControlManager();
    using Kind = ASTDropAccessEntityQuery::Kind;

    switch (query.kind)
    {
        case Kind::QUOTA:
        {
            context.checkQuotaManagementIsAllowed();
            if (query.if_exists)
                access_control.tryRemove(access_control.find<Quota>(query.names));
            else
                access_control.remove(access_control.getIDs<Quota>(query.names));
            return {};
        }
    }

    __builtin_unreachable();
}
}
