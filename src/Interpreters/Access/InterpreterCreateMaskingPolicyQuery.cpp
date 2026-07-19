#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateMaskingPolicyQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}


BlockIO InterpreterCreateMaskingPolicyQuery::execute()
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Masking Policies are available only in ClickHouse Cloud");
}

void registerInterpreterCreateMaskingPolicyQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateMaskingPolicyQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateMaskingPolicyQuery", create_fn);
}

}
