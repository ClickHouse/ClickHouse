#include <Interpreters/InterpreterAlterRewriteRuleQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Common/RewriteRules/RewriteRules.h>
#include <Core/ServerSettings.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>


namespace DB
{

BlockIO InterpreterAlterRewriteRuleQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::ALTER_RULE);

    const auto & query = query_ptr->as<const ASTAlterRewriteRuleQuery &>();

    RewriteRules::instance().updateRule(query);
    return {};
}

void registerInterpreterAlterRewriteRuleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAlterRewriteRuleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterAlterRewriteRuleQuery", create_fn);
}

}
