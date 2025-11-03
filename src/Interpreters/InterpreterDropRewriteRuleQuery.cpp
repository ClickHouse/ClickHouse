#include <Interpreters/InterpreterDropRewriteRuleQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTDropRewriteRuleQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Common/RewriteRules/RewriteRules.h>
#include <Core/ServerSettings.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>


namespace DB
{

BlockIO InterpreterDropRewriteRuleQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::DROP_RULE);

    const auto & query = query_ptr->as<const ASTDropRewriteRuleQuery &>();

    RewriteRules::instance().removeRule(query);
    return {};
}

void registerInterpreterDropRewriteRuleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropRewriteRuleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropRewriteRuleQuery", create_fn);
}

}
