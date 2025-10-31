#include <Interpreters/InterpreterCreateRewriteRuleQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Common/RewriteRules/RewriteRules.h>
#include <Core/ServerSettings.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>


namespace DB
{

BlockIO InterpreterCreateRewriteRuleQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_RULE);

    const auto & query = query_ptr->as<const ASTCreateRewriteRuleQuery &>();

    RewriteRules::instance().createRule(query);
    return {};
}

void registerInterpreterCreateRewriteRuleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateRewriteRuleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateRewriteRuleQuery", create_fn);
}

}
