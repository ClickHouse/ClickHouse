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

    /// `max_ast_depth` / `max_ast_elements` for the rule templates are enforced earlier, as
    /// part of the generic `checkASTSizeLimits` pre-execution gate in `executeQuery` (before
    /// the access check above), so they cannot be bypassed via wrappers such as `EXPLAIN AST`.
    RewriteRules::instance().updateRule(query);
    return {};
}

void registerInterpreterAlterRewriteRuleQuery(InterpreterFactory & factory);
void registerInterpreterAlterRewriteRuleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAlterRewriteRuleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterAlterRewriteRuleQuery", create_fn);
}

}
