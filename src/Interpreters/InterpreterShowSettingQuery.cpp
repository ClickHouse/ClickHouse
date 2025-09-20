#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowSettingQuery.h>

#include <Common/escapeString.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>


namespace DB
{


InterpreterShowSettingQuery::InterpreterShowSettingQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}


String InterpreterShowSettingQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowSettingQuery &>();
    return fmt::format(R"(SELECT value FROM system.settings WHERE name = '{0}')", escapeString(query.getSettingName()));
}


BlockIO InterpreterShowSettingQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    BlockIO io = executeQuery(getRewrittenQuery(), query_context, QueryFlags{ .internal = true }).second;
    io.context_holder = std::move(query_context);
    return io;
}

void registerInterpreterShowSettingQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowSettingQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowSettingQuery", create_fn);
}

}
