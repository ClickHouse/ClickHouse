#include <Interpreters/InterpreterDropIndexQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropIndexQuery.h>

namespace DB
{

namespace
{

ASTPtr rewriteToAlterTable(const ASTDropIndexQuery & query)
{
    auto alter = make_intrusive<ASTAlterQuery>();
    alter->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
    alter->setDatabase(query.getDatabase());
    alter->setTable(query.getTable());
    alter->cluster = query.cluster;

    auto command_list = make_intrusive<ASTExpressionList>();
    command_list->children.push_back(query.convertToASTAlterCommand());

    alter->command_list = command_list.get();
    alter->children.push_back(std::move(command_list));

    return alter;
}

}

BlockIO InterpreterDropIndexQuery::execute()
{
    const auto & drop_index = query_ptr->as<ASTDropIndexQuery &>();
    const auto context = Context::createCopy(getContext());

    auto alter_query = rewriteToAlterTable(drop_index);
    return InterpreterAlterQuery(alter_query, context).execute();
}

void registerInterpreterDropIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropIndexQuery", create_fn);
}

}
