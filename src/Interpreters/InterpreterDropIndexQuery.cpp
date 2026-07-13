#include <Interpreters/InterpreterDropIndexQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace
{

ASTPtr rewriteToAlterTable(const ASTDropIndexQuery & query, const ContextPtr & context)
{
    auto alter = make_intrusive<ASTAlterQuery>();
    alter->alter_object = ASTAlterQuery::AlterObjectType::TABLE;

    /// Resolve the canonical target quote-aware first and pin it: copying plain names
    /// would drop the quote pins and let a double-quoted wrong-case target fold.
    if (auto resolved = context->tryResolveStorageID(query))
    {
        if (query.database)
            alter->setDatabase(resolved.database_name, IdentifierPartQuote::DoubleQuoted);
        alter->setTable(resolved.table_name, IdentifierPartQuote::DoubleQuoted);
    }
    else
    {
        if (query.database)
            alter->setDatabase(query.getDatabase(), identifierPartQuoteFromAST(query.database));
        alter->setTable(query.getTable(), identifierPartQuoteFromAST(query.table));
    }

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

    auto alter_query = rewriteToAlterTable(drop_index, context);
    return InterpreterAlterQuery(alter_query, context).execute();
}

void registerInterpreterDropIndexQuery(InterpreterFactory & factory);
void registerInterpreterDropIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropIndexQuery", create_fn);
}

}
