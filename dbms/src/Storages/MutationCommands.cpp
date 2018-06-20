#include <Storages/MutationCommands.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/FilterDescription.h>
#include <IO/Operators.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


namespace DB
{

std::optional<MutationCommand> MutationCommand::parse(ASTAlterCommand * command)
{
    if (command->type == ASTAlterCommand::DELETE)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = DELETE;
        res.predicate = command->predicate;
        return res;
    }
    else
        return {};
}


std::shared_ptr<ASTAlterCommandList> MutationCommands::ast() const
{
    auto res = std::make_shared<ASTAlterCommandList>();
    for (const MutationCommand & command : *this)
        res->add(command.ast->clone());
    return res;
}

void MutationCommands::validate(const IStorage & table, const Context & context) const
{
    auto all_columns = table.getColumns().getAll();

    for (const MutationCommand & command : *this)
    {
        switch (command.type)
        {
            case MutationCommand::DELETE:
            {
                auto actions = ExpressionAnalyzer(command.predicate, context, {}, all_columns).getActions(true);
                const ColumnWithTypeAndName & predicate_column = actions->getSampleBlock().getByName(
                    command.predicate->getColumnName());
                checkColumnCanBeUsedAsFilter(predicate_column);
                break;
            }
            default:
                throw Exception("Bad mutation type: " + toString<int>(command.type), ErrorCodes::LOGICAL_ERROR);
        }
    }
}

}
