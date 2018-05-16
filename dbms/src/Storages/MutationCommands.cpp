#include <Storages/MutationCommands.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/FilterDescription.h>


namespace DB
{

void MutationCommands::validate(const IStorage & table, const Context & context)
{
    auto all_columns = table.getColumns().getAll();

    for (const MutationCommand & command : commands)
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
