#include <DataStreams/ApplyingMutationsBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

ApplyingMutationsBlockInputStream::ApplyingMutationsBlockInputStream(
    const BlockInputStreamPtr & input, const std::vector<MutationCommand> & commands, const Context & context)
{
    if (commands.empty())
        throw Exception("Empty mutation commands list. This is a bug.", ErrorCodes::LOGICAL_ERROR);

    children.push_back(input);

    for (const MutationCommand & cmd : commands)
    {
        const BlockInputStreamPtr & cur_input = impl ? impl : input;

        switch (cmd.type)
        {
        case MutationCommand::DELETE:
        {
            auto predicate = std::make_shared<ASTFunction>();
            predicate->name = "not";
            predicate->arguments = std::make_shared<ASTExpressionList>();
            predicate->arguments->children.push_back(cmd.predicate);
            predicate->children.push_back(predicate->arguments);

            auto predicate_expr = ExpressionAnalyzer(
                predicate, context, nullptr, cur_input->getHeader().getNamesAndTypesList()).getActions(false);
            String col_name = predicate->getColumnName();

            impl = std::make_shared<FilterBlockInputStream>(cur_input, predicate_expr, col_name);
            break;
        }
        default:
            throw Exception("Unsupported mutation cmd type: " + toString(static_cast<int>(cmd.type)),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
}

Block ApplyingMutationsBlockInputStream::getHeader() const
{
    return impl->getHeader();
}

Block ApplyingMutationsBlockInputStream::getTotals()
{
    return impl->getTotals();
}

Block ApplyingMutationsBlockInputStream::readImpl()
{
    return impl->read();
}

}
