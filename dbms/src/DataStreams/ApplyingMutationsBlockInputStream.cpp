#include <DataStreams/ApplyingMutationsBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ApplyingMutationsBlockInputStream::ApplyingMutationsBlockInputStream(
    const BlockInputStreamPtr & input, const std::vector<MutationCommand> & commands, const Context & context)
{
    children.push_back(input);

    if (commands.empty())
    {
        impl = input;
        return;
    }

    /// Create a total predicate for all mutations and then pass it to a single FilterBlockInputStream
    /// because ExpressionAnalyzer won't detect that some columns in the block are already calculated
    /// and will try to calculate them twice. This works as long as all mutations are DELETE.
    /// TODO: fix ExpressionAnalyzer.

    std::vector<ASTPtr> predicates;

    for (const MutationCommand & cmd : commands)
    {
        switch (cmd.type)
        {
        case MutationCommand::DELETE:
        {
            auto predicate = std::make_shared<ASTFunction>();
            predicate->name = "not";
            predicate->arguments = std::make_shared<ASTExpressionList>();
            predicate->arguments->children.push_back(cmd.predicate);
            predicate->children.push_back(predicate->arguments);
            predicates.push_back(predicate);
            break;
        }
        default:
            throw Exception("Unsupported mutation cmd type: " + toString<int>(cmd.type),
                ErrorCodes::LOGICAL_ERROR);
        }
    }

    ASTPtr total_predicate;
    if (predicates.size() == 1)
        total_predicate = predicates[0];
    else
    {
        auto and_func = std::make_shared<ASTFunction>();
        and_func->name = "and";
        and_func->arguments = std::make_shared<ASTExpressionList>();
        and_func->children.push_back(and_func->arguments);
        and_func->arguments->children = predicates;
        total_predicate = and_func;
    }

    auto predicate_expr = ExpressionAnalyzer(
        total_predicate, context, nullptr, input->getHeader().getNamesAndTypesList()).getActions(false);
    String col_name = total_predicate->getColumnName();
    impl = std::make_shared<FilterBlockInputStream>(input, predicate_expr, col_name);
}

Block ApplyingMutationsBlockInputStream::getHeader() const
{
    return impl->getHeader();
}

Block ApplyingMutationsBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * profiling = dynamic_cast<IProfilingBlockInputStream *>(impl.get()))
        return profiling->getTotals();

    return IProfilingBlockInputStream::getTotals();
}

Block ApplyingMutationsBlockInputStream::readImpl()
{
    return impl->read();
}

}
