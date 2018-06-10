#include <DataStreams/ApplyingMutationsBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

ApplyingMutationsBlockInputStream::ApplyingMutationsBlockInputStream(
    const BlockInputStreamPtr & input, const std::vector<MutationCommand> & commands, const Context & context)
{
    children.push_back(input);

    impl = input;
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

            auto predicate_expr = ExpressionAnalyzer(
                predicate, context, nullptr, impl->getHeader().getNamesAndTypesList()).getActions(false);
            String col_name = predicate->getColumnName();

            impl = std::make_shared<FilterBlockInputStream>(impl, predicate_expr, col_name);
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
    if (IProfilingBlockInputStream * profiling = dynamic_cast<IProfilingBlockInputStream *>(impl.get()))
        return profiling->getTotals();

    return IProfilingBlockInputStream::getTotals();
}

Block ApplyingMutationsBlockInputStream::readImpl()
{
    return impl->read();
}

}
