#include <DataStreams/ApplyingMutationsBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ApplyingMutationsBlockInputStream::ApplyingMutationsBlockInputStream(
    const BlockInputStreamPtr & input, const MutationCommand & command, const Context & context)
{
    children.push_back(input);

    switch (command.type)
    {
    case MutationCommand::DELETE:
    {
        auto negated_predicate = makeASTFunction("not", command.predicate);
        auto predicate_expr = ExpressionAnalyzer(
            negated_predicate, context, nullptr, input->getHeader().getNamesAndTypesList())
            .getActions(false);
        String col_name = negated_predicate->getColumnName();
        impl = std::make_shared<FilterBlockInputStream>(input, predicate_expr, col_name);
        break;
    }
    case MutationCommand::UPDATE:
    {
        auto new_column_exprs = std::make_shared<ASTExpressionList>();
        std::unordered_map<String, ASTPtr> column_to_updated;
        for (const auto & pair : command.column_to_update_expression)
        {
            auto new_col = makeASTFunction("CAST",
                makeASTFunction("if",
                    command.predicate,
                    pair.second->clone(),
                    std::make_shared<ASTIdentifier>(pair.first)),
                std::make_shared<ASTLiteral>(input->getHeader().getByName(pair.first).type->getName()));
            new_column_exprs->children.push_back(new_col);
            column_to_updated.emplace(pair.first, new_col);
        }

        auto updating_expr = ExpressionAnalyzer(
            new_column_exprs, context, nullptr, input->getHeader().getNamesAndTypesList())
            .getActions(false);

        /// Calling getColumnName() for updating expressions after the ExpressionAnalyzer pass, because
        /// it can change the AST of the expressions.
        for (const auto & pair : column_to_updated)
            updating_expr->add(ExpressionAction::copyColumn(pair.second->getColumnName(), pair.first, /* can_replace = */ true));

        impl = std::make_shared<ExpressionBlockInputStream>(input, updating_expr);
        break;
    }
    default:
        throw Exception("Unsupported mutation command type: " + toString<int>(command.type),
            ErrorCodes::LOGICAL_ERROR);
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
