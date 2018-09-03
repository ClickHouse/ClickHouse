#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
}

bool MutationsInterpreter::isStorageTouchedByMutations() const
{
    if (commands.empty())
        return false;

    for (const MutationCommand & command : commands)
    {
        if (!command.predicate) /// The command touches all rows.
            return true;
    }

    /// Execute `SELECT count() FROM storage WHERE predicate1 OR predicate2 OR ...` query.
    /// The result can differ from tne number of affected rows (e.g. if there is an UPDATE command that
    /// changes how many rows satisfy the predicates of the subsequent commands).
    /// But we can be sure that if count = 0, then no rows will be touched.

    auto select = std::make_shared<ASTSelectQuery>();

    select->select_expression_list = std::make_shared<ASTExpressionList>();
    select->children.push_back(select->select_expression_list);
    auto count_func = std::make_shared<ASTFunction>();
    count_func->name = "count";
    count_func->arguments = std::make_shared<ASTExpressionList>();
    select->select_expression_list->children.push_back(count_func);

    if (commands.size() == 1)
        select->where_expression = commands[0].predicate;
    else
    {
        auto coalesced_predicates = std::make_shared<ASTFunction>();
        coalesced_predicates->name = "or";
        coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
        coalesced_predicates->children.push_back(coalesced_predicates->arguments);

        for (const MutationCommand & command : commands)
            coalesced_predicates->arguments->children.push_back(command.predicate);

        select->where_expression = std::move(coalesced_predicates);
    }
    select->children.push_back(select->where_expression);

    auto context_copy = context;
    context_copy.getSettingsRef().merge_tree_uniform_read_distribution = 0;
    context_copy.getSettingsRef().max_threads = 1;

    InterpreterSelectQuery interpreter_select(select, context_copy, storage, QueryProcessingStage::Complete);
    BlockInputStreamPtr in = interpreter_select.execute().in;

    Block block = in->read();
    if (!block.rows())
        return false;
    else if (block.rows() != 1)
        throw Exception("count() expression returned " + toString(block.rows()) + " rows, not 1",
            ErrorCodes::LOGICAL_ERROR);

    auto count = (*block.getByName("count()").column)[0].get<UInt64>();
    return count != 0;
}

void MutationsInterpreter::prepare()
{
    if (is_prepared)
        return;

    if (commands.empty())
        throw Exception("Empty mutation commands list", ErrorCodes::LOGICAL_ERROR);

    NamesAndTypesList all_columns = storage->getColumns().getAllPhysical();

    /// First, break a sequence of commands into stages.
    stages.emplace_back();
    for (const auto & command : commands)
    {
        if (stages.back().update)
            stages.emplace_back();

        if (command.type == MutationCommand::DELETE)
            stages.back().deletes.push_back(command);
        else if (command.type == MutationCommand::UPDATE)
        {
            if (stages.size() == 1) /// First stage only supports DELETEs.
                stages.emplace_back();

            stages.back().update = command;
        }
        else
            throw Exception("Unknown mutation command type: " + DB::toString<int>(command.type), ErrorCodes::UNKNOWN_MUTATION_COMMAND);
    }

    /// Next, for each stage calculate columns changed by this and previous stages.
    for (size_t i = 0; i < stages.size(); ++i)
    {
        if (!stages[i].deletes.empty())
        {
            for (const auto & column : all_columns)
                stages[i].output_columns.insert(column.name);
            continue;
        }

        if (i > 0)
            stages[i].output_columns = stages[i - 1].output_columns;

        if (stages[i].update && stages[i].output_columns.size() < all_columns.size())
        {
            for (const auto & kv : (*stages[i].update).column_to_update_expression)
                stages[i].output_columns.insert(kv.first);
        }
    }

    /// Now, calculate `expressions_chain` for each stage except the first.
    /// Do it backwards to propagate information about columns required as input for a stage to the previous stage.
    for (size_t i = stages.size() - 1; i > 0; --i)
    {
        auto & stage = stages[i];

        ASTPtr all_asts = std::make_shared<ASTExpressionList>();
        ASTs delete_filter_columns;
        std::unordered_map<String, ASTPtr> column_to_updated;

        for (const auto & command : stage.deletes)
        {
            auto negated_predicate = makeASTFunction("not", command.predicate->clone());
            all_asts->children.push_back(negated_predicate);
            delete_filter_columns.push_back(negated_predicate);
        }

        if (stage.update)
        {
            for (const auto & kv : stage.update->column_to_update_expression)
            {
                const String & column = kv.first;
                const auto & update_expr = kv.second;
                auto updated_column = makeASTFunction("CAST",
                    makeASTFunction("if",
                        stage.update->predicate->clone(),
                        update_expr->clone(),
                        std::make_shared<ASTIdentifier>(column)),
                    std::make_shared<ASTLiteral>(storage->getColumn(column).type->getName()));
                column_to_updated.emplace(column, updated_column);
                all_asts->children.push_back(updated_column);
            }
        }

        /// Add all output columns to prevent ExpressionAnalyzer from deleting them from source columns.
        for (const String & column : stage.output_columns)
            all_asts->children.push_back(std::make_shared<ASTIdentifier>(column));

        ExpressionAnalyzer analyzer(all_asts, context, nullptr, all_columns);

        ExpressionActionsChain & actions_chain = stage.expressions_chain;

        for (const auto & ast : delete_filter_columns)
        {
            if (!actions_chain.steps.empty())
                actions_chain.addStep();
            analyzer.appendExpression(actions_chain, ast);
            stage.delete_filter_column_names.push_back(ast->getColumnName());
        }

        if (stage.update)
        {
            if (!actions_chain.steps.empty())
                actions_chain.addStep();

            for (const auto & kv : column_to_updated)
                analyzer.appendExpression(actions_chain, kv.second);

            for (const auto & kv : column_to_updated)
            {
                actions_chain.getLastActions()->add(ExpressionAction::copyColumn(
                    kv.second->getColumnName(), kv.first, /* can_replace = */ true));
            }
        }

        /// Remove all intermediate columns.
        actions_chain.addStep();
        actions_chain.getLastStep().required_output.assign(stage.output_columns.begin(), stage.output_columns.end());

        actions_chain.finalize();

        /// Propagate information about columns needed as input.
        for (const auto & column : actions_chain.steps.front().actions->getRequiredColumnsWithTypes())
            stages[i - 1].output_columns.insert(column.name);
    }

    /// Execute first stage as a SELECT statement.

    auto select = std::make_shared<ASTSelectQuery>();

    select->select_expression_list = std::make_shared<ASTExpressionList>();
    select->children.push_back(select->select_expression_list);
    for (const auto & column_name : stages[0].output_columns)
        select->select_expression_list->children.push_back(std::make_shared<ASTIdentifier>(column_name));

    if (!stages[0].deletes.empty())
    {
        ASTs delete_filters;
        for (const auto & delete_ : stages[0].deletes)
            delete_filters.push_back(makeASTFunction("not", delete_.predicate->clone()));

        ASTPtr where_expression;
        if (stages[0].deletes.size() == 1)
            where_expression = delete_filters[0];
        else
        {
            auto coalesced_predicates = std::make_shared<ASTFunction>();
            coalesced_predicates->name = "and";
            coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
            coalesced_predicates->children.push_back(coalesced_predicates->arguments);
            coalesced_predicates->arguments->children = delete_filters;
            where_expression = std::move(coalesced_predicates);
        }
        select->where_expression = where_expression;
        select->children.push_back(where_expression);
    }

    interpreter_select = std::make_unique<InterpreterSelectQuery>(select, context, storage);

    is_prepared = true;
}

BlockInputStreamPtr MutationsInterpreter::execute()
{
    prepare();

    BlockInputStreamPtr in = interpreter_select->execute().in;
    for (size_t i_stage = 1; i_stage < stages.size(); ++i_stage)
    {
        const Stage & stage = stages[i_stage];

        for (size_t i = 0; i < stage.expressions_chain.steps.size(); ++i)
        {
            const auto & step = stage.expressions_chain.steps[i];
            if (i < stage.delete_filter_column_names.size())
            {
                /// Execute DELETE.
                in = std::make_shared<FilterBlockInputStream>(in, step.actions, stage.delete_filter_column_names[i]);
            }
            else
            {
                /// Execute UPDATE or final projection.
                in = std::make_shared<ExpressionBlockInputStream>(in, step.actions);
            }
        }
    }

    in = std::make_shared<MaterializingBlockInputStream>(in);

    return in;
}

}
