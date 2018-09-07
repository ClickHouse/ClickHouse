#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
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
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_UPDATE_COLUMN;
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


static NameSet getKeyColumns(const StoragePtr & storage)
{
    const MergeTreeData * merge_tree_data = nullptr;
    if (auto merge_tree = dynamic_cast<StorageMergeTree *>(storage.get()))
        merge_tree_data = &merge_tree->getData();
    else if (auto replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(storage.get()))
        merge_tree_data = &replicated_merge_tree->getData();
    else
        return {};

    NameSet key_columns;

    if (merge_tree_data->partition_expr)
        for (const String & col : merge_tree_data->partition_expr->getRequiredColumns())
            key_columns.insert(col);

    auto primary_expr = merge_tree_data->getPrimaryExpression();
    if (primary_expr)
        for (const String & col : primary_expr->getRequiredColumns())
            key_columns.insert(col);
    /// We don't process sampling_expression separately because it must be among the primary key columns.

    auto secondary_sort_expr = merge_tree_data->getSecondarySortExpression();
    if (secondary_sort_expr)
        for (const String & col : secondary_sort_expr->getRequiredColumns())
            key_columns.insert(col);

    if (!merge_tree_data->merging_params.sign_column.empty())
        key_columns.insert(merge_tree_data->merging_params.sign_column);

    return key_columns;
}

static void validateUpdateColumns(
    const StoragePtr & storage, const NameSet & updated_columns,
    const std::unordered_map<String, Names> & column_to_affected_materialized)
{
    NameSet key_columns = getKeyColumns(storage);

    for (const String & column_name : updated_columns)
    {
        auto found = false;
        for (const auto & col : storage->getColumns().ordinary)
        {
            if (col.name == column_name)
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            for (const auto & col : storage->getColumns().materialized)
            {
                if (col.name == column_name)
                    throw Exception("Cannot UPDATE materialized column `" + column_name + "`", ErrorCodes::CANNOT_UPDATE_COLUMN);
            }

            throw Exception("There is no column `" + column_name + "` in table", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        }

        if (key_columns.count(column_name))
            throw Exception("Cannot UPDATE key column `" + column_name + "`", ErrorCodes::CANNOT_UPDATE_COLUMN);

        auto materialized_it = column_to_affected_materialized.find(column_name);
        if (materialized_it != column_to_affected_materialized.end())
        {
            for (const String & materialized : materialized_it->second)
            {
                if (key_columns.count(materialized))
                    throw Exception("Updated column `" + column_name + "` affects MATERIALIZED column `"
                        + materialized + "`, which is a key column. Cannot UPDATE it.",
                        ErrorCodes::CANNOT_UPDATE_COLUMN);
            }
        }
    }
}


void MutationsInterpreter::prepare(bool dry_run)
{
    if (is_prepared)
        throw Exception("MutationsInterpreter is already prepared. It is a bug.", ErrorCodes::LOGICAL_ERROR);

    if (commands.empty())
        throw Exception("Empty mutation commands list", ErrorCodes::LOGICAL_ERROR);

    const ColumnsDescription & columns_desc = storage->getColumns();
    NamesAndTypesList all_columns = columns_desc.getAllPhysical();

    NameSet updated_columns;
    for (const MutationCommand & command : commands)
    {
        for (const auto & kv : command.column_to_update_expression)
            updated_columns.insert(kv.first);
    }

    /// We need to know which columns affect which MATERIALIZED columns to recalculate them if dependencies
    /// are updated.
    std::unordered_map<String, Names> column_to_affected_materialized;
    if (!updated_columns.empty())
    {
        for (const auto & kv : columns_desc.defaults)
        {
            const String & column = kv.first;
            const ColumnDefault & col_default = kv.second;
            if (col_default.kind == ColumnDefaultKind::Materialized)
            {
                ExpressionAnalyzer analyzer(col_default.expression->clone(), context, nullptr, all_columns);
                for (const String & dependency : analyzer.getRequiredSourceColumns())
                {
                    if (updated_columns.count(dependency))
                        column_to_affected_materialized[dependency].push_back(column);
                }
            }
        }
    }

    if (!updated_columns.empty())
        validateUpdateColumns(storage, updated_columns, column_to_affected_materialized);

    /// First, break a sequence of commands into stages.
    stages.emplace_back(context);
    for (const auto & command : commands)
    {
        if (!stages.back().column_to_updated.empty())
            stages.emplace_back(context);

        if (command.type == MutationCommand::DELETE)
        {
            auto negated_predicate = makeASTFunction("not", command.predicate->clone());
            stages.back().filters.push_back(negated_predicate);
        }
        else if (command.type == MutationCommand::UPDATE)
        {
            if (stages.size() == 1) /// First stage only supports filtering and can't update columns.
                stages.emplace_back(context);

            NameSet affected_materialized;

            for (const auto & kv : command.column_to_update_expression)
            {
                const String & column = kv.first;

                auto materialized_it = column_to_affected_materialized.find(column);
                if (materialized_it != column_to_affected_materialized.end())
                {
                    for (const String & mat_column : materialized_it->second)
                        affected_materialized.emplace(mat_column);
                }

                const auto & update_expr = kv.second;
                auto updated_column = makeASTFunction("CAST",
                    makeASTFunction("if",
                        command.predicate->clone(),
                        update_expr->clone(),
                        std::make_shared<ASTIdentifier>(column)),
                    std::make_shared<ASTLiteral>(columns_desc.getPhysical(column).type->getName()));
                stages.back().column_to_updated.emplace(column, updated_column);
            }

            if (!affected_materialized.empty())
            {
                stages.emplace_back(context);
                for (const auto & column : columns_desc.materialized)
                {
                    stages.back().column_to_updated.emplace(
                        column.name,
                        columns_desc.defaults.at(column.name).expression->clone());
                }
            }
        }
        else
            throw Exception("Unknown mutation command type: " + DB::toString<int>(command.type), ErrorCodes::UNKNOWN_MUTATION_COMMAND);
    }

    /// Next, for each stage calculate columns changed by this and previous stages.
    for (size_t i = 0; i < stages.size(); ++i)
    {
        if (!stages[i].filters.empty())
        {
            for (const auto & column : all_columns)
                stages[i].output_columns.insert(column.name);
            continue;
        }

        if (i > 0)
            stages[i].output_columns = stages[i - 1].output_columns;

        if (stages[i].output_columns.size() < all_columns.size())
        {
            for (const auto & kv : stages[i].column_to_updated)
                stages[i].output_columns.insert(kv.first);
        }
    }

    /// Now, calculate `expressions_chain` for each stage except the first.
    /// Do it backwards to propagate information about columns required as input for a stage to the previous stage.
    for (size_t i = stages.size() - 1; i > 0; --i)
    {
        auto & stage = stages[i];

        ASTPtr all_asts = std::make_shared<ASTExpressionList>();

        for (const auto & ast : stage.filters)
            all_asts->children.push_back(ast);

        for (const auto & kv : stage.column_to_updated)
            all_asts->children.push_back(kv.second);

        /// Add all output columns to prevent ExpressionAnalyzer from deleting them from source columns.
        for (const String & column : stage.output_columns)
            all_asts->children.push_back(std::make_shared<ASTIdentifier>(column));

        stage.analyzer = std::make_unique<ExpressionAnalyzer>(all_asts, context, nullptr, all_columns);

        ExpressionActionsChain & actions_chain = stage.expressions_chain;

        for (const auto & ast : stage.filters)
        {
            if (!actions_chain.steps.empty())
                actions_chain.addStep();
            stage.analyzer->appendExpression(actions_chain, ast, dry_run);
            stage.filter_column_names.push_back(ast->getColumnName());
        }

        if (!stage.column_to_updated.empty())
        {
            if (!actions_chain.steps.empty())
                actions_chain.addStep();

            for (const auto & kv : stage.column_to_updated)
                stage.analyzer->appendExpression(actions_chain, kv.second, dry_run);

            for (const auto & kv : stage.column_to_updated)
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

    if (!stages[0].filters.empty())
    {
        ASTPtr where_expression;
        if (stages[0].filters.size() == 1)
            where_expression = stages[0].filters[0];
        else
        {
            auto coalesced_predicates = std::make_shared<ASTFunction>();
            coalesced_predicates->name = "and";
            coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
            coalesced_predicates->children.push_back(coalesced_predicates->arguments);
            coalesced_predicates->arguments->children = stages[0].filters;
            where_expression = std::move(coalesced_predicates);
        }
        select->where_expression = where_expression;
        select->children.push_back(where_expression);
    }

    interpreter_select = std::make_unique<InterpreterSelectQuery>(select, context, storage, QueryProcessingStage::Complete, dry_run);

    is_prepared = true;
}

BlockInputStreamPtr MutationsInterpreter::addStreamsForLaterStages(BlockInputStreamPtr in) const
{
    for (size_t i_stage = 1; i_stage < stages.size(); ++i_stage)
    {
        const Stage & stage = stages[i_stage];

        for (size_t i = 0; i < stage.expressions_chain.steps.size(); ++i)
        {
            const auto & step = stage.expressions_chain.steps[i];
            if (i < stage.filter_column_names.size())
            {
                /// Execute DELETEs.
                in = std::make_shared<FilterBlockInputStream>(in, step.actions, stage.filter_column_names[i]);
            }
            else
            {
                /// Execute UPDATE or final projection.
                in = std::make_shared<ExpressionBlockInputStream>(in, step.actions);
            }
        }

        const SubqueriesForSets & subqueries_for_sets = stage.analyzer->getSubqueriesForSets();
        if (!subqueries_for_sets.empty())
        {
            const auto & settings = context.getSettingsRef();
            in = std::make_shared<CreatingSetsBlockInputStream>(in, subqueries_for_sets,
                SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode));
        }
    }

    in = std::make_shared<MaterializingBlockInputStream>(in);

    return in;
}

void MutationsInterpreter::validate()
{
    prepare(/* dry_run = */ true);
    Block first_stage_header = interpreter_select->getSampleBlock();
    BlockInputStreamPtr in = std::make_shared<NullBlockInputStream>(first_stage_header);
    addStreamsForLaterStages(in)->getHeader();
}

BlockInputStreamPtr MutationsInterpreter::execute()
{
    prepare(/* dry_run = */ false);
    BlockInputStreamPtr in = interpreter_select->execute().in;
    return addStreamsForLaterStages(in);
}

}
