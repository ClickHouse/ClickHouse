#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/StorageMergeTree.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/CheckSortedTransform.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/LightweightDeleteDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_UPDATE_COLUMN;
}

namespace
{

/// Helps to detect situations, where non-deterministic functions may be used in mutations of Replicated*MergeTree.
class FirstNonDeterministicFunctionMatcher
{
public:
    struct Data
    {
        ContextPtr context;
        std::optional<String> nondeterministic_function_name;
        bool subquery = false;
    };

    static bool needChildVisit(const ASTPtr & /*node*/, const ASTPtr & /*child*/)
    {
        return true;
    }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.nondeterministic_function_name || data.subquery)
            return;

        if (node->as<ASTSelectQuery>())
        {
            /// We cannot determine if subquery is deterministic or not,
            /// so we do not allow to use subqueries in mutation without allow_nondeterministic_mutations=1
            data.subquery = true;
        }
        else if (const auto * function = typeid_cast<const ASTFunction *>(node.get()))
        {
            /// Property of being deterministic for lambda expression is completely determined
            /// by the contents of its definition, so we just proceed to it.
            if (function->name != "lambda")
            {
                /// NOTE It may be an aggregate function, so get(...) may throw.
                /// However, an aggregate function can be used only in subquery and we do not go into subquery.
                const auto func = FunctionFactory::instance().get(function->name, data.context);
                if (!func->isDeterministic())
                    data.nondeterministic_function_name = func->getName();
            }
        }
    }
};

using FirstNonDeterministicFunctionFinder = InDepthNodeVisitor<FirstNonDeterministicFunctionMatcher, true>;
using FirstNonDeterministicFunctionData = FirstNonDeterministicFunctionMatcher::Data;

FirstNonDeterministicFunctionData findFirstNonDeterministicFunctionName(const MutationCommand & command, ContextPtr context)
{
    FirstNonDeterministicFunctionMatcher::Data finder_data{context, std::nullopt, false};

    switch (command.type)
    {
        case MutationCommand::UPDATE:
        {
            auto update_assignments_ast = command.ast->as<const ASTAlterCommand &>().update_assignments->clone();
            FirstNonDeterministicFunctionFinder(finder_data).visit(update_assignments_ast);

            if (finder_data.nondeterministic_function_name)
                return finder_data;

            /// Currently UPDATE and DELETE both always have predicates so we can use fallthrough
            [[fallthrough]];
        }

        case MutationCommand::DELETE:
        {
            auto predicate_ast = command.predicate->clone();
            FirstNonDeterministicFunctionFinder(finder_data).visit(predicate_ast);

            return finder_data;
        }

        default:
            break;
    }

    return {};
}

ASTPtr prepareQueryAffectedAST(const std::vector<MutationCommand> & commands, const StoragePtr & storage, ContextPtr context)
{
    /// Execute `SELECT count() FROM storage WHERE predicate1 OR predicate2 OR ...` query.
    /// The result can differ from the number of affected rows (e.g. if there is an UPDATE command that
    /// changes how many rows satisfy the predicates of the subsequent commands).
    /// But we can be sure that if count = 0, then no rows will be touched.

    auto select = std::make_shared<ASTSelectQuery>();

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    auto count_func = std::make_shared<ASTFunction>();
    count_func->name = "count";
    count_func->arguments = std::make_shared<ASTExpressionList>();
    select->select()->children.push_back(count_func);

    ASTs conditions;
    for (const MutationCommand & command : commands)
    {
        if (ASTPtr condition = getPartitionAndPredicateExpressionForMutationCommand(command, storage, context))
            conditions.push_back(std::move(condition));
    }

    if (conditions.size() > 1)
    {
        auto coalesced_predicates = makeASTFunction("or");
        coalesced_predicates->arguments->children = std::move(conditions);
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(coalesced_predicates));
    }
    else if (conditions.size() == 1)
    {
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(conditions.front()));
    }

    return select;
}

ColumnDependencies getAllColumnDependencies(const StorageMetadataPtr & metadata_snapshot, const NameSet & updated_columns)
{
    NameSet new_updated_columns = updated_columns;
    ColumnDependencies dependencies;
    while (!new_updated_columns.empty())
    {
        auto new_dependencies = metadata_snapshot->getColumnDependencies(new_updated_columns, true);
        new_updated_columns.clear();
        for (const auto & dependency : new_dependencies)
        {
            if (!dependencies.contains(dependency))
            {
                dependencies.insert(dependency);
                if (!dependency.isReadOnly())
                    new_updated_columns.insert(dependency.column_name);
            }
        }
    }

    return dependencies;
}

}


bool isStorageTouchedByMutations(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MutationCommand> & commands,
    ContextMutablePtr context_copy)
{
    if (commands.empty())
        return false;

    bool all_commands_can_be_skipped = true;
    auto storage_from_merge_tree_data_part = std::dynamic_pointer_cast<StorageFromMergeTreeDataPart>(storage);
    for (const MutationCommand & command : commands)
    {
        if (!command.predicate) /// The command touches all rows.
            return true;

        if (command.partition && !storage_from_merge_tree_data_part)
            throw Exception("ALTER UPDATE/DELETE ... IN PARTITION is not supported for non-MergeTree tables", ErrorCodes::NOT_IMPLEMENTED);

        if (command.partition && storage_from_merge_tree_data_part)
        {
            const String partition_id = storage_from_merge_tree_data_part->getPartitionIDFromQuery(command.partition, context_copy);
            if (partition_id == storage_from_merge_tree_data_part->getPartitionId())
                all_commands_can_be_skipped = false;
        }
        else
            all_commands_can_be_skipped = false;
    }

    if (all_commands_can_be_skipped)
        return false;

    context_copy->setSetting("max_streams_to_max_threads_ratio", 1);
    context_copy->setSetting("max_threads", 1);

    ASTPtr select_query = prepareQueryAffectedAST(commands, storage, context_copy);

    /// Interpreter must be alive, when we use result of execute() method.
    /// For some reason it may copy context and and give it into ExpressionTransform
    /// after that we will use context from destroyed stack frame in our stream.
    InterpreterSelectQuery interpreter(
        select_query, context_copy, storage, metadata_snapshot, SelectQueryOptions().ignoreLimits().ignoreProjections());
    auto io = interpreter.execute();
    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    while (executor.pull(block)) {}

    if (!block.rows())
        return false;
    else if (block.rows() != 1)
        throw Exception("count() expression returned " + toString(block.rows()) + " rows, not 1",
            ErrorCodes::LOGICAL_ERROR);

    Block tmp_block;
    while (executor.pull(tmp_block));

    auto count = (*block.getByName("count()").column)[0].get<UInt64>();
    return count != 0;
}


ASTPtr getPartitionAndPredicateExpressionForMutationCommand(
    const MutationCommand & command,
    const StoragePtr & storage,
    ContextPtr context
)
{
    ASTPtr partition_predicate_as_ast_func;
    if (command.partition)
    {
        String partition_id;

        auto storage_merge_tree = std::dynamic_pointer_cast<MergeTreeData>(storage);
        auto storage_from_merge_tree_data_part = std::dynamic_pointer_cast<StorageFromMergeTreeDataPart>(storage);
        if (storage_merge_tree)
            partition_id = storage_merge_tree->getPartitionIDFromQuery(command.partition, context);
        else if (storage_from_merge_tree_data_part)
            partition_id = storage_from_merge_tree_data_part->getPartitionIDFromQuery(command.partition, context);
        else
            throw Exception("ALTER UPDATE/DELETE ... IN PARTITION is not supported for non-MergeTree tables", ErrorCodes::NOT_IMPLEMENTED);

        partition_predicate_as_ast_func = makeASTFunction("equals",
                    std::make_shared<ASTIdentifier>("_partition_id"),
                    std::make_shared<ASTLiteral>(partition_id)
        );
    }

    if (command.predicate && command.partition)
        return makeASTFunction("and", command.predicate->clone(), std::move(partition_predicate_as_ast_func));
    else
        return command.predicate ? command.predicate->clone() : partition_predicate_as_ast_func;
}


MutationsInterpreter::MutationsInterpreter(
    StoragePtr storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    MutationCommands commands_,
    ContextPtr context_,
    bool can_execute_)
    : storage(std::move(storage_))
    , metadata_snapshot(metadata_snapshot_)
    , commands(std::move(commands_))
    , context(Context::createCopy(context_))
    , can_execute(can_execute_)
    , select_limits(SelectQueryOptions().analyze(!can_execute).ignoreLimits().ignoreProjections())
{
    mutation_ast = prepare(!can_execute);
}

static NameSet getKeyColumns(const StoragePtr & storage, const StorageMetadataPtr & metadata_snapshot)
{
    const MergeTreeData * merge_tree_data = dynamic_cast<const MergeTreeData *>(storage.get());
    if (!merge_tree_data)
        return {};

    NameSet key_columns;

    for (const String & col : metadata_snapshot->getColumnsRequiredForPartitionKey())
        key_columns.insert(col);

    for (const String & col : metadata_snapshot->getColumnsRequiredForSortingKey())
        key_columns.insert(col);
    /// We don't process sample_by_ast separately because it must be among the primary key columns.

    if (!merge_tree_data->merging_params.sign_column.empty())
        key_columns.insert(merge_tree_data->merging_params.sign_column);

    if (!merge_tree_data->merging_params.version_column.empty())
        key_columns.insert(merge_tree_data->merging_params.version_column);

    return key_columns;
}

static bool materializeTTLRecalculateOnly(const StoragePtr & storage)
{
    auto storage_from_merge_tree_data_part = std::dynamic_pointer_cast<StorageFromMergeTreeDataPart>(storage);
    if (!storage_from_merge_tree_data_part)
        return false;

    return storage_from_merge_tree_data_part->materializeTTLRecalculateOnly();
}

static void validateUpdateColumns(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot, const NameSet & updated_columns,
    const std::unordered_map<String, Names> & column_to_affected_materialized)
{
    NameSet key_columns = getKeyColumns(storage, metadata_snapshot);

    for (const String & column_name : updated_columns)
    {
        auto found = false;
        for (const auto & col : metadata_snapshot->getColumns().getOrdinary())
        {
            if (col.name == column_name)
            {
                found = true;
                break;
            }
        }

        /// Allow to override value of lightweight delete filter virtual column
        if (!found && column_name == LightweightDeleteDescription::filter_column.name)
            found = true;

        if (!found)
        {
            for (const auto & col : metadata_snapshot->getColumns().getMaterialized())
            {
                if (col.name == column_name)
                    throw Exception("Cannot UPDATE materialized column " + backQuote(column_name), ErrorCodes::CANNOT_UPDATE_COLUMN);
            }

            throw Exception("There is no column " + backQuote(column_name) + " in table", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        }

        if (key_columns.contains(column_name))
            throw Exception("Cannot UPDATE key column " + backQuote(column_name), ErrorCodes::CANNOT_UPDATE_COLUMN);

        auto materialized_it = column_to_affected_materialized.find(column_name);
        if (materialized_it != column_to_affected_materialized.end())
        {
            for (const String & materialized : materialized_it->second)
            {
                if (key_columns.contains(materialized))
                    throw Exception("Updated column " + backQuote(column_name) + " affects MATERIALIZED column "
                        + backQuote(materialized) + ", which is a key column. Cannot UPDATE it.",
                        ErrorCodes::CANNOT_UPDATE_COLUMN);
            }
        }
    }
}

/// Returns ASTs of updated nested subcolumns, if all of subcolumns were updated.
/// They are used to validate sizes of nested arrays.
/// If some of subcolumns were updated and some weren't,
/// it makes sense to validate only updated columns with their old versions,
/// because their sizes couldn't change, since sizes of all nested subcolumns must be consistent.
static std::optional<std::vector<ASTPtr>> getExpressionsOfUpdatedNestedSubcolumns(
    const String & column_name,
    const NamesAndTypesList & all_columns,
    const std::unordered_map<String, ASTPtr> & column_to_update_expression)
{
    std::vector<ASTPtr> res;
    auto source_name = Nested::splitName(column_name).first;

    /// Check this nested subcolumn
    for (const auto & column : all_columns)
    {
        auto split = Nested::splitName(column.name);
        if (isArray(column.type) && split.first == source_name && !split.second.empty())
        {
            auto it = column_to_update_expression.find(column.name);
            if (it == column_to_update_expression.end())
                return {};

            res.push_back(it->second);
        }
    }

    return res;
}

ASTPtr MutationsInterpreter::prepare(bool dry_run)
{
    if (is_prepared)
        throw Exception("MutationsInterpreter is already prepared. It is a bug.", ErrorCodes::LOGICAL_ERROR);

    if (commands.empty())
        throw Exception("Empty mutation commands list", ErrorCodes::LOGICAL_ERROR);

    const ColumnsDescription & columns_desc = metadata_snapshot->getColumns();
    const IndicesDescription & indices_desc = metadata_snapshot->getSecondaryIndices();
    const ProjectionsDescription & projections_desc = metadata_snapshot->getProjections();
    NamesAndTypesList all_columns = columns_desc.getAllPhysical();

    NameSet updated_columns;
    bool materialize_ttl_recalculate_only = materializeTTLRecalculateOnly(storage);

    for (const MutationCommand & command : commands)
    {
        if (command.type == MutationCommand::Type::UPDATE
            || command.type == MutationCommand::Type::DELETE)
            materialize_ttl_recalculate_only = false;

        for (const auto & kv : command.column_to_update_expression)
        {
            updated_columns.insert(kv.first);
        }
    }

    /// We need to know which columns affect which MATERIALIZED columns, data skipping indices
    /// and projections to recalculate them if dependencies are updated.
    std::unordered_map<String, Names> column_to_affected_materialized;
    if (!updated_columns.empty())
    {
        for (const auto & column : columns_desc)
        {
            if (column.default_desc.kind == ColumnDefaultKind::Materialized)
            {
                auto query = column.default_desc.expression->clone();
                auto syntax_result = TreeRewriter(context).analyze(query, all_columns);
                for (const String & dependency : syntax_result->requiredSourceColumns())
                {
                    if (updated_columns.contains(dependency))
                        column_to_affected_materialized[dependency].push_back(column.name);
                }
            }
        }

        validateUpdateColumns(storage, metadata_snapshot, updated_columns, column_to_affected_materialized);
    }

    dependencies = getAllColumnDependencies(metadata_snapshot, updated_columns);

    /// First, break a sequence of commands into stages.
    for (auto & command : commands)
    {
        if (command.type == MutationCommand::DELETE)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);

            auto negated_predicate = makeASTFunction("isZeroOrNull", getPartitionAndPredicateExpressionForMutationCommand(command));
            stages.back().filters.push_back(negated_predicate);
        }
        else if (command.type == MutationCommand::UPDATE)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);
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

                /// When doing UPDATE column = expression WHERE condition
                /// we will replace column to the result of the following expression:
                ///
                /// CAST(if(condition, CAST(expression, type), column), type)
                ///
                /// Inner CAST is needed to make 'if' work when branches have no common type,
                /// example: type is UInt64, UPDATE x = -1 or UPDATE x = x - 1.
                ///
                /// Outer CAST is added just in case if we don't trust the returning type of 'if'.

                DataTypePtr type;
                if (auto physical_column = columns_desc.tryGetPhysical(column))
                    type = physical_column->type;
                else if (column == LightweightDeleteDescription::filter_column.name)
                    type = LightweightDeleteDescription::filter_column.type;
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column {}", column);

                auto type_literal = std::make_shared<ASTLiteral>(type->getName());

                const auto & update_expr = kv.second;

                ASTPtr condition = getPartitionAndPredicateExpressionForMutationCommand(command);

                /// And new check validateNestedArraySizes for Nested subcolumns
                if (isArray(type) && !Nested::splitName(column).second.empty())
                {
                    std::shared_ptr<ASTFunction> function = nullptr;

                    auto nested_update_exprs = getExpressionsOfUpdatedNestedSubcolumns(column, all_columns, command.column_to_update_expression);
                    if (!nested_update_exprs)
                    {
                        function = makeASTFunction("validateNestedArraySizes",
                            condition,
                            update_expr->clone(),
                            std::make_shared<ASTIdentifier>(column));
                        condition = makeASTFunction("and", condition, function);
                    }
                    else if (nested_update_exprs->size() > 1)
                    {
                        function = std::make_shared<ASTFunction>();
                        function->name = "validateNestedArraySizes";
                        function->arguments = std::make_shared<ASTExpressionList>();
                        function->children.push_back(function->arguments);
                        function->arguments->children.push_back(condition);
                        for (const auto & it : *nested_update_exprs)
                            function->arguments->children.push_back(it->clone());
                        condition = makeASTFunction("and", condition, function);
                    }
                }

                auto updated_column = makeASTFunction("_CAST",
                    makeASTFunction("if",
                        condition,
                        makeASTFunction("_CAST",
                            update_expr->clone(),
                            type_literal),
                        std::make_shared<ASTIdentifier>(column)),
                    type_literal);

                stages.back().column_to_updated.emplace(column, updated_column);
            }

            if (!affected_materialized.empty())
            {
                stages.emplace_back(context);
                for (const auto & column : columns_desc)
                {
                    if (column.default_desc.kind == ColumnDefaultKind::Materialized)
                    {
                        stages.back().column_to_updated.emplace(
                            column.name,
                            column.default_desc.expression->clone());
                    }
                }
            }
        }
        else if (command.type == MutationCommand::MATERIALIZE_COLUMN)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);
            if (stages.size() == 1) /// First stage only supports filtering and can't update columns.
                stages.emplace_back(context);

            const auto & column = columns_desc.get(command.column_name);

            if (!column.default_desc.expression)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot materialize column `{}` because it doesn't have default expression", column.name);

            auto materialized_column = makeASTFunction(
                "_CAST", column.default_desc.expression->clone(), std::make_shared<ASTLiteral>(column.type->getName()));

            stages.back().column_to_updated.emplace(column.name, materialized_column);
        }
        else if (command.type == MutationCommand::MATERIALIZE_INDEX)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_PROJECTION);
            auto it = std::find_if(
                    std::cbegin(indices_desc), std::end(indices_desc),
                    [&](const IndexDescription & index)
                    {
                        return index.name == command.index_name;
                    });
            if (it == std::cend(indices_desc))
                throw Exception("Unknown index: " + command.index_name, ErrorCodes::BAD_ARGUMENTS);

            auto query = (*it).expression_list_ast->clone();
            auto syntax_result = TreeRewriter(context).analyze(query, all_columns);
            const auto required_columns = syntax_result->requiredSourceColumns();
            for (const auto & column : required_columns)
                dependencies.emplace(column, ColumnDependency::SKIP_INDEX);
            materialized_indices.emplace(command.index_name);
        }
        else if (command.type == MutationCommand::MATERIALIZE_PROJECTION)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_PROJECTION);
            const auto & projection = projections_desc.get(command.projection_name);
            for (const auto & column : projection.required_columns)
                dependencies.emplace(column, ColumnDependency::PROJECTION);
            materialized_projections.emplace(command.projection_name);
        }
        else if (command.type == MutationCommand::DROP_INDEX)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_PROJECTION);
            materialized_indices.erase(command.index_name);
        }
        else if (command.type == MutationCommand::DROP_PROJECTION)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_PROJECTION);
            materialized_projections.erase(command.projection_name);
        }
        else if (command.type == MutationCommand::MATERIALIZE_TTL)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            if (materialize_ttl_recalculate_only)
            {
                // just recalculate ttl_infos without remove expired data
                auto all_columns_vec = all_columns.getNames();
                auto new_dependencies = metadata_snapshot->getColumnDependencies(NameSet(all_columns_vec.begin(), all_columns_vec.end()), false);
                for (const auto & dependency : new_dependencies)
                {
                    if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
                        dependencies.insert(dependency);
                }
            }
            else if (metadata_snapshot->hasRowsTTL()
                || metadata_snapshot->hasAnyRowsWhereTTL()
                || metadata_snapshot->hasAnyGroupByTTL())
            {
                for (const auto & column : all_columns)
                    dependencies.emplace(column.name, ColumnDependency::TTL_TARGET);
            }
            else
            {
                NameSet new_updated_columns;
                auto column_ttls = metadata_snapshot->getColumns().getColumnTTLs();
                for (const auto & elem : column_ttls)
                {
                    dependencies.emplace(elem.first, ColumnDependency::TTL_TARGET);
                    new_updated_columns.insert(elem.first);
                }

                auto all_columns_vec = all_columns.getNames();
                auto all_dependencies = getAllColumnDependencies(metadata_snapshot, NameSet(all_columns_vec.begin(), all_columns_vec.end()));

                for (const auto & dependency : all_dependencies)
                {
                    if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
                        dependencies.insert(dependency);
                }

                /// Recalc only skip indices and projections of columns which could be updated by TTL.
                auto new_dependencies = metadata_snapshot->getColumnDependencies(new_updated_columns, true);
                for (const auto & dependency : new_dependencies)
                {
                    if (dependency.kind == ColumnDependency::SKIP_INDEX || dependency.kind == ColumnDependency::PROJECTION)
                        dependencies.insert(dependency);
                }
            }

            if (dependencies.empty())
            {
                /// Very rare case. It can happen if we have only one MOVE TTL with constant expression.
                /// But we still have to read at least one column.
                dependencies.emplace(all_columns.front().name, ColumnDependency::TTL_EXPRESSION);
            }
        }
        else if (command.type == MutationCommand::READ_COLUMN)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);
            if (stages.size() == 1) /// First stage only supports filtering and can't update columns.
                stages.emplace_back(context);

            stages.back().column_to_updated.emplace(command.column_name, std::make_shared<ASTIdentifier>(command.column_name));
        }
        else
            throw Exception("Unknown mutation command type: " + DB::toString<int>(command.type), ErrorCodes::UNKNOWN_MUTATION_COMMAND);
    }

    /// We care about affected indices and projections because we also need to rewrite them
    /// when one of index columns updated or filtered with delete.
    /// The same about columns, that are needed for calculation of TTL expressions.
    if (!dependencies.empty())
    {
        NameSet changed_columns;
        NameSet unchanged_columns;
        for (const auto & dependency : dependencies)
        {
            if (dependency.isReadOnly())
                unchanged_columns.insert(dependency.column_name);
            else
                changed_columns.insert(dependency.column_name);
        }

        if (!changed_columns.empty())
        {
            if (stages.empty() || !stages.back().column_to_updated.empty())
                stages.emplace_back(context);
            if (stages.size() == 1) /// First stage only supports filtering and can't update columns.
                stages.emplace_back(context);

            for (const auto & column : changed_columns)
                stages.back().column_to_updated.emplace(
                    column, std::make_shared<ASTIdentifier>(column));
        }

        if (!unchanged_columns.empty())
        {
            if (!stages.empty())
            {
                std::vector<Stage> stages_copy;
                /// Copy all filled stages except index calculation stage.
                for (const auto & stage : stages)
                {
                    stages_copy.emplace_back(context);
                    stages_copy.back().column_to_updated = stage.column_to_updated;
                    stages_copy.back().output_columns = stage.output_columns;
                    stages_copy.back().filters = stage.filters;
                }

                const ASTPtr select_query = prepareInterpreterSelectQuery(stages_copy, /* dry_run = */ true);
                InterpreterSelectQuery interpreter{
                    select_query, context, storage, metadata_snapshot,
                    SelectQueryOptions().analyze(/* dry_run = */ false).ignoreLimits().ignoreProjections()};

                auto first_stage_header = interpreter.getSampleBlock();
                QueryPlan plan;
                auto source = std::make_shared<NullSource>(first_stage_header);
                plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source))));
                auto pipeline = addStreamsForLaterStages(stages_copy, plan);
                updated_header = std::make_unique<Block>(pipeline.getHeader());
            }

            /// Special step to recalculate affected indices, projections and TTL expressions.
            stages.emplace_back(context);
            for (const auto & column : unchanged_columns)
                stages.back().column_to_updated.emplace(
                    column, std::make_shared<ASTIdentifier>(column));
        }
    }

    is_prepared = true;

    return prepareInterpreterSelectQuery(stages, dry_run);
}

ASTPtr MutationsInterpreter::prepareInterpreterSelectQuery(std::vector<Stage> & prepared_stages, bool dry_run)
{
    auto storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects();
    auto all_columns = storage_snapshot->getColumns(options);

    /// Add _row_exists column if it is present in the part
    if (auto part_storage = dynamic_pointer_cast<DB::StorageFromMergeTreeDataPart>(storage))
    {
        if (part_storage->hasLightweightDeletedMask())
            all_columns.push_back({LightweightDeleteDescription::filter_column});
    }

    /// Next, for each stage calculate columns changed by this and previous stages.
    for (size_t i = 0; i < prepared_stages.size(); ++i)
    {
        if (!prepared_stages[i].filters.empty())
        {
            for (const auto & column : all_columns)
                prepared_stages[i].output_columns.insert(column.name);
            continue;
        }

        if (i > 0)
            prepared_stages[i].output_columns = prepared_stages[i - 1].output_columns;

        /// Make sure that all updated columns are included into output_columns set.
        /// This is important for a "hidden" column like _row_exists gets because it is a virtual column
        /// and so it is not in the list of AllPhysical columns.
        for (const auto & kv : prepared_stages[i].column_to_updated)
            prepared_stages[i].output_columns.insert(kv.first);
    }

    /// Now, calculate `expressions_chain` for each stage except the first.
    /// Do it backwards to propagate information about columns required as input for a stage to the previous stage.
    for (size_t i = prepared_stages.size() - 1; i > 0; --i)
    {
        auto & stage = prepared_stages[i];

        ASTPtr all_asts = std::make_shared<ASTExpressionList>();

        for (const auto & ast : stage.filters)
            all_asts->children.push_back(ast);

        for (const auto & kv : stage.column_to_updated)
            all_asts->children.push_back(kv.second);

        /// Add all output columns to prevent ExpressionAnalyzer from deleting them from source columns.
        for (const String & column : stage.output_columns)
            all_asts->children.push_back(std::make_shared<ASTIdentifier>(column));

        /// Executing scalar subquery on that stage can lead to deadlock
        /// e.g. ALTER referencing the same table in scalar subquery
        bool execute_scalar_subqueries = !dry_run;
        auto syntax_result = TreeRewriter(context).analyze(
            all_asts, all_columns, storage, storage_snapshot,
            false, true, execute_scalar_subqueries);

        if (execute_scalar_subqueries && context->hasQueryContext())
            for (const auto & it : syntax_result->getScalars())
                context->getQueryContext()->addScalar(it.first, it.second);

        stage.analyzer = std::make_unique<ExpressionAnalyzer>(all_asts, syntax_result, context);

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

            auto & actions = actions_chain.getLastStep().actions();

            for (const auto & kv : stage.column_to_updated)
            {
                auto column_name = kv.second->getColumnName();
                const auto & dag_node = actions->findInIndex(column_name);
                const auto & alias = actions->addAlias(dag_node, kv.first);
                actions->addOrReplaceInIndex(alias);
            }
        }

        /// Remove all intermediate columns.
        actions_chain.addStep();
        actions_chain.getLastStep().required_output.clear();
        ActionsDAG::NodeRawConstPtrs new_index;
        for (const auto & name : stage.output_columns)
            actions_chain.getLastStep().addRequiredOutput(name);

        actions_chain.getLastActions();

        actions_chain.finalize();

        /// Propagate information about columns needed as input.
        for (const auto & column : actions_chain.steps.front()->getRequiredColumns())
            prepared_stages[i - 1].output_columns.insert(column.name);
    }

    /// Execute first stage as a SELECT statement.

    auto select = std::make_shared<ASTSelectQuery>();

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    for (const auto & column_name : prepared_stages[0].output_columns)
        select->select()->children.push_back(std::make_shared<ASTIdentifier>(column_name));

    /// Don't let select list be empty.
    if (select->select()->children.empty())
        select->select()->children.push_back(std::make_shared<ASTLiteral>(Field(0)));

    if (!prepared_stages[0].filters.empty())
    {
        ASTPtr where_expression;
        if (prepared_stages[0].filters.size() == 1)
            where_expression = prepared_stages[0].filters[0];
        else
        {
            auto coalesced_predicates = std::make_shared<ASTFunction>();
            coalesced_predicates->name = "and";
            coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
            coalesced_predicates->children.push_back(coalesced_predicates->arguments);
            coalesced_predicates->arguments->children = prepared_stages[0].filters;
            where_expression = std::move(coalesced_predicates);
        }
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    }

    return select;
}

QueryPipelineBuilder MutationsInterpreter::addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, QueryPlan & plan) const
{
    for (size_t i_stage = 1; i_stage < prepared_stages.size(); ++i_stage)
    {
        const Stage & stage = prepared_stages[i_stage];

        for (size_t i = 0; i < stage.expressions_chain.steps.size(); ++i)
        {
            const auto & step = stage.expressions_chain.steps[i];
            if (i < stage.filter_column_names.size())
            {
                /// Execute DELETEs.
                plan.addStep(std::make_unique<FilterStep>(plan.getCurrentDataStream(), step->actions(), stage.filter_column_names[i], false));
            }
            else
            {
                /// Execute UPDATE or final projection.
                plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), step->actions()));
            }
        }

        SubqueriesForSets & subqueries_for_sets = stage.analyzer->getSubqueriesForSets();
        if (!subqueries_for_sets.empty())
        {
            const Settings & settings = context->getSettingsRef();
            SizeLimits network_transfer_limits(
                    settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode);
            addCreatingSetsStep(plan, std::move(subqueries_for_sets), network_transfer_limits, context);
        }
    }

    QueryPlanOptimizationSettings do_not_optimize_plan;
    do_not_optimize_plan.optimize_plan = false;

    auto pipeline = std::move(*plan.buildQueryPipeline(
        do_not_optimize_plan,
        BuildQueryPipelineSettings::fromContext(context)));

    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<MaterializingTransform>(header);
    });

    return pipeline;
}

void MutationsInterpreter::validate()
{
    if (!select_interpreter)
        select_interpreter = std::make_unique<InterpreterSelectQuery>(mutation_ast, context, storage, metadata_snapshot, select_limits);

    const Settings & settings = context->getSettingsRef();

    /// For Replicated* storages mutations cannot employ non-deterministic functions
    /// because that produces inconsistencies between replicas
    if (startsWith(storage->getName(), "Replicated") && !settings.allow_nondeterministic_mutations)
    {
        for (const auto & command : commands)
        {
            const auto nondeterministic_func_data = findFirstNonDeterministicFunctionName(command, context);
            if (nondeterministic_func_data.subquery)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER UPDATE/ALTER DELETE statement with subquery may be nondeterministic, "
                                                           "see allow_nondeterministic_mutations setting");

            if (nondeterministic_func_data.nondeterministic_function_name)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ALTER UPDATE/ALTER DELETE statements must use only deterministic functions. "
                    "Function '{}' is non-deterministic", *nondeterministic_func_data.nondeterministic_function_name);
        }
    }

    QueryPlan plan;
    select_interpreter->buildQueryPlan(plan);
    auto pipeline = addStreamsForLaterStages(stages, plan);
}

QueryPipelineBuilder MutationsInterpreter::execute()
{
    if (!can_execute)
        throw Exception("Cannot execute mutations interpreter because can_execute flag set to false", ErrorCodes::LOGICAL_ERROR);

    if (!select_interpreter)
    {
        /// Skip to apply deleted mask for MutateSomePartColumn cases when part has lightweight delete.
        if (!apply_deleted_mask)
        {
            auto context_for_reading = Context::createCopy(context);
            context_for_reading->setApplyDeletedMask(apply_deleted_mask);
            select_interpreter = std::make_unique<InterpreterSelectQuery>(mutation_ast, context_for_reading, storage, metadata_snapshot, select_limits);
        }
        else
            select_interpreter = std::make_unique<InterpreterSelectQuery>(mutation_ast, context, storage, metadata_snapshot, select_limits);
    }


    QueryPlan plan;
    select_interpreter->buildQueryPlan(plan);

    auto builder = addStreamsForLaterStages(stages, plan);

    /// Sometimes we update just part of columns (for example UPDATE mutation)
    /// in this case we don't read sorting key, so just we don't check anything.
    if (auto sort_desc = getStorageSortDescriptionIfPossible(builder.getHeader()))
    {
        builder.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<CheckSortedTransform>(header, *sort_desc);
        });
    }

    if (!updated_header)
        updated_header = std::make_unique<Block>(builder.getHeader());

    return builder;
}

Block MutationsInterpreter::getUpdatedHeader() const
{
    // If it's an index/projection materialization, we don't write any data columns, thus empty header is used
    return mutation_kind.mutation_kind == MutationKind::MUTATE_INDEX_PROJECTION ? Block{} : *updated_header;
}

const ColumnDependencies & MutationsInterpreter::getColumnDependencies() const
{
    return dependencies;
}

size_t MutationsInterpreter::evaluateCommandsSize()
{
    for (const MutationCommand & command : commands)
        if (unlikely(!command.predicate && !command.partition)) /// The command touches all rows.
            return mutation_ast->size();

    return std::max(prepareQueryAffectedAST(commands, storage, context)->size(), mutation_ast->size());
}

std::optional<SortDescription> MutationsInterpreter::getStorageSortDescriptionIfPossible(const Block & header) const
{
    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
    {
        if (header.has(sort_columns[i]))
            sort_description.emplace_back(sort_columns[i], 1, 1);
        else
            return {};
    }

    return sort_description;
}

ASTPtr MutationsInterpreter::getPartitionAndPredicateExpressionForMutationCommand(const MutationCommand & command) const
{
    return DB::getPartitionAndPredicateExpressionForMutationCommand(command, storage, context);
}

bool MutationsInterpreter::Stage::isAffectingAllColumns(const Names & storage_columns) const
{
    /// is subset
    for (const auto & storage_column : storage_columns)
        if (!output_columns.contains(storage_column))
            return false;

    return true;
}

bool MutationsInterpreter::isAffectingAllColumns() const
{
    auto storage_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
    if (stages.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation interpreter has no stages");

    return stages.back().isAffectingAllColumns(storage_columns);
}

void MutationsInterpreter::MutationKind::set(const MutationKindEnum & kind)
{
    if (mutation_kind < kind)
        mutation_kind = kind;
}

}
