#include <algorithm>
#include <limits>
#include <optional>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/arithmeticOverflow.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Databases/IDatabase.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/QueryPlan/ABR/ReadFromABR.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/ABR/StorageABR.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/checkAndGetLiteralArgument.h>

namespace ProfileEvents
{
    extern const Event ABRChooseBestTableDurationMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 abr_first_table_suffix;
    extern const SettingsUInt64 abr_last_table_suffix;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsSeconds max_execution_time_leaf;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_MANY_ROWS;
    extern const int QUERY_NOT_ALLOWED;
    extern const int NO_AVAILABLE_DATA;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

constexpr Int64 SECONDS_PER_DAY = 24 * 60 * 60;
constexpr UInt64 NANOSECONDS_PER_SECOND = 1'000'000'000;

/// The limits that are split between the attempts, see `StorageABR::buildAttemptContext`.
constexpr std::string_view SPLIT_SETTINGS[] = {
    "max_execution_time",
    "max_execution_time_leaf",
    "max_rows_to_read",
    "max_bytes_to_read",
    "max_rows_to_read_leaf",
    "max_bytes_to_read_leaf",
};

/// Unwraps the result of `QueryNode::toAST` (`ASTSelectWithUnionQuery` or `ASTSubquery`) down to the `ASTSelectQuery`.
ASTPtr unwrapSelectQuery(ASTPtr ast)
{
    while (true)
    {
        if (ast->as<ASTSelectQuery>())
            return ast;
        if (auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
            ast = select_with_union->list_of_selects->children.at(0);
        else if (auto * subquery = ast->as<ASTSubquery>())
            ast = subquery->children.at(0);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST {} when converting query node to select query", ast->getID());
    }
}

}

StorageABR::StorageABR(
    const StorageID & table_id,
    const String & table_prefix,
    const std::vector<UInt64> & sample_intervals_,
    const String & time_column_,
    const Names & retention_expression,
    ContextPtr context)
    : IStorage(table_id)
    , sample_intervals(sample_intervals_)
    , time_column(time_column_)
    , logger(table_id.getNameForLogs() + " (StorageABR)")
{
    const DatabasePtr & db = DatabaseCatalog::instance().getDatabase(table_id.database_name);

    copyTableMetadata(db->getTable(fmt::format("{}_{}", table_prefix, sample_intervals.front()), context));
    const auto metadata = getInMemoryMetadataPtr(context, false);
    const ColumnsDescription columns = metadata->getColumns();

    /// Iterate from sorted sample intervals to get tables sorted by sample interval
    for (const UInt64 & interval : sample_intervals)
    {
        const String name = fmt::format("{}_{}", table_prefix, interval);
        const StoragePtr table = db->tryGetTable(name, context);
        if (!table)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find table '{}' when creating '{}'", name, table_id.getTableName());

        if (!dynamic_cast<const MergeTreeData *>(table.get()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table '{}' is not from MergeTree family, which is not supported", name);

        const auto table_metadata = table->getInMemoryMetadataPtr(context, false);
        if (columns.getAll() != table_metadata->getColumns().getAll())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not all underlying tables have the same column set (names and types)");

        tables.emplace_back(table);
        LOG_DEBUG(logger, "Registered underlying table {}", name);
    }

    const auto & partition_key = metadata->getPartitionKey();
    const auto & first_merge_tree = dynamic_cast<const MergeTreeData &>(*tables.front());
    /// The partition key columns always come first in the part minmax index.
    const NamesAndTypesList minmax_columns = MergeTreeData::getMinMaxColumns(
        partition_key, first_merge_tree.getSettings(), MergeTreePartMinMaxIndexColumns::PARTITION_KEY_ONLY);
    const Names minmax_names = minmax_columns.getNames();
    const DataTypes minmax_types = minmax_columns.getTypes();
    NamesAndTypesList retention_cols_with_types;

    auto find_idx_minmax = [&](const String & col) -> size_t
    {
        auto it = std::ranges::find(minmax_names, col);
        if (it == minmax_names.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' from retention expression must be part of the partition key", col);

        return it - minmax_names.begin();
    };

    for (const auto & column : retention_expression)
    {
        size_t idx = find_idx_minmax(column);
        const auto & type = minmax_types[idx];
        column_info_minmax[column] = {.type = type, .idx_in_minmax = idx};
        retention_cols_with_types.emplace_back(column, type);
    }

    if (isNullableOrLowCardinalityNullable(column_info_minmax.at(time_column).type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time column '{}' must not be Nullable", time_column);

    for (const auto & [name, info] : column_info_minmax)
    {
        if (name != time_column)
        {
            retention_expr_col_types.emplace_back(info.type);
            retention_expr_col_names.push_back(name);
        }
    }

    /// Non-time retention columns must appear as bare identifiers in PARTITION BY.
    /// The time column is exempt because it typically appears inside a function (e.g. toStartOfMonth).
    for (const String & col : retention_expression)
    {
        auto is_direct_column = [&](const ASTPtr & child) -> bool
        {
            const auto * id = child->as<ASTIdentifier>();
            return col == time_column || (id && id->name() == col);
        };

        if (!std::ranges::any_of(partition_key.expression_list_ast->children, is_direct_column))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Retention column '{}' must appear as a direct (unmodified) column in the PARTITION BY expression",
                col);
    }

    retention_key_expr = std::make_shared<ExpressionActions>(ActionsDAG(retention_cols_with_types), ExpressionActionsSettings(context));

    LOG_DEBUG(logger, "Initialized: retention=({}), time_column='{}' (minmax_idx={}), {} underlying tables",
        fmt::join(retention_expression, ", "),
        time_column,
        column_info_minmax.at(time_column).idx_in_minmax,
        tables.size());
}

ASTPtr StorageABR::rewriteQueryForStorage(const SelectQueryInfo & query_info, const StoragePtr & target, const ContextPtr & context)
{
    ASTPtr select_ast;
    if (query_info.query_tree)
    {
        /// Replace the ABR table expression in the query tree with the underlying table and convert back to AST,
        /// so that the query is analyzed again against the underlying table.
        auto replacement_table_expression = std::make_shared<TableNode>(target, context);
        replacement_table_expression->setAlias(query_info.table_expression->getAlias());
        if (query_info.table_expression_modifiers)
            replacement_table_expression->setTableExpressionModifiers(*query_info.table_expression_modifiers);

        auto modified_query_tree = query_info.query_tree->cloneAndReplace(query_info.table_expression, replacement_table_expression);
        select_ast = unwrapSelectQuery(modified_query_tree->toAST());
    }
    else
    {
        select_ast = query_info.query->clone();
        select_ast->as<ASTSelectQuery &>().replaceDatabaseAndTable(target->getStorageID());
    }

    /// The time and read limits are recalculated per-attempt in buildAttemptContext
    if (auto settings_ast = select_ast->as<ASTSelectQuery &>().settings())
    {
        std::erase_if(settings_ast->as<ASTSetQuery &>().changes, [](const SettingChange & change)
        {
            return std::ranges::contains(SPLIT_SETTINGS, change.name);
        });
    }

    return select_ast;
}

ContextMutablePtr StorageABR::buildAttemptContext(
    const ContextPtr & base_context, const SelectQueryInfo & query_info, size_t tables_left, size_t total_tables)
{
    /// The `SETTINGS` of the query are not necessarily applied to the context passed to `read`
    /// (with the analyzer they belong to the query node), and the split limits are removed from the
    /// rewritten query, so apply them here before splitting the budgets.
    SettingsChanges query_settings;
    if (query_info.query_tree)
        query_settings = query_info.query_tree->as<const QueryNode &>().getSettingsChanges();
    else if (auto settings_ast = query_info.query->as<const ASTSelectQuery &>().settings())
        query_settings = settings_ast->as<const ASTSetQuery &>().changes;

    /// `tables_left` is the number of candidate tables from the one of this attempt to the one of
    /// abr_last_table_suffix, this one included; `total_tables` is the number of all candidate tables.
    ///
    /// Each attempt is given the execution time that the query has left, divided by `tables_left`:
    /// an attempt that fails quickly leaves more time to the next ones, and the total time across
    /// all attempts stays within the original `max_execution_time`. The time already spent is taken
    /// from the process list element of the query; without one, each attempt gets an equal share.
    ///
    /// Each attempt is given an equal share of the limits on the amount of data to read
    /// (`max_rows_to_read`, `max_bytes_to_read` and their `_leaf` variants), the limit divided by
    /// `total_tables`. The read limits of an attempt are checked against the data read by that
    /// attempt only (see `ReadFromABR::executeAndBuffer`), so a failed attempt does not reduce the
    /// share of the next one, and the total across all attempts stays within the original limit.
    ContextMutablePtr context = Context::createCopy(base_context);
    context->applySettingsChanges(query_settings);
    const auto & settings = context->getSettingsRef();

    std::optional<UInt64> elapsed_us;
    if (const auto query_status = base_context->getProcessListElementSafe())
        elapsed_us = query_status->getInfo().elapsed_microseconds;

    auto split_time_limit = [&](std::string_view name, Int64 total_us)
    {
        if (total_us <= 0)
            return;

        Float64 budget_us;
        if (elapsed_us)
        {
            const UInt64 remaining_us = static_cast<UInt64>(total_us) - std::min(static_cast<UInt64>(total_us), *elapsed_us);
            budget_us = static_cast<Float64>(remaining_us) / static_cast<Float64>(tables_left);
        }
        else
        {
            budget_us = static_cast<Float64>(total_us) / static_cast<Float64>(total_tables);
        }

        /// 0 means no limit, so the budget is at least 1 microsecond.
        context->applySettingChange(SettingChange(name, std::max<Float64>(budget_us, 1) / 1'000'000));
    };
    split_time_limit("max_execution_time", settings[Setting::max_execution_time].totalMicroseconds());
    split_time_limit("max_execution_time_leaf", settings[Setting::max_execution_time_leaf].totalMicroseconds());

    auto split_read_limit = [&](std::string_view name, UInt64 limit)
    {
        /// 0 means no limit, so a non-zero share is rounded up to at least 1.
        if (limit > 0)
            context->applySettingChange(SettingChange(name, std::max<UInt64>(1, limit / total_tables)));
    };
    split_read_limit("max_rows_to_read", settings[Setting::max_rows_to_read]);
    split_read_limit("max_bytes_to_read", settings[Setting::max_bytes_to_read]);
    split_read_limit("max_rows_to_read_leaf", settings[Setting::max_rows_to_read_leaf]);
    split_read_limit("max_bytes_to_read_leaf", settings[Setting::max_bytes_to_read_leaf]);
    return context;
}

StorageABR::Attempt StorageABR::buildAttempt(
    const SelectQueryInfo & query_info,
    const StoragePtr & target,
    const ContextPtr & context,
    size_t tables_left,
    size_t total_tables,
    QueryProcessingStage::Enum processed_stage)
{
    auto attempt_context = buildAttemptContext(context, query_info, tables_left, total_tables);
    auto ast = rewriteQueryForStorage(query_info, target, attempt_context);

    /// The result of the attempt is consumed by `ReadFromABR` in the same process, where nothing
    /// unmarshalls its blocks, so `BlocksMarshallingStep` must not be added to it.
    auto options = SelectQueryOptions(processed_stage);
    options.is_local_plan_for_distributed_query = true;

    /// The interpreters are built without `analyze()`, so they are ready to be executed.
    /// Computing the header here allows to reuse the interpreter on the first attempt without an extra analysis pass.
    if (query_info.query_tree)
    {
        auto interpreter = std::make_shared<InterpreterSelectQueryAnalyzer>(ast, attempt_context, options);
        auto header = interpreter->getSampleBlock();
        return {.interpreter = std::move(interpreter), .header = std::move(header)};
    }

    auto interpreter = std::make_shared<InterpreterSelectQuery>(ast, attempt_context, options);
    auto header = interpreter->getSampleBlock();
    return {.interpreter = std::move(interpreter), .header = std::move(header)};
}

bool StorageABR::isFallbackError(int error)
{
    return error == ErrorCodes::TOO_MANY_ROWS || error == ErrorCodes::TIMEOUT_EXCEEDED;
}

StorageABR::ColumnSizeByName StorageABR::getColumnSizes() const
{
    return tables.front()->getColumnSizes();
}

bool StorageABR::supportsPrewhere() const
{
    return std::ranges::all_of(tables, [](const auto & t) { return t->supportsPrewhere(); });
}

bool StorageABR::canMoveConditionsToPrewhere() const
{
    return std::ranges::all_of(tables, [](const auto & t) { return t->canMoveConditionsToPrewhere(); });
}

bool StorageABR::supportsFinal() const
{
    return std::ranges::all_of(tables, [](const auto & t) { return t->supportsFinal(); });
}

bool StorageABR::supportsSubcolumns() const
{
    return std::ranges::all_of(tables, [](const auto & t) { return t->supportsSubcolumns(); });
}

void StorageABR::copyTableMetadata(const StoragePtr & storage)
{
    if (!storage)
        return;

    StorageInMemoryMetadata metadata;
    const auto source = storage->getInMemoryMetadataPtr(nullptr, false);

    /// Set some useful metadata in ABR table itself
    metadata.setColumns(source->getColumns());
    metadata.primary_key = source->getPrimaryKey();
    metadata.partition_key = source->getPartitionKey();
    metadata.sorting_key = source->getSortingKey();
    setInMemoryMetadata(metadata);
}

std::shared_ptr<StorageABR> StorageABR::creatorFunction(const StorageFactory::Arguments & args)
{
    ASTs & engine_args = args.engine_args;
    if (engine_args.size() != 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage ABR requires exactly three arguments: table_prefix, sample_intervals and retention_expression");

    const ContextPtr & context = args.getContext();

    /// table_prefix -- the base prefix for all underlying tables
    String table_prefix = checkAndGetLiteralArgument<String>(
        evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], context),
        "table_prefix");

    if (table_prefix.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "table_prefix must not be empty");

    /// sample_intervals -- a list of the sample intervals used for the underlying tables
    Array interval_array = checkAndGetLiteralArgument<Array>(
        evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], context),
        "sample_intervals");

    if (interval_array.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "sample_intervals array must not be empty");

    std::vector<UInt64> sample_intervals;
    for (const Field & interval : interval_array)
    {
        if (interval.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "sample_intervals array's elements must be of type UInt64");

        sample_intervals.push_back(interval.safeGet<UInt64>());
    }

    /// sort and remove duplicates
    std::ranges::sort(sample_intervals);
    sample_intervals.erase(std::unique(sample_intervals.begin(), sample_intervals.end()), sample_intervals.end());
    if (sample_intervals.front() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "All sample intervals must be greater than 0");

    /// retention_expression -- tuple of columns which we use to calculate retention
    auto retention_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], context);
    Tuple retention_tuple;
    if (const auto * lit = retention_arg->as<ASTLiteral>(); lit && lit->value.getType() == Field::Types::String)
        retention_tuple.push_back(lit->value);
    else
        retention_tuple = checkAndGetLiteralArgument<Tuple>(retention_arg, "retention_expression");

    if (retention_tuple.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "retention_expression must not be empty");

    Names retention_expression;
    for (const Field & column : retention_tuple)
    {
        if (column.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "retention_expression tuple's elements must be of type String");

        retention_expression.push_back(column.safeGet<String>());
    }

    /// Assumes that first argument is the time column
    String time_column = retention_expression.front();

    return std::make_shared<StorageABR>(
        args.table_id,
        std::move(table_prefix),
        std::move(sample_intervals),
        std::move(time_column),
        std::move(retention_expression),
        context);
}

std::optional<StorageABR::UnixTimePoint> StorageABR::getMinMergeTreeDate(
    const MergeTreeData::SnapshotData & snapshot,
    const KeyCondition & retention_condition) const
{
    std::optional<UnixTimePoint> min_date;
    const size_t time_column_idx_minmax = column_info_minmax.at(time_column).idx_in_minmax;
    size_t total_parts = 0;
    size_t pruned_parts = 0;
    size_t skipped_parts = 0;
    Hyperrectangle sub_rect;

    for (const auto & part : *snapshot.parts)
    {
        ++total_parts;

        const auto minmax_idx = part.data_part->getMinMaxIndex();
        if (!minmax_idx || !minmax_idx->initialized)
        {
            ++skipped_parts;
            continue;
        }

        const auto & hyperrectangle = minmax_idx->hyperrectangle;

        /// Defensive check: idx_in_minmax was computed at ABR construction time from the
        /// ABR table's own partition key. If an underlying table's partition key has since
        /// diverged (e.g. ALTER on one of the underlying tables), the cached indices may
        /// exceed this part's hyperrectangle size. Skip such parts rather than risk UB.
        const bool indices_in_range = std::ranges::all_of(
            column_info_minmax, [&](const auto & name_and_info) { return name_and_info.second.idx_in_minmax < hyperrectangle.size(); });
        if (!indices_in_range)
        {
            ++skipped_parts;
            continue;
        }

        sub_rect.clear();
        for (const auto & [name, info] : column_info_minmax)
        {
            if (name != time_column)
                sub_rect.push_back(hyperrectangle[info.idx_in_minmax]);
        }

        if (!retention_condition.checkInHyperrectangle(sub_rect, retention_expr_col_types).can_be_true)
        {
            ++pruned_parts;
            continue;
        }

        const auto & min_val = hyperrectangle[time_column_idx_minmax].left;
        const UnixTimePoint date = timeValueToUnixTimePoint(min_val).value_or(UnixTimePoint{});
        min_date = std::min(min_date.value_or(date), date);
    }

    LOG_DEBUG(logger, "getMinMergeTreeDate: total_parts={}, pruned={}, skipped={}, surviving={}, min_date={}",
        total_parts, pruned_parts, skipped_parts, total_parts - pruned_parts - skipped_parts,
        min_date ? DateLUT::instance().timeToString(min_date->seconds) : "none");

    return min_date;
}

std::optional<ActionsDAG> StorageABR::buildFilterActionsDAG(
    const SelectQueryInfo & query_info,
    ContextPtr context) const
{
    /// Fast path: the analyzer (PlannerJoinTree) populates both
    /// query_info.filter_actions_dag (the WHERE condition) and
    /// query_info.prewhere_info before storage->read() is called.
    /// Reuse them and skip a full InterpreterSelectQuery analysis pass.
    /// The legacy InterpreterSelectQuery path leaves filter_actions_dag null,
    /// so we fall back to building everything from scratch in that case.
    ///
    /// buildFilterActionsDAG copies nodes into a new DAG, so the source nodes
    /// only need to live until the call returns; query_info outlives this
    /// function, so passing raw pointers into its DAGs is safe.
    if (query_info.filter_actions_dag || query_info.query_tree)
    {
        ActionsDAG::NodeRawConstPtrs filter_nodes;
        if (query_info.filter_actions_dag)
            for (const auto * output : query_info.filter_actions_dag->getOutputs())
                filter_nodes.push_back(output);

        if (const auto & pw = query_info.prewhere_info; pw && !pw->prewhere_actions.getOutputs().empty())
            filter_nodes.push_back(&pw->prewhere_actions.findInOutputs(pw->prewhere_column_name));

        return ActionsDAG::buildFilterActionsDAG(filter_nodes);
    }

    InterpreterSelectQuery interpreter(query_info.query, context, SelectQueryOptions().analyze());
    ActionDAGNodes filter_nodes = MergeTreeData::getFiltersForPrimaryKeyAnalysis(interpreter);
    const auto & analysis_result = interpreter.getAnalysisResult();
    if (analysis_result.hasPrewhere())
    {
        const auto & pw = analysis_result.prewhere_info;
        if (pw && !pw->prewhere_actions.getOutputs().empty())
            filter_nodes.nodes.push_back(&pw->prewhere_actions.findInOutputs(pw->prewhere_column_name));
    }
    return ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
}

std::optional<StorageABR::UnixTimePoint> StorageABR::getQueryLowerBound(const KeyCondition & time_cond) const
{
    /// Conjuncts that cannot be analyzed are ignored, so the ranges are a conservative superset of the condition.
    const Ranges ranges = time_cond.extractBounds();

    const auto time_type = removeLowCardinality(column_info_minmax.at(time_column).type);
    const WhichDataType which_time_type(time_type);
    std::optional<UnixTimePoint> min_left;
    for (const auto & range : ranges)
    {
        if (range.left.isNull() || range.left.isNegativeInfinity())
            return std::nullopt; /// unbounded range -> no finite lower bound

        Field lower_bound = range.left;
        if (which_time_type.isDateTime64())
        {
            /// The bound keeps the scale of the literal, which may differ from the scale of the column.
            /// Bring it to the scale of the column, rounding down, which keeps it a valid lower bound.
            const auto & date_time = lower_bound.safeGet<DecimalField<DateTime64>>();
            Int64 ticks = date_time.getValue().value;
            const UInt32 scale = date_time.getScale();
            const UInt32 column_scale = getDecimalScale(*time_type);
            if (scale < column_scale)
            {
                if (common::mulOverflow(ticks, DecimalUtils::scaleMultiplier<Int64>(column_scale - scale), ticks))
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Cannot route query: the lower bound on '{}' does not fit into the type of the column",
                        time_column);
            }
            else if (scale > column_scale)
            {
                ticks /= DecimalUtils::scaleMultiplier<Int64>(scale - column_scale);
            }

            /// Range::shrinkToIncludedIfPossible() advances exclusive integer-backed bounds
            /// by one unit. DateTime64 uses DecimalField, so advance it by one tick of the column here.
            if (!range.left_included)
            {
                if (ticks == std::numeric_limits<Int64>::max())
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Cannot route query with '{} > ...': the exclusive lower bound is the maximum "
                        "representable DateTime64 value. Use a smaller timestamp",
                        time_column);
                ++ticks;
            }

            lower_bound = DecimalField<DateTime64>(DateTime64(ticks), column_scale);
        }

        const auto left = timeValueToUnixTimePoint(lower_bound);
        if (!left)
            throw Exception(
                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "ABR does not support query bounds before 1970-01-01");

        min_left = std::min(min_left.value_or(*left), *left);
    }
    return min_left;
}

std::optional<StorageABR::UnixTimePoint> StorageABR::timeValueToUnixTimePoint(const Field & value) const
{
    const auto time_type = removeLowCardinality(column_info_minmax.at(time_column).type);
    const WhichDataType which_time_type(time_type);

    if (which_time_type.isDateTime64())
    {
        const auto & date_time = value.safeGet<DecimalField<DateTime64>>();
        const Int64 ticks = date_time.getValue().value;

        if (ticks < 0)
            return std::nullopt;

        const UInt64 scale_multiplier = date_time.getScaleMultiplier().value;
        const UInt64 unsigned_ticks = static_cast<UInt64>(ticks);
        return UnixTimePoint{
            .seconds = unsigned_ticks / scale_multiplier,
            .nanoseconds = static_cast<UInt32>(
                (unsigned_ticks % scale_multiplier) * (NANOSECONDS_PER_SECOND / scale_multiplier))};
    }

    if (which_time_type.isDateOrDate32() || which_time_type.isDateTime())
    {
        Int64 timestamp = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
        if (timestamp < 0)
            return std::nullopt;

        if (which_time_type.isDateOrDate32())
            timestamp *= SECONDS_PER_DAY;

        return UnixTimePoint{.seconds = static_cast<UInt64>(timestamp), .nanoseconds = 0};
    }

    return UnixTimePoint{.seconds = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value), .nanoseconds = 0};
}

size_t StorageABR::chooseBestTable(SelectQueryInfo & query_info, ContextPtr context) const
{
    ProfileEventTimeIncrement<Time::Microseconds> watch(ProfileEvents::ABRChooseBestTableDurationMicroseconds);
    const auto abr_info = context->getABRQueryInfoPtr();

    auto record_retention_skip = [&](size_t idx)
    {
        if (!abr_info)
            return;
        std::lock_guard lock(abr_info->mutex);
        /// Don't clobber a more specific code (e.g. TOO_MANY_ROWS from a previous
        /// retry) if one is already there. In practice chooseBestTable runs before
        /// any execution attempts, so the map is empty here.
        abr_info->exception_codes.try_emplace(sample_intervals[idx], ErrorCodes::NO_AVAILABLE_DATA);
    };

    auto filter_dag = buildFilterActionsDAG(query_info, context);
    if (!filter_dag.has_value())
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED,
            "Query must filter on the time column '{}'. Add a condition that uses it on the WHERE clause",
            time_column);

    const ActionsDAGWithInversionPushDown filter_dag_with_inversion(filter_dag->getOutputs().front(), context, /* boolean_context */ true);

    KeyCondition time_cond(filter_dag_with_inversion, context, {time_column}, retention_key_expr);
    std::optional<UnixTimePoint> min_query_date = getQueryLowerBound(time_cond);
    if (!min_query_date.has_value())
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED,
            "Query must have a lower bound on the time column '{}'. Add a condition that uses it on the WHERE clause",
            time_column);

    LOG_DEBUG(logger, "Query lower bound: {}", DateLUT::instance().timeToString(min_query_date->seconds));
    if (abr_info)
        abr_info->min_date.store(min_query_date->seconds);

    KeyCondition retention_condition(filter_dag_with_inversion, context, retention_expr_col_names, retention_key_expr);
    LOG_DEBUG(logger, "Retention condition: {}", retention_condition.toString());

    /// Resolve start/end indices from abr_first_table_suffix / abr_last_table_suffix.
    /// These settings override retention-based selection: tables outside the
    /// [first, last] range are excluded even if retention would choose them.
    /// If no table in the allowed range covers the query's time range, the
    /// query falls back to the table at last_suffix rather than a coarser one.
    /// The retries after a fallback error do not go beyond last_suffix either.
    UInt64 first_suffix = context->getSettingsRef()[Setting::abr_first_table_suffix];
    if (first_suffix == 0)
        first_suffix = sample_intervals.front();

    const size_t start = getTableIndexFromSuffix(first_suffix);
    const size_t end = getLastCandidateTableIndex(context);
    if (start > end)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "abr_first_table_suffix ({}) must be less than or equal to abr_last_table_suffix ({})",
            first_suffix,
            sample_intervals[end]);

    /// Store minimum dates for each table in the selected range, indexed by (i - start)
    std::vector<std::optional<UnixTimePoint>> min_storage_dates(end - start + 1);

    /// Iterate finest to coarsest within [start, end].
    for (size_t i = start; i <= end; ++i)
    {
        const StoragePtr & storage = tables.at(i);
        const String name = storage->getStorageID().getNameForLogs();
        const auto storage_metadata = storage->getInMemoryMetadataPtr(context, false);
        const StorageSnapshotPtr snapshot = storage->getStorageSnapshot(storage_metadata, context);

        const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*snapshot->data);
        const auto min_storage_date = getMinMergeTreeDate(snapshot_data, retention_condition);

        if (!min_storage_date.has_value())
        {
            LOG_DEBUG(logger, "Table {} skipped: all parts pruned by retention condition", name);
            record_retention_skip(i);
            continue;
        }

        LOG_DEBUG(logger, "Table {} min_date={}", name, DateLUT::instance().timeToString(min_storage_date->seconds));
        min_storage_dates[i - start] = min_storage_date.value();

        if (min_query_date.value() >= min_storage_date.value())
        {
            LOG_DEBUG(logger, "Selected table {}", name);
            return i;
        }

        LOG_DEBUG(logger, "Table {} skipped: min_date > query lower bound", name);
        record_retention_skip(i);
    }

    const size_t fallback_idx = pickFallbackTable(min_storage_dates, start);
    const auto & fallback_min_date = min_storage_dates[fallback_idx - start];
    /// Retract the NO_AVAILABLE_DATA tag if the fallback has data;
    /// keep it when every table was pruned
    if (abr_info && fallback_min_date.has_value())
    {
        std::lock_guard lock(abr_info->mutex);
        abr_info->exception_codes.erase(sample_intervals[fallback_idx]);
    }

    if (fallback_min_date.has_value())
        LOG_DEBUG(logger, "No table covers query range, falling back to {} (oldest timestamp at day precision: {})",
            tables[fallback_idx]->getStorageID().getNameForLogs(), DateLUT::instance().dateToString(fallback_min_date->seconds));
    else
        LOG_DEBUG(logger, "No table covers query range, falling back to {}", tables[fallback_idx]->getStorageID().getNameForLogs());

    return fallback_idx;
}

size_t StorageABR::getTableIndexFromSuffix(UInt64 suffix) const
{
    auto it = std::ranges::find(sample_intervals, suffix);
    if (it == sample_intervals.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No table was found with suffix {}", suffix);
    return it - sample_intervals.begin();
}

size_t StorageABR::getLastCandidateTableIndex(const ContextPtr & context) const
{
    const UInt64 last_suffix = context->getSettingsRef()[Setting::abr_last_table_suffix];
    if (last_suffix == 0)
        return tables.size() - 1;
    return getTableIndexFromSuffix(last_suffix);
}

size_t StorageABR::pickFallbackTable(
    const std::vector<std::optional<UnixTimePoint>> & min_storage_dates,
    size_t start) const
{
    /// Pick the table with the oldest min_storage_date at day precision (the "best coverage" group)
    /// Among ties, pick the smallest index, i.e. the finest sample interval (highest data quality)
    const auto & date_lut = DateLUT::instance();
    std::optional<Int32> min_day_num;
    size_t selected_offset = 0;

    for (size_t offset = 0; offset < min_storage_dates.size(); ++offset)
    {
        if (!min_storage_dates[offset].has_value())
            continue;

        const Int32 day_num = static_cast<Int32>(date_lut.toDayNum(static_cast<time_t>(min_storage_dates[offset]->seconds)));
        if (!min_day_num.has_value() || day_num < min_day_num.value())
        {
            min_day_num = day_num;
            selected_offset = offset;
        }
    }

    return start + selected_offset;
}

void StorageABR::read(
    QueryPlan & query_plan,
    const Names & /* column_names */,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    Stopwatch choose_watch;
    auto abr_info = context->getABRQueryInfoPtr();
    const size_t initial_table_idx = chooseBestTable(query_info, context);
    if (abr_info)
        abr_info->duration_ms.fetch_add(choose_watch.elapsedMilliseconds());
    const size_t last_table_idx = getLastCandidateTableIndex(context);

    /// The retries go from the chosen table to the one of abr_last_table_suffix, and the limits are split between them.
    const size_t num_candidates = last_table_idx - initial_table_idx + 1;
    auto initial_attempt = buildAttempt(
        query_info, tables.at(initial_table_idx), context, num_candidates, num_candidates, processed_stage);
    LOG_DEBUG(logger, "Starting read from table {} ({} candidates for retry)",
        tables.at(initial_table_idx)->getStorageID().getNameForLogs(),
        num_candidates);

    auto step = std::make_unique<ReadFromABR>(
        query_info,
        std::span<const StoragePtr>(tables).first(last_table_idx + 1),
        std::span<const UInt64>(sample_intervals).first(last_table_idx + 1),
        initial_table_idx,
        std::move(initial_attempt),
        context,
        processed_stage,
        logger.load());

    query_plan.addStep(std::move(step));
}

void registerStorageABR(StorageFactory & factory);
void registerStorageABR(StorageFactory & factory)
{
    factory.registerStorage(
        "ABR",
        StorageABR::creatorFunction,
        SecretArgumentsSpec{},
        {.supports_schema_inference = true},
        Documentation{
            .description = R"DOCS_MD(
The `ABR` (Adaptive Bit Rate) engine is a read-only proxy that routes queries to the best of several `MergeTree`-family tables
that store the same data at different sampling granularities. It picks the finest-grained underlying table whose retention covers
the query's time range, and on `TOO_MANY_ROWS` or `TIMEOUT_EXCEEDED` it transparently retries the query on the next coarser table.

The underlying tables are expected to follow a naming convention of `{table_prefix}_{sample_interval}`, where `sample_interval`
distinguishes the granularity. The engine never writes; data must be ingested directly into the underlying tables.

**Engine parameters**

- `table_prefix` — A `String` literal giving the common prefix of the underlying tables. For each interval in `sample_intervals`,
  `ABR` discovers the table `{table_prefix}_{interval}`. All discovered tables must exist in the same database as the `ABR` table,
  be from the `MergeTree` family, and share an identical column set (names and types).
- `sample_intervals` — A non-empty `Array(UInt64)` of granularity levels. Values are sorted ascending and deduplicated; `0` is not
  allowed. The smallest interval is the finest-grained table and is preferred whenever it has enough retention; the largest
  interval is the coarsest fallback.
- `retention_expression` — A `Tuple(String, ...)` (or a single `String`) of partition key column names that `ABR` uses for
  pruning. The first element must be the time column, of type `Date`, `Date32`, `DateTime` or `DateTime64` (optionally wrapped
  into `LowCardinality`, but not `Nullable`). All other columns must appear as direct, unmodified columns in the `PARTITION BY`
  expression of the underlying tables. The time column is exempt, as it typically appears inside a function such as
  `toStartOfMonth(ts)`.

**How routing works**

1. `ABR` analyzes the query's `WHERE` and `PREWHERE` clauses and extracts a lower bound on the time column. Queries without a
   finite lower bound are rejected with `QUERY_NOT_ALLOWED`.
2. `ABR` walks each candidate table from finest to coarsest. For each table it reads the partition minmax index, prunes parts
   whose retention columns don't satisfy the query, and computes the earliest surviving timestamp.
3. The first table whose earliest surviving timestamp is not later than the query's lower bound is selected. If none qualify,
   `ABR` falls back to the table with the broadest coverage at day precision, breaking ties toward the finest interval.
4. The query is executed on the selected table. If it raises `TOO_MANY_ROWS` or `TIMEOUT_EXCEEDED`, `ABR` retries it on the next
   coarser table. The output of an attempt is buffered (in memory, then on disk) so that the partial output of a failed attempt is
   never visible to the client.

The settings `abr_memory_buffer_size`, `abr_first_table_suffix` and `abr_last_table_suffix` tune this behavior.

**Limits**

The limits on the execution time (`max_execution_time`, `max_execution_time_leaf`) and on the amount of data to read
(`max_rows_to_read`, `max_bytes_to_read`, `max_rows_to_read_leaf`, `max_bytes_to_read_leaf`) are the budget of the query as a
whole, across all attempts. The candidate tables are the chosen table and the coarser ones, up to the one allowed by
`abr_last_table_suffix`.

- Each attempt gets the execution time the query has left, divided by the number of candidate tables left, this attempt
  included. An attempt that fails quickly leaves more time to the next ones.
- Each attempt gets an equal share of the read limits: the limit divided by the number of candidate tables. It is checked
  against the data it read itself, so a failed attempt does not reduce the share of the next one. For example, to allow each
  attempt to read up to 1 million rows with three candidate tables, set `max_rows_to_read = 3000000`.

**Limitations**

- `ABR` is read-only. `INSERT`, `ALTER` and mutations must target the underlying tables directly.
- All underlying tables must share an identical column set.
)DOCS_MD",
            .syntax = "ENGINE = ABR(table_prefix, sample_intervals, retention_expression)",
            .examples = {{
                .name = "Route a query to the finest table that covers its time range",
                .query = R"(
CREATE TABLE events_1 (ts DateTime, region String, value UInt64)
ENGINE = MergeTree PARTITION BY (toYYYYMM(ts), region) ORDER BY ts;
CREATE TABLE events_10 AS events_1;
CREATE TABLE events_100 AS events_1;

CREATE TABLE events (ts DateTime, region String, value UInt64)
ENGINE = ABR('events', [1, 10, 100], ('ts', 'region'));

SELECT count() FROM events WHERE ts >= now() - INTERVAL 1 DAY AND region = 'eu';
)",
                .result = "",
            }},
            .introduced_in = {26, 10},
        });
}

}
