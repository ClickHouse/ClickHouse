#include <algorithm>
#include <limits>
#include <optional>

#include <base/arithmeticOverflow.h>
#include <fmt/format.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Databases/IDatabase.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
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
#include <Storages/StorageProxy.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/getEffectiveRowPolicyFilter.h>

namespace ProfileEvents
{
    extern const Event ABRChooseBestTableDurationMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_abr;
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
    extern const int ACCESS_DENIED;
    extern const int UNKNOWN_TABLE;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
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

/// Returns a lower bound on a `DateTime64` column as ticks and their scale.
/// The bound is either a decimal or a whole number of seconds.
std::pair<Int64, UInt32> boundToTicksAndScale(const Field & bound, const String & time_column)
{
    auto out_of_range = [&]
    {
        return Exception(
            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
            "Cannot route query: the lower bound on '{}' does not fit into the type of the column",
            time_column);
    };

    auto narrow = [&](const auto & value) -> Int64
    {
        if (value < std::numeric_limits<Int64>::min() || value > std::numeric_limits<Int64>::max())
            throw out_of_range();
        return static_cast<Int64>(value);
    };

    switch (bound.getType())
    {
        case Field::Types::UInt64:
        {
            const UInt64 seconds = bound.safeGet<UInt64>();
            if (seconds > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw out_of_range();
            return {static_cast<Int64>(seconds), 0};
        }
        case Field::Types::Int64:
            return {bound.safeGet<Int64>(), 0};
        case Field::Types::Decimal32:
        {
            const auto & decimal = bound.safeGet<DecimalField<Decimal32>>();
            return {decimal.getValue().value, decimal.getScale()};
        }
        case Field::Types::Decimal64:
        {
            const auto & decimal = bound.safeGet<DecimalField<DateTime64>>();
            return {decimal.getValue().value, decimal.getScale()};
        }
        case Field::Types::Decimal128:
        {
            const auto & decimal = bound.safeGet<DecimalField<Decimal128>>();
            return {narrow(decimal.getValue().value), decimal.getScale()};
        }
        case Field::Types::Decimal256:
        {
            const auto & decimal = bound.safeGet<DecimalField<Decimal256>>();
            return {narrow(decimal.getValue().value), decimal.getScale()};
        }
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected type {} of the lower bound on '{}'", bound.getTypeName(), time_column);
    }
}

}

StorageABR::StorageABR(
    const StorageID & table_id,
    const String & table_prefix_,
    const std::vector<UInt64> & sample_intervals_,
    const Names & retention_expression_)
    : IStorage(table_id, std::make_unique<StorageInMemoryMetadata>())
    , table_prefix(table_prefix_)
    , sample_intervals(sample_intervals_)
    , retention_expression(retention_expression_)
    , time_column(retention_expression.front())
    , logger(table_id.getNameForLogs() + " (StorageABR)")
{
}

StoragePtr StorageABR::tryGetFinestSourceTable() const
{
    const String database = getStorageID().database_name;
    for (const UInt64 interval : sample_intervals)
    {
        if (auto table = DatabaseCatalog::instance().tryGetTable(
                StorageID(database, getSourceTableName(table_prefix, interval)), Context::getGlobalContextInstance()))
            return table;
    }
    return nullptr;
}

StorageMetadataHandle StorageABR::getInMemoryMetadataPtr(ContextPtr query_context, bool bypass_metadata_cache) const
{
    StorageMetadataHandle own_metadata = IStorage::getInMemoryMetadataPtr(query_context, bypass_metadata_cache);
    const auto table = tryGetFinestSourceTable();
    if (!table)
        return own_metadata;

    const auto table_metadata = table->getInMemoryMetadataPtr(query_context, bypass_metadata_cache);
    auto metadata = std::make_shared<StorageInMemoryMetadata>(*own_metadata);
    metadata->setColumns(table_metadata->getColumns());
    return metadata;
}

bool StorageABR::isExposedMetadataGranted(const ContextPtr & query_context, AccessType access_type, const String & column_name) const
{
    const auto table = tryGetFinestSourceTable();
    if (!table)
        return true; /// No columns are exposed.
    if (!query_context)
        return false;

    const auto table_id = table->getStorageID();
    const auto access = query_context->getAccess();
    return column_name.empty() ? access->isGranted(access_type, table_id.database_name, table_id.table_name)
                               : access->isGranted(access_type, table_id.database_name, table_id.table_name, column_name);
}

std::optional<NameSet> StorageABR::filterColumnsWithExposedMetadataGranted(
    const ContextPtr & query_context, AccessType access_type, const Names & column_names) const
{
    const auto table = tryGetFinestSourceTable();
    if (!table)
        return std::nullopt;

    NameSet granted;
    if (!query_context)
        return granted;

    const auto table_id = table->getStorageID();
    const auto access = query_context->getAccess();
    for (const auto & column_name : column_names)
    {
        if (access->isGranted(access_type, table_id.database_name, table_id.table_name, column_name))
            granted.insert(column_name);
    }
    return granted;
}

StorageABR::Retention StorageABR::getRetention(const ColumnsDescription & columns, const ContextPtr & context) const
{
    Retention retention;
    NamesAndTypesList retention_columns;
    for (const String & name : retention_expression)
    {
        const auto column = columns.tryGetPhysical(name);
        if (!column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' from retention expression does not exist", name);

        retention_columns.push_back(*column);
        if (name != time_column)
        {
            retention.names.push_back(name);
            retention.types.push_back(column->type);
        }
    }

    retention.time_type = retention_columns.front().type;
    if (isNullableOrLowCardinalityNullable(retention.time_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time column '{}' must not be Nullable", time_column);

    retention.key_expr = std::make_shared<ExpressionActions>(ActionsDAG(retention_columns), ExpressionActionsSettings(context));
    return retention;
}

void StorageABR::checkSourceTables(const ContextPtr & context) const
{
    const auto first_table_metadata = getSourceTable(sample_intervals.front(), context)->getInMemoryMetadataPtr(context, false);
    const ColumnsDescription & columns = first_table_metadata->getColumns();
    for (const SourceTable & table : getSourceTables(sample_intervals, getRetention(columns, context), context))
    {
        const auto table_metadata = table.storage->getInMemoryMetadataPtr(context, false);
        if (columns.getAll() != table_metadata->getColumns().getAll())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not all source tables have the same column set (names and types)");
    }
}

ASTPtr StorageABR::rewriteQueryForStorage(const SelectQueryInfo & query_info, const StoragePtr & target, const ContextPtr & context)
{
    ASTPtr select_ast;
    if (query_info.query_tree)
    {
        /// Replace the ABR table expression in the query tree with the source table and convert back to AST,
        /// so that the query is analyzed again against the source table.
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

        Float64 budget_us = static_cast<Float64>(total_us) / static_cast<Float64>(total_tables);
        if (elapsed_us)
        {
            const UInt64 remaining_us = static_cast<UInt64>(total_us) - std::min(static_cast<UInt64>(total_us), *elapsed_us);
            budget_us = static_cast<Float64>(remaining_us) / static_cast<Float64>(tables_left);
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
        return {.interpreter = std::move(interpreter), .header = std::move(header), .context = attempt_context};
    }

    auto interpreter = std::make_shared<InterpreterSelectQuery>(ast, attempt_context, options);
    auto header = interpreter->getSampleBlock();
    return {.interpreter = std::move(interpreter), .header = std::move(header), .context = attempt_context};
}

bool StorageABR::isFallbackError(int error)
{
    return error == ErrorCodes::TOO_MANY_ROWS || error == ErrorCodes::TIMEOUT_EXCEEDED;
}

String StorageABR::getSourceTableName(const String & prefix, UInt64 interval)
{
    return fmt::format("{}_{}", prefix, interval);
}

TableNamesSet StorageABR::getSourceTableNames(const ASTFunction & engine, const String & database)
{
    if (!engine.arguments || engine.arguments->children.size() != 3)
        return {};

    const auto & args = engine.arguments->children;

    String table_prefix;
    if (const auto * literal = args[0]->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
        table_prefix = literal->value.safeGet<String>();
    else if (const auto * identifier = args[0]->as<ASTIdentifier>())
        table_prefix = identifier->name();
    if (table_prefix.empty())
        return {};

    const auto * intervals = args[1]->as<ASTLiteral>();
    if (table_prefix.empty() || !intervals || intervals->value.getType() != Field::Types::Array)
        return {};

    TableNamesSet names;
    for (const Field & interval : intervals->value.safeGet<Array>())
    {
        if (interval.getType() == Field::Types::UInt64)
            names.emplace(QualifiedTableName{.database = database, .table = getSourceTableName(table_prefix, interval.safeGet<UInt64>())});
    }
    return names;
}

std::span<const UInt64> StorageABR::getCandidateIntervals(const ContextPtr & context) const
{
    auto find_interval = [&](UInt64 suffix, std::vector<UInt64>::const_iterator unset)
    {
        if (suffix == 0)
            return unset;
        const auto it = std::ranges::find(sample_intervals, suffix);
        if (it == sample_intervals.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No table was found with suffix {}", suffix);
        return it;
    };

    const auto & settings = context->getSettingsRef();
    const auto first = find_interval(settings[Setting::abr_first_table_suffix], sample_intervals.begin());
    const auto last = find_interval(settings[Setting::abr_last_table_suffix], std::prev(sample_intervals.end()));
    if (first > last)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "abr_first_table_suffix ({}) must be less than or equal to abr_last_table_suffix ({})", *first, *last);

    return {first, std::next(last)};
}

StoragePtr StorageABR::getSourceTable(UInt64 interval, const ContextPtr & context) const
{
    const StorageID table_id = getStorageID();
    const String name = getSourceTableName(table_prefix, interval);
    const StoragePtr table = DatabaseCatalog::instance().getDatabase(table_id.database_name)->tryGetTable(name, context);
    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Source table {} of {} does not exist", name, table_id.getNameForLogs());

    auto merge_tree = castStorage<MergeTreeData>(table, DeferredTable::Load);
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Source table {} is not from MergeTree family, which is not supported",
            table->getStorageID().getNameForLogs());

    return merge_tree;
}

StorageABR::SourceTable StorageABR::resolveSourceTable(UInt64 interval, const Retention & retention, const ContextPtr & context) const
{
    SourceTable table{
        .storage = getSourceTable(interval, context),
        .interval = interval,
        .time_idx_in_minmax = 0,
        .retention_idx_in_minmax = {}};
    const String name = table.storage->getStorageID().getNameForLogs();
    const auto metadata = table.storage->getInMemoryMetadataPtr(context, false);
    const auto & partition_key = metadata->getPartitionKey();
    const auto settings = castStorage<MergeTreeData>(table.storage, DeferredTable::Load)->getSettings();
    const NamesAndTypesList minmax_columns
        = MergeTreeData::getMinMaxColumns(partition_key, settings, MergeTreePartMinMaxIndexColumns::PARTITION_KEY_ONLY);

    auto find_idx_in_minmax = [&](const String & column, const DataTypePtr & type) -> size_t
    {
        const auto it = std::ranges::find(minmax_columns, column, &NameAndTypePair::name);
        if (it == minmax_columns.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' from retention expression must be part of the partition key of table {}", column, name);
        if (!it->type->equals(*type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' from retention expression has type {} in table {}, but {} in the ABR table",
                column, it->type->getName(), name, type->getName());
        return static_cast<size_t>(std::distance(minmax_columns.begin(), it));
    };

    table.time_idx_in_minmax = find_idx_in_minmax(time_column, retention.time_type);

    /// Non-time retention columns must appear as bare identifiers in `PARTITION BY`.
    /// This guarantees that each part only holds one value for retention columns other than time.
    /// Otherwise, we could end up choosing source tables that do not contain the data we need: a part could hold several
    /// values of the column, so its minmax range could match the filter of the query even if the part has no matching rows.
    for (size_t i = 0; i < retention.names.size(); ++i)
    {
        const String & column = retention.names[i];
        auto is_bare_column = [&](const ASTPtr & child)
        {
            const auto * identifier = child->as<ASTIdentifier>();
            return identifier && identifier->name() == column;
        };
        if (!partition_key.expression_list_ast || !std::ranges::any_of(partition_key.expression_list_ast->children, is_bare_column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Retention column '{}' must appear as a direct (unmodified) column in the PARTITION BY expression of table {}",
                column, name);

        table.retention_idx_in_minmax.push_back(find_idx_in_minmax(column, retention.types[i]));
    }

    return table;
}

std::vector<StorageABR::SourceTable> StorageABR::getSourceTables(
    std::span<const UInt64> intervals, const Retention & retention, const ContextPtr & context) const
{
    std::vector<SourceTable> tables;
    for (const UInt64 interval : intervals)
        tables.push_back(resolveSourceTable(interval, retention, context));
    return tables;
}

std::vector<StoragePtr> StorageABR::tryGetSourceTables() const
{
    try
    {
        const auto context = Context::getGlobalContextInstance();
        std::vector<StoragePtr> tables;
        for (const UInt64 interval : sample_intervals)
            tables.push_back(getSourceTable(interval, context));
        return tables;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_TABLE)
            return {};
        throw;
    }
}

/// The methods below have no query context, so they resolve the source tables with the global context.
/// If one of the source tables does not exist anymore, the features are reported as unsupported and
/// the column sizes as unknown rather than throwing, so that `system.columns` stays readable: a query
/// on the table fails anyway with `UNKNOWN_TABLE`.
StorageABR::ColumnSizeByName StorageABR::getColumnSizes() const
{
    const auto source_tables = tryGetSourceTables();
    return source_tables.empty() ? ColumnSizeByName{} : source_tables.front()->getColumnSizes();
}

std::optional<StorageABR::ColumnSizeByName> StorageABR::tryGetColumnSizes(const ContextPtr & query_context) const
{
    if (!query_context
        || !query_context->getAccess()->isGranted(
            AccessType::SHOW_COLUMNS, getStorageID().database_name, getSourceTableName(table_prefix, sample_intervals.front())))
        return std::nullopt;
    return getColumnSizes();
}

bool StorageABR::supportsPrewhere() const
{
    const auto source_tables = tryGetSourceTables();
    return !source_tables.empty() && std::ranges::all_of(source_tables, [](const auto & t) { return t->supportsPrewhere(); });
}

bool StorageABR::canMoveConditionsToPrewhere() const
{
    const auto source_tables = tryGetSourceTables();
    return !source_tables.empty() && std::ranges::all_of(source_tables, [](const auto & t) { return t->canMoveConditionsToPrewhere(); });
}

bool StorageABR::supportsFinal() const
{
    const auto source_tables = tryGetSourceTables();
    return !source_tables.empty() && std::ranges::all_of(source_tables, [](const auto & t) { return t->supportsFinal(); });
}

bool StorageABR::supportsSubcolumns() const
{
    const auto source_tables = tryGetSourceTables();
    return !source_tables.empty() && std::ranges::all_of(source_tables, [](const auto & t) { return t->supportsSubcolumns(); });
}

std::shared_ptr<StorageABR> StorageABR::creatorFunction(const StorageFactory::Arguments & args)
{
    /// Loading the table at startup, restoring or replaying it runs without the settings of the creating user, and must not fail.
    const bool is_replayed = isReplayedTableDefinition(args.mode, args.query, args.getLocalContext());
    if (!is_replayed && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_abr])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Table engine ABR is experimental, set `allow_experimental_abr` setting to enable it");

    /// The columns are always taken from the source tables, which hold the same dataset at different sampling rates.
    if (!args.columns.empty() && !is_replayed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table engine ABR does not support explicit column definitions: its columns are taken from its source tables");

    const ContextPtr & context = args.getContext();
    ASTs & engine_args = args.engine_args;
    if (engine_args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage ABR requires exactly three arguments: table_prefix, sample_intervals and retention_expression");

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

    String table_prefix = checkAndGetLiteralArgument<String>(engine_args[0], "table_prefix");
    if (table_prefix.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "table_prefix must not be empty");

    Array interval_array = checkAndGetLiteralArgument<Array>(engine_args[1], "sample_intervals");
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
    const auto [new_end, old_end] = std::ranges::unique(sample_intervals);
    sample_intervals.erase(new_end, old_end);

    if (sample_intervals.front() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "All sample intervals must be greater than 0");

    /// The source tables are checked below, revealing their columns and keys, so the user must be allowed to see them.
    if (!(isLoadingFromExistingMetadata(args.mode) || args.query.attach_short_syntax))
    {
        for (const auto & interval : sample_intervals)
            args.getLocalContext()->checkAccess(AccessType::SHOW_COLUMNS,
                args.table_id.database_name, getSourceTableName(table_prefix, interval));
    }

    Tuple retention_tuple;
    if (const auto * lit = engine_args[2]->as<ASTLiteral>(); lit && lit->value.getType() == Field::Types::String)
        retention_tuple.push_back(lit->value);
    else
        retention_tuple = checkAndGetLiteralArgument<Tuple>(engine_args[2], "retention_expression");
    if (retention_tuple.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "retention_expression must not be empty");

    Names retention_expression;
    for (const Field & column : retention_tuple)
    {
        if (column.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "retention_expression tuple's elements must be of type String");

        retention_expression.push_back(column.safeGet<String>());
    }

    auto storage = std::make_shared<StorageABR>(args.table_id, table_prefix, sample_intervals, retention_expression);

    /// A definition that was already accepted must stay loadable. The source tables are checked on every read anyway.
    if (!is_replayed)
        storage->checkSourceTables(context);

    return storage;
}

std::optional<StorageABR::UnixTimePoint> StorageABR::getMinMergeTreeDate(
    const SourceTable & table,
    const Retention & retention,
    const KeyCondition & retention_condition,
    const ContextPtr & context) const
{
    const auto metadata = table.storage->getInMemoryMetadataPtr(context, false);
    const StorageSnapshotPtr snapshot = table.storage->getStorageSnapshot(metadata, context);
    const auto & parts = assert_cast<const MergeTreeData::SnapshotData &>(*snapshot->data).parts;

    std::optional<UnixTimePoint> min_date;
    size_t total_parts = 0;
    size_t pruned_parts = 0;
    size_t skipped_parts = 0;
    Hyperrectangle sub_rect;

    for (const auto & part : *parts)
    {
        ++total_parts;

        const auto minmax_idx = part.data_part->getMinMaxIndex();
        if (!minmax_idx || !minmax_idx->initialized)
        {
            ++skipped_parts;
            continue;
        }

        const auto & hyperrectangle = minmax_idx->hyperrectangle;
        sub_rect.clear();
        for (const size_t idx : table.retention_idx_in_minmax)
            sub_rect.push_back(hyperrectangle[idx]);

        if (!retention_condition.checkInHyperrectangle(sub_rect, retention.types).can_be_true)
        {
            ++pruned_parts;
            continue;
        }

        const auto & min_val = hyperrectangle[table.time_idx_in_minmax].left;
        const UnixTimePoint date = timeValueToUnixTimePoint(min_val, retention.time_type).value_or(UnixTimePoint{});
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

std::optional<StorageABR::UnixTimePoint> StorageABR::getQueryLowerBound(const KeyCondition & time_cond, const DataTypePtr & time_type) const
{
    /// Conjuncts that cannot be analyzed are ignored, so the ranges are a conservative superset of the condition.
    const Ranges ranges = time_cond.extractBounds();

    const auto column_type = removeLowCardinality(time_type);
    const WhichDataType which_time_type(column_type);
    std::optional<UnixTimePoint> min_left;
    for (const auto & range : ranges)
    {
        if (range.left.isNull() || range.left.isNegativeInfinity())
            return std::nullopt; /// unbounded range -> no finite lower bound

        Field lower_bound = range.left;
        if (which_time_type.isDateTime64())
        {
            /// The bound is either a decimal with the scale of the literal, or whole seconds for a `DateTime` or integer
            /// constant, so its scale may differ from the scale of the column. Bring it to the scale of the column,
            /// rounding down, which keeps it a valid lower bound.
            auto [ticks, scale] = boundToTicksAndScale(lower_bound, time_column);
            const UInt32 column_scale = getDecimalScale(*column_type);
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

        const auto left = timeValueToUnixTimePoint(lower_bound, time_type);
        if (!left)
            throw Exception(
                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "ABR does not support query bounds before 1970-01-01");

        min_left = std::min(min_left.value_or(*left), *left);
    }
    return min_left;
}

std::optional<StorageABR::UnixTimePoint> StorageABR::timeValueToUnixTimePoint(const Field & value, const DataTypePtr & time_type) const
{
    const WhichDataType which_time_type(removeLowCardinality(time_type));

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

size_t StorageABR::chooseBestTable(
    SelectQueryInfo & query_info, ContextPtr context, const std::vector<SourceTable> & tables, const Retention & retention) const
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
        abr_info->exception_codes.try_emplace(tables[idx].interval, ErrorCodes::NO_AVAILABLE_DATA);
    };

    auto filter_dag = buildFilterActionsDAG(query_info, context);
    if (!filter_dag.has_value())
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED,
            "Query must filter on the time column '{}'. Add a condition that uses it on the WHERE clause",
            time_column);

    const ActionsDAGWithInversionPushDown filter_dag_with_inversion(filter_dag->getOutputs().front(), context, /* boolean_context */ true);

    KeyCondition time_cond(filter_dag_with_inversion, context, {time_column}, retention.key_expr);
    std::optional<UnixTimePoint> min_query_date = getQueryLowerBound(time_cond, retention.time_type);
    if (!min_query_date.has_value())
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED,
            "Query must have a lower bound on the time column '{}'. Add a condition that uses it on the WHERE clause",
            time_column);

    LOG_DEBUG(logger, "Query lower bound: {}", DateLUT::instance().timeToString(min_query_date->seconds));
    if (abr_info)
        abr_info->min_date.store(min_query_date->seconds);

    KeyCondition retention_condition(filter_dag_with_inversion, context, retention.names, retention.key_expr);
    LOG_DEBUG(logger, "Retention condition: {}", retention_condition.toString());

    std::vector<std::optional<UnixTimePoint>> min_storage_dates(tables.size());
    for (size_t i = 0; i < tables.size(); ++i)
    {
        const String name = tables[i].storage->getStorageID().getNameForLogs();
        const auto min_storage_date = getMinMergeTreeDate(tables[i], retention, retention_condition, context);

        if (!min_storage_date.has_value())
        {
            LOG_DEBUG(logger, "Table {} skipped: all parts pruned by retention condition", name);
            record_retention_skip(i);
            continue;
        }

        LOG_DEBUG(logger, "Table {} min_date={}", name, DateLUT::instance().timeToString(min_storage_date->seconds));
        min_storage_dates[i] = min_storage_date.value();

        if (min_query_date.value() >= min_storage_date.value())
        {
            LOG_DEBUG(logger, "Selected table {}", name);
            return i;
        }

        LOG_DEBUG(logger, "Table {} skipped: min_date > query lower bound", name);
        record_retention_skip(i);
    }

    const size_t fallback_idx = pickFallbackTable(min_storage_dates);
    const auto & fallback_min_date = min_storage_dates[fallback_idx];
    const String fallback_name = tables[fallback_idx].storage->getStorageID().getNameForLogs();
    /// Retract the NO_AVAILABLE_DATA tag if the fallback has data;
    /// keep it when every table was pruned
    if (abr_info && fallback_min_date.has_value())
    {
        std::lock_guard lock(abr_info->mutex);
        abr_info->exception_codes.erase(tables[fallback_idx].interval);
    }

    if (fallback_min_date.has_value())
        LOG_DEBUG(logger, "No table covers query range, falling back to {} (oldest timestamp at day precision: {})",
            fallback_name, DateLUT::instance().dateToString(fallback_min_date->seconds));
    else
        LOG_DEBUG(logger, "No table covers query range, falling back to {}", fallback_name);

    return fallback_idx;
}

void StorageABR::checkTableCanBeRenamed(const StorageID & new_name) const
{
    const auto table_id = getStorageID();
    if (new_name.database_name != table_id.database_name)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot move table {} with the `ABR` engine to another database: its source tables are found in its own database. "
            "Create a new `ABR` table in database {} instead",
            table_id.getNameForLogs(), backQuoteIfNeed(new_name.database_name));
}

void StorageABR::rename(const String & /* new_path_to_table_data */, const StorageID & new_table_id)
{
    /// A database that is not `Atomic` does not call `checkTableCanBeRenamed`, but renames the table before anything is committed.
    checkTableCanBeRenamed(new_table_id);
    renameInMemory(new_table_id);
}

size_t StorageABR::pickFallbackTable(const std::vector<std::optional<UnixTimePoint>> & min_storage_dates) const
{
    /// Pick the table with the oldest min_storage_date at day precision (the "best coverage" group)
    /// Among ties, pick the smallest index, i.e. the finest sample interval (highest data quality)
    const auto & date_lut = DateLUT::instance();
    std::optional<Int32> min_day_num;
    size_t selected = 0;

    for (size_t i = 0; i < min_storage_dates.size(); ++i)
    {
        if (!min_storage_dates[i].has_value())
            continue;

        const Int32 day_num = static_cast<Int32>(date_lut.toDayNum(static_cast<time_t>(min_storage_dates[i]->seconds)));
        if (!min_day_num.has_value() || day_num < min_day_num.value())
        {
            min_day_num = day_num;
            selected = i;
        }
    }

    return selected;
}

void StorageABR::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    /// `ABR` executes the query on a source table, where a row policy of this table cannot be applied. Neither can the
    /// policy of a table that reads this one, such as an `Alias`, which the planner passes in `row_level_filter`.
    if (query_info.row_level_filter || getEffectiveRowPolicyFilter(*this, context))
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Row policies on table {}, or on a table that reads from it, are not supported: the `ABR` engine "
            "executes the query on a source table, where they cannot be applied. Define them on the source tables instead",
            getStorageID().getNameForLogs());

    /// Neither can the filter of `additional_table_filters` for this table, or for a table that reads it, which the planner
    /// passes in `additional_filter_ast` and would only apply to the columns read from this table, not to the result of `ABR`.
    if (query_info.additional_filter_ast)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "`additional_table_filters` for table {}, or for a table that reads from it, are not supported: the `ABR` engine "
            "executes the query on a source table, where they cannot be applied. Add the condition to the query instead",
            getStorageID().getNameForLogs());

    /// `ABR` runs the whole query on a source table. As the left table of a JOIN, it runs the whole query, which the planner expects.
    /// On the right side of a JOIN, the planner only expects the columns of this table, but `ABR` would still run the whole JOIN
    /// query and return the wrong columns.
    if (query_info.query_tree && query_info.table_expression
        && extractLeftTableExpression(query_info.query_tree->as<const QueryNode &>().getJoinTreeNodeTyped()).get()
            != query_info.table_expression.get())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Table {} with the `ABR` engine can only be read as the leftmost table of a JOIN. Read it through a subquery "
            "with a condition on its time column instead: `JOIN (SELECT ... FROM {} WHERE {} >= ...)`",
            getStorageID().getNameForLogs(), getStorageID().getFullTableName(), time_column);

    /// Neither the choice of the table nor the retries go outside the range of `abr_first_table_suffix` and `abr_last_table_suffix`.
    const std::span<const UInt64> candidate_intervals = getCandidateIntervals(context);

    /// The table is chosen by inspecting the data of every candidate source table, and the query may be retried on any of them,
    /// so the reader must be allowed to read all of them, before anything about them is resolved or revealed.
    /// A subcolumn is checked as its column, unless the source has a grant or revoke on the subcolumn itself. The source
    /// tables have the columns this table was analyzed with, so they are resolved from those.
    Names checked_columns = column_names;
    checked_columns.insert(checked_columns.end(), std::next(retention_expression.begin()), retention_expression.end());
    const String database = getStorageID().database_name;
    const auto & columns = storage_snapshot->metadata->getColumns();
    for (const UInt64 interval : candidate_intervals)
    {
        const StorageID source_id(database, getSourceTableName(table_prefix, interval));
        context->checkAccess(
            AccessType::SELECT,
            source_id.database_name,
            source_id.table_name,
            columns.getColumnNamesForSelectAccessCheck(checked_columns, context, source_id));
    }

    /// The retention columns are taken from the columns the query was analyzed with.
    const Retention retention = getRetention(storage_snapshot->metadata->getColumns(), context);

    /// The source tables are resolved once, so that the table is chosen and read from the same tables,
    /// and they are kept alive only for the duration of the query.
    std::vector<SourceTable> source_tables = getSourceTables(candidate_intervals, retention, context);

    Stopwatch choose_watch;
    auto abr_info = context->getABRQueryInfoPtr();
    const size_t chosen_table_idx = chooseBestTable(query_info, context, source_tables, retention);
    if (abr_info)
        abr_info->duration_ms.fetch_add(choose_watch.elapsedMilliseconds());

    /// The retries go from the chosen table to the coarsest candidate, and the limits are split between them.
    source_tables.erase(source_tables.begin(), source_tables.begin() + chosen_table_idx);
    auto initial_attempt = buildAttempt(
        query_info, source_tables.front().storage, context, source_tables.size(), source_tables.size(), processed_stage);
    LOG_DEBUG(logger, "Starting read from table {} ({} candidates for retry)",
        source_tables.front().storage->getStorageID().getNameForLogs(), source_tables.size());

    auto step = std::make_unique<ReadFromABR>(
        query_info, std::move(source_tables), std::move(initial_attempt), context, processed_stage, logger.load());

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
that store the same data at different sampling granularities. It picks the finest-grained source table whose retention covers
the query's time range, and on `TOO_MANY_ROWS` or `TIMEOUT_EXCEEDED` it transparently retries the query on the next coarser table.

<Info>
This is an experimental feature that may change in backwards-incompatible ways in future releases. Enable the creation of `ABR`
tables with the setting `allow_experimental_abr`: `SET allow_experimental_abr = 1`. It is only checked when a table is created:
`ABR` tables are loaded and can be queried regardless of this setting.
</Info>

The source tables are expected to follow a naming convention of `{table_prefix}_{sample_interval}`, where `sample_interval`
distinguishes the granularity. The columns of an `ABR` table are taken from its source tables, so they cannot be specified
in the `CREATE` query. The engine never writes; data must be ingested directly into the source tables. Creating
an `ABR` table requires `SHOW COLUMNS` on all its source tables, and reading from it requires `SELECT` on all
the source tables the query may be executed on, from the one of `abr_first_table_suffix` to the one of `abr_last_table_suffix`.

The source tables are registered as dependencies of the `ABR` table: they are loaded and restored from a backup before it, and
dropping or renaming one of them while the `ABR` table exists is refused, unless `check_table_dependencies` is disabled.

The source tables are resolved by name on every query, and the columns of the `ABR` table are those of its finest source table that exists.
A source table that is dropped or detached makes the queries on the `ABR` table fail with `UNKNOWN_TABLE`, and a source table
created again under the same name is used from the next query on.

**Engine parameters**

- `table_prefix` — A `String` literal giving the common prefix of the source tables. For each interval in `sample_intervals`,
  `ABR` discovers the table `{table_prefix}_{interval}`. All discovered tables must exist in the same database as the `ABR` table,
  be from the `MergeTree` family, and share an identical column set (names and types).
- `sample_intervals` — A non-empty `Array(UInt64)` of granularity levels. Values are sorted ascending and deduplicated; `0` is not
  allowed. The smallest interval is the finest-grained table and is preferred whenever it has enough retention; the largest
  interval is the coarsest fallback.
- `retention_expression` — A `Tuple(String, ...)` (or a single `String`) of partition key column names that `ABR` uses for
  pruning. The first element must be the time column, of type `Date`, `Date32`, `DateTime` or `DateTime64` (optionally wrapped
  into `LowCardinality`, but not `Nullable`). All other columns must appear as direct, unmodified columns in the `PARTITION BY`
  expression of the source tables. The time column is exempt, as it typically appears inside a function such as
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
- The estimate of the rows an attempt plans to read is only checked against the share of that attempt. Outside the attempt,
  including on the initiator of a distributed query, only the rows actually read count against the limits.

**Limitations**

- `ABR` is read-only. `INSERT`, `ALTER` and mutations must target the source tables directly.
- All source tables must share an identical column set.
- `ABR` does not support row policies on the `ABR` table, or on a table that reads from it such as an `Alias`. Define them on
  the source tables instead.
- `ABR` does not support `additional_table_filters` for the `ABR` table, or for a table that reads from it. Add the condition
  to the query instead.
- An `ABR` table cannot be moved to another database with `RENAME TABLE` or `EXCHANGE TABLES`, because its source tables are
  found in its own database. Renaming it within its database is supported.
- A database that contains an `ABR` table can only be renamed with `RENAME DATABASE` when `check_table_dependencies` is
  disabled, as for any database with dependencies between its tables.
- An `ABR` table can only be the leftmost table of a `JOIN`. To join it on another side, read it through a subquery with a
  condition on its time column: `JOIN (SELECT ... FROM abr_table WHERE ts >= ...)`.
- An `ABR` table cannot be read from a serialized query plan, which does not carry the filters of the query. With
  `serialize_query_plan = 1`, a distributed query that reads `ABR` tables on remote servers is sent to them as SQL instead.
)DOCS_MD",
            .syntax = "ENGINE = ABR(table_prefix, sample_intervals, retention_expression)",
            .examples = {{
                .name = "Route a query to the finest table that covers its time range",
                .query = R"(
CREATE TABLE events_1 (ts DateTime, region String, value UInt64)
ENGINE = MergeTree PARTITION BY (toYYYYMM(ts), region) ORDER BY ts;
CREATE TABLE events_10 AS events_1;
CREATE TABLE events_100 AS events_1;

SET allow_experimental_abr = 1;
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
