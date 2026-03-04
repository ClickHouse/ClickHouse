#include <Processors/Transforms/FilterTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PredicateAtomExtractor.h>
#include <Interpreters/PredicateStatisticsLog.h>
#include <Storages/IStorage.h>
#include <Processors/Chunk.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Interpreters/ActionsDAG.h>
#include <Functions/IFunction.h>

namespace ProfileEvents
{
    extern const Event FilterTransformPassedRows;
    extern const Event FilterTransformPassedBytes;
}

namespace DB
{

struct PredicateAtomsHolder
{
    std::vector<PredicateAtom> atoms;
};

namespace ServerSetting
{
    extern const ServerSettingsUInt64 predicate_statistics_sample_rate;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

bool FilterTransform::canUseType(const DataTypePtr & filter_type)
{
    return filter_type->canBeUsedInBooleanContext();
}

auto incrementProfileEvents = [](size_t num_rows, const Columns & columns)
{
    ProfileEvents::increment(ProfileEvents::FilterTransformPassedRows, num_rows);

    size_t num_bytes = 0;
    for (const auto & column : columns)
    {
        if (column)
            num_bytes += column->byteSize();
    }
    ProfileEvents::increment(ProfileEvents::FilterTransformPassedBytes, num_bytes);
};

Block FilterTransform::transformHeader(
    const Block & header, const ActionsDAG * expression, const String & filter_column_name, bool remove_filter_column)
{
    Block result = expression ? expression->updateHeader(header) : header;

    auto filter_type = result.getByName(filter_column_name).type;
    if (!canUseType(filter_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column {} for filter. Must be UInt8 or Nullable(UInt8).",
            filter_type->getName(), filter_column_name);

    if (remove_filter_column)
        result.erase(filter_column_name);

    return result;
}

FilterTransform::FilterTransform(
    SharedHeader header_,
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_,
    std::optional<std::pair<UInt64, String>> condition_)
    : ISimpleTransform(
            header_,
            std::make_shared<const Block>(transformHeader(*header_, expression_ ? &expression_->getActionsDAG() : nullptr, filter_column_name_, remove_filter_column_)),
            true)
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
    , rows_filtered(rows_filtered_)
    , condition(condition_)
{
    transformed_header = getInputPort().getHeader();
    if (expression)
    {
        expression->execute(transformed_header);

        /// Special check to stop queries like "WHERE ignore(...)"
        {
            const auto * node = &expression->getActionsDAG().findInOutputs(filter_column_name);
            while (node->type == ActionsDAG::ActionType::ALIAS)
                node = node->children[0];

            if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "ignore")
                always_false = true;
        }
    }
    filter_column_position = transformed_header.getPositionByName(filter_column_name);

    auto & column = transformed_header.getByPosition(filter_column_position).column;
    if (column)
        always_false = always_false || ConstantFilterDescription(*column).always_false;

    if (condition.has_value())
        query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();

    /// predicate statistics collection
    if (auto global_context = Context::getGlobalContextInstance())
    {
        predicate_stats_sample_rate = global_context->getServerSettings()[ServerSetting::predicate_statistics_sample_rate];
        if (predicate_stats_sample_rate > 0)
        {
            predicate_stats_log = global_context->getPredicateStatisticsLog();
            if (predicate_stats_log && expression)
            {
                const auto * node = &expression->getActionsDAG().findInOutputs(filter_column_name);
                while (node->type == ActionsDAG::ActionType::ALIAS)
                    node = node->children[0];
                auto atoms = extractPredicateAtoms(node);
                if (!atoms.empty())
                {
                    predicate_atoms = std::make_unique<PredicateAtomsHolder>(PredicateAtomsHolder{std::move(atoms)});
                    collect_predicate_stats = true;
                }
            }
        }
    }
}

IProcessor::Status FilterTransform::prepare()
{
    if (!on_totals
        && (always_false
            /// Optimization for `WHERE column in (empty set)`.
            /// The result will not change after set was created, so we can skip this check.
            /// It is implemented in prepare() stop pipeline before reading from input port.
            || (!are_prepared_sets_initialized && expression && expression->checkColumnIsAlwaysFalse(filter_column_name))))
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    auto status = ISimpleTransform::prepare();

    /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
    if (status != IProcessor::Status::PortFull)
        are_prepared_sets_initialized = true;

    if (status == IProcessor::Status::Finished)
        writeIntoQueryConditionCache({});

    return status;
}


void FilterTransform::removeFilterIfNeed(Columns & columns) const
{
    if (remove_filter_column)
        columns.erase(columns.begin() + filter_column_position);
}

void FilterTransform::transform(Chunk & chunk)
{
    auto chunk_rows_before = chunk.getNumRows();
    bool actually_filtered = !on_totals && !isVirtualRow(chunk);
    doTransform(chunk);
    auto chunk_rows_after = chunk.getNumRows();
    if (rows_filtered)
        *rows_filtered += chunk_rows_before - chunk_rows_after;
    if (collect_predicate_stats && actually_filtered && chunk_rows_before > 0)
        collectPredicateStatistics(chunk_rows_before, chunk_rows_after, chunk);
}

void FilterTransform::doTransform(Chunk & chunk)
{
    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    DataTypes types;

    {
        Block block = getInputPort().getHeader().cloneWithColumns(columns);
        columns.clear();

        if (expression)
            expression->execute(block, num_rows_before_filtration);

        columns = block.getColumns();
        types = block.getDataTypes();
    }

    size_t num_columns = columns.size();
    ColumnPtr filter_column = columns[filter_column_position];
    ConstantFilterDescription constant_filter_description(*filter_column);

    if (constant_filter_description.always_true || on_totals || isVirtualRow(chunk))
    {
        incrementProfileEvents(num_rows_before_filtration, columns);
        removeFilterIfNeed(columns);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        return;
    }

    if (constant_filter_description.always_false)
    {
        writeIntoQueryConditionCache(chunk.getChunkInfos().get<MarkRangesInfo>());
        incrementProfileEvents(0, {});
        return;
    }

    std::unique_ptr<IFilterDescription> filter_description;

    if (isColumnConst(*filter_column))
        filter_column = filter_column->convertToFullColumnIfConst();

    if (filter_column->isSparse())
        filter_description = std::make_unique<SparseFilterDescription>(*filter_column);
    else
        filter_description = std::make_unique<FilterDescription>(*filter_column);

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = num_columns;
    size_t min_size_in_memory = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < num_columns; ++i)
    {
        DataTypePtr type_not_null = removeNullableOrLowCardinalityNullable(types[i]);
        if (i != filter_column_position && !isColumnConst(*columns[i]) && type_not_null->isValueRepresentedByNumber())
        {
            size_t size_in_memory = type_not_null->getSizeOfValueInMemory() + (isNullableOrLowCardinalityNullable(types[i]) ? 1 : 0);
            if (size_in_memory < min_size_in_memory)
            {
                min_size_in_memory = size_in_memory;
                first_non_constant_column = i;
            }
        }
    }
    (void)min_size_in_memory; /// Suppress error of clang-analyzer-deadcode.DeadStores

    size_t num_filtered_rows = 0;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = filter_description->filter(*columns[first_non_constant_column], -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = filter_description->countBytesInFilter();

    incrementProfileEvents(num_filtered_rows, columns);

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
    {
        writeIntoQueryConditionCache(chunk.getChunkInfos().get<MarkRangesInfo>());
        /// SimpleTransform will skip it.
        return;
    }

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        /// No need to touch the rest of the columns.
        removeFilterIfNeed(columns);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        return;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & current_column = columns[i];

        if (i == filter_column_position && remove_filter_column)
            continue;

        if (i == first_non_constant_column)
            continue;

        if (isColumnConst(*current_column))
            current_column = current_column->cut(0, num_filtered_rows);
        else
            current_column = filter_description->filter(*current_column, num_filtered_rows);
    }

    removeFilterIfNeed(columns);
    chunk.setColumns(std::move(columns), num_filtered_rows);
}

void FilterTransform::writeIntoQueryConditionCache(const MarkRangesInfoPtr & mark_ranges_info)
{
    if (!query_condition_cache)
        return;

    if (!mark_ranges_info)
    {
        /// FilterTransform has finished, we need to flush to the query result cache.

        if (!buffered_mark_ranges_info)
            return;

        query_condition_cache->write(
            buffered_mark_ranges_info->table_uuid,
            buffered_mark_ranges_info->part_name,
            condition->first,
            condition->second,
            buffered_mark_ranges_info->mark_ranges,
            buffered_mark_ranges_info->marks_count,
            buffered_mark_ranges_info->has_final_mark);

        buffered_mark_ranges_info = nullptr;

        return;
    }

    if (!buffered_mark_ranges_info)
    {
        buffered_mark_ranges_info = std::static_pointer_cast<MarkRangesInfo>(mark_ranges_info->clone());
    }
    else
    {
        /// If the current and the buffer mark range info are from the same table/part, append to the buffer.
        /// Otherwise write to the query condition cache and reset the buffer.

        if (buffered_mark_ranges_info->table_uuid != mark_ranges_info->table_uuid || buffered_mark_ranges_info->part_name != mark_ranges_info->part_name)
        {
            query_condition_cache->write(
                buffered_mark_ranges_info->table_uuid,
                buffered_mark_ranges_info->part_name,
                condition->first,
                condition->second,
                buffered_mark_ranges_info->mark_ranges,
                buffered_mark_ranges_info->marks_count,
                buffered_mark_ranges_info->has_final_mark);

            buffered_mark_ranges_info = std::static_pointer_cast<MarkRangesInfo>(mark_ranges_info->clone());
        }
        else
        {
            buffered_mark_ranges_info->appendMarkRanges(mark_ranges_info->mark_ranges);
        }
    }
}

void FilterTransform::collectPredicateStatistics(size_t num_rows_before_filtration, size_t num_rows_after_filtration, const Chunk & chunk)
{
    ++chunk_counter;
    if (chunk_counter % predicate_stats_sample_rate != 0)
        return;

    time_t now = time(nullptr);
    UInt16 today = static_cast<UInt16>(DateLUT::instance().toDayNum(now));
    Float64 selectivity = static_cast<Float64>(num_rows_after_filtration) / static_cast<Float64>(num_rows_before_filtration);
    String query_id(CurrentThread::getQueryId());

    /// resolve database/table from MarkRangesInfo, caching to avoid repeated DatabaseCatalog lookups
    if (auto mark_ranges_info = chunk.getChunkInfos().get<MarkRangesInfo>())
    {
        if (mark_ranges_info->table_uuid != cached_table_uuid)
        {
            cached_table_uuid = mark_ranges_info->table_uuid;
            cached_database.clear();
            cached_table.clear();
            auto db_and_table = DatabaseCatalog::instance().tryGetByUUID(cached_table_uuid);
            if (db_and_table.first && db_and_table.second)
            {
                auto storage_id = db_and_table.second->getStorageID();
                cached_database = storage_id.database_name;
                cached_table = storage_id.table_name;
            }
        }
    }

    /// for conjunctive filters (multiple atoms), the selectivity recorded here is the
    /// combined pass rate, which is a lower bound on each individual atom's selectivity.
    /// this logic aligns well with physical design advisor logic, no need to leave only atoms
    for (const auto & atom : predicate_atoms->atoms)
    {
        PredicateStatisticsLogElement elem;
        elem.event_date = today;
        elem.event_time = now;
        elem.database = cached_database;
        elem.table = cached_table;
        elem.column_name = atom.column_name;
        elem.predicate_class = atom.predicate_class;
        elem.function_name = atom.function_name;
        elem.input_rows = num_rows_before_filtration;
        elem.passed_rows = num_rows_after_filtration;
        elem.selectivity = selectivity;
        elem.query_id = query_id;
        predicate_stats_log->add(std::move(elem));
    }
}

FilterTransform::~FilterTransform() = default;

}
