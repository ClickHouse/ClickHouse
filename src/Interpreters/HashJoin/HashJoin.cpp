#include <Interpreters/HashJoin/HashJoin.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Common/FailPoint.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/HashJoin/SharedFixedHashTableFilter.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnIndex.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/RowRefs.h>
#include <base/getL2CacheSize.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/StackTrace.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace CurrentMetrics
{
extern const Metric HashJoinDestroyThreads;
extern const Metric HashJoinDestroyThreadsActive;
extern const Metric HashJoinDestroyThreadsScheduled;
}

namespace ProfileEvents
{
extern const Event HashJoinBuildMicroseconds;
extern const Event HashJoinPartitionedBuildMicroseconds;
extern const Event HashJoinPartitionedBuildFillMicroseconds;
extern const Event HashJoinPartitionedBuildInsertMicroseconds;
extern const Event HashJoinPartitionedProbeMicroseconds;
extern const Event HashJoinPartitions;
extern const Event HashJoinTableBytes;
extern const Event HashJoinPartitionOverflowRows;
extern const Event HashJoinDuplicateRunBytes;
extern const Event HashJoinTeardownMicroseconds;
extern const Event HashJoinRowStoreBlocks;
extern const Event HashJoinPreallocatedElementsInHashTables;
}

namespace DB
{

namespace ErrorCodes
{
extern const int FAULT_INJECTED;
extern const int INCOMPATIBLE_TYPE_OF_JOIN;
extern const int INVALID_JOIN_ON_EXPRESSION;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int NO_SUCH_COLUMN_IN_TABLE;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
}

namespace FailPoints
{
extern const char hash_join_throw_after_data_release[];
}

size_t getMinBytesForPrefetchInJoin()
{
    /// Enable prefetch once the hash table no longer fits in L2; below that it
    /// is cache resident and prefetching is pure overhead. Cached after first call.
    static const size_t result = getL2CacheSize();
    return result;
}

namespace
{

/// `joinBlock` only sets up a lazy result - the matching runs inside `IJoinResult::next` - so the
/// probe time has to be accounted there.
class TimedJoinResult : public IJoinResult
{
public:
    TimedJoinResult(JoinResultPtr result_, ProfileEvents::Event event_)
        : result(std::move(result_))
        , event(event_)
    {
    }

    JoinResultBlock next() override
    {
        ProfileEventTimeIncrement<Microseconds> watch(event);
        return result->next();
    }

    /// The transform sums this into the count `onProbePhaseFinish` receives. Without it the join would
    /// publish zero matches. The planner would then never enable the row store for the next run.
    std::optional<size_t> getMatchedRightRows() const override { return result->getMatchedRightRows(); }

private:
    JoinResultPtr result;
    ProfileEvents::Event event;
};

Block filterColumnsPresentInSampleBlock(const Block & block, const Block & sample_block)
{
    Block filtered_block;
    for (const auto & sample_column : sample_block.getColumnsWithTypeAndName())
        filtered_block.insert(block.getByName(sample_column.name));
    return filtered_block;
}

std::pair<Columns, Columns> extractRowStoreColumns(const Block & block, const ColumnAccessIndexes & access_indexes)
{
    Columns row_store_columns;
    Columns remaining_columns;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i);
        if (access_indexes[i].type == ColumnAccessIndex::Type::RowStore)
            row_store_columns.push_back(column.column);
        else
            remaining_columns.push_back(column.column);
    }

    return {row_store_columns, remaining_columns};
}

}

static HashJoin::Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);
static std::optional<HashJoin::Type> tryGetLowCardinalityMethod(const ColumnPtr & column);

/// A multi-disjunct (OR) join shares a single data->type across all disjuncts. When the disjuncts
/// pick different packed fixed-key maps (e.g. keys32 for a (UInt16, UInt16) clause and keys64 for a
/// (UInt32, UInt32) clause), use the widest packed map that can hold all of them instead of
/// downgrading the whole join to the generic `hashed` map: a narrower packing always fits into a
/// wider fixed-key map. Only genuinely different key kinds fall back to `hashed`.
static HashJoin::Type mergeJoinMethods(HashJoin::Type lhs, HashJoin::Type rhs)
{
    using Type = HashJoin::Type;

    auto packed_rank = [](Type type) -> int
    {
        switch (type)
        {
            case Type::keys32: return 1;
            case Type::keys64: return 2;
            case Type::keys128: return 3;
            case Type::keys256: return 4;
            default: return 0;
        }
    };

    const int lhs_rank = packed_rank(lhs);
    const int rhs_rank = packed_rank(rhs);
    if (lhs_rank != 0 && rhs_rank != 0)
        return lhs_rank >= rhs_rank ? lhs : rhs;

    return Type::hashed;
}

/// The right columns a join with several disjuncts adds to the result. Every right key is read to build
/// the maps, but only the keys the query asks for belong in the result; `requiredRightKeys` says which
/// those are - the planner fills it in `setUsedColumns`, `TreeRewriter` in `addJoinedColumn`.
static Block rightColumnsToAddWithSeveralDisjuncts(const TableJoin & table_join, const Block & right_columns)
{
    NameSet key_names;
    for (const auto & clause : table_join.getClauses())
        key_names.insert(clause.key_names_right.begin(), clause.key_names_right.end());

    const NameSet required_keys = table_join.requiredRightKeys();

    Block columns_to_add;
    for (const auto & column : right_columns)
    {
        if (!key_names.contains(column.name) || required_keys.contains(column.name))
            columns_to_add.insert(column);
    }
    return columns_to_add;
}

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    size_t num_threads_,
    bool any_take_last_row_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    size_t max_bytes_before_external_join_,
    std::optional<size_t> build_rows_hint_)
    : HashJoin(
          std::move(table_join_),
          std::move(right_sample_block_),
          num_threads_,
          any_take_last_row_,
          stats_collecting_params_,
          max_bytes_before_external_join_,
          build_rows_hint_,
          /*join_table_mode_=*/false)
{
}

HashJoin::HashJoin(
    JoinTableTag, std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_)
    : HashJoin(
          std::move(table_join_),
          std::move(right_sample_block_),
          /*num_threads_=*/1,
          any_take_last_row_,
          HashJoinStatsCollectingParams{},
          /*max_bytes_before_external_join_=*/0,
          /*build_rows_hint_=*/std::nullopt,
          /*join_table_mode_=*/true)
{
}

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    size_t num_threads_,
    bool any_take_last_row_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    size_t max_bytes_before_external_join_,
    std::optional<size_t> build_rows_hint_,
    bool join_table_mode_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , right_sample_block(*right_sample_block_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , joined_block_split_single_row(table_join->joinedBlockAllowSplitSingleRow())
    , enable_lazy_columns_replication(table_join->enableColumnsLazyReplication())
    , enable_prefetch(table_join->enableSoftwarePrefetchInJoin())
    /// `HashJoinTable` has no key-only counterpart.
    , allow_set_maps(false)
    , log(getLogger("HashJoin"))
    , right_input_header(std::move(right_sample_block_))
    , any_take_last_row(any_take_last_row_)
    , num_threads(std::max<size_t>(1, num_threads_))
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , join_table_mode(join_table_mode_)
    , used_flags_per_row(needUsedFlagsForPerRightTableRow(table_join))
    , cached_distinct_estimates(table_join->getClauses().size())
    , build_rows_hint(build_rows_hint_)
    , single_fill_thread(
          !join_table_mode && (num_threads == 1 || (build_rows_hint_ && *build_rows_hint_ < table_join->parallelHashJoinThreshold())))
    , stats_collecting_params(stats_collecting_params_.build)
    , match_stats_collecting_params(stats_collecting_params_.match)
{
    if (isCrossOrComma(kind))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot execute {}", kind);

    if (table_join->getClauses().empty() || table_join->isJoinWithConstant())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot execute JOIN without keys or with constant keys");

    if (joined_block_split_single_row && max_joined_block_rows == 0)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Setting `joined_block_split_single_row` is set to true, but `max_joined_block_rows` is 0 (no limit). "
            "Set max_joined_block_rows > 0 or use `max_joined_block_bytes` with default `max_joined_block_rows` (by default equals to block size).");
    }

    for (auto & column : right_sample_block)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    validateAdditionalFilterExpression(table_join->getMixedJoinExpression());

    used_flags = std::make_unique<JoinStuff::JoinUsedFlags>();
    /// Published before the build: a stored block whose flags are not there yet then reads as not used.
    if (needUsedFlagsForPerRightTableRow(table_join))
        used_flags->need_flags = true;

    if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(key_names_right, right_sample_block, right_table_keys, sample_block_with_columns_to_add);
        required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);
    }
    else
    {
        /// With several disjuncts a right key can differ from the left key it matched - the match may
        /// have come from another clause - so it cannot be restored from the left column the way
        /// `required_right_keys` does it, and a key the query asks for stays a column the join adds.
        /// The keys nobody asks for are needed to build the maps and for nothing else.
        right_table_keys = materializeBlock(right_sample_block);
        sample_block_with_columns_to_add = rightColumnsToAddWithSeveralDisjuncts(*table_join, right_table_keys);
    }

    /// Detect a single non-nullable LowCardinality key before the keys are materialized below, so it
    /// can use a dictionary-aware map. Restricted to one disjunct and non-ASOF for now.
    std::optional<Type> low_cardinality_method;
    if (table_join->oneDisjunct() && strictness != JoinStrictness::Asof)
    {
        const auto & only_clause_key_names = table_join->getOnlyClause().key_names_right;
        if (only_clause_key_names.size() == 1)
            low_cardinality_method = tryGetLowCardinalityMethod(right_table_keys.getByName(only_clause_key_names[0]).column);
    }

    materializeBlockInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);
    data->sample_block = prepareRightBlock(data->sample_block);

    size_t disjuncts_num = table_join->getClauses().size();
    data->maps.resize(disjuncts_num);

    if (!table_join->isRowStoreEnabled() || !isRowStoreSupported() || data->sample_block.columns() == 0)
        data->row_store_state = RowStoreState::Disabled;
    else
        /// Publish the layout before any `FillingRightJoinSide` thread can call `addBlockToJoin`.
        initRowStore(data->sample_block);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

    key_sizes.reserve(disjuncts_num);

    std::optional<Type> selected_join_method;
    auto set_join_method = [&](Type current_join_method)
    {
        if (!selected_join_method)
            selected_join_method = current_join_method;
        else if (*selected_join_method != current_join_method)
            selected_join_method = mergeJoinMethods(*selected_join_method, current_join_method);
    };

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right);

        if (strictness == JoinStrictness::Asof)
        {
            chassert(disjuncts_num == 1);

            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported");

            if (key_columns.size() <= 1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join with hash algorithm needs at least one equi-join column");

            size_t asof_size = 0;
            asof_type = SortedLookupVectorBase::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            selected_join_method = chooseMethod(key_columns, asof_key_sizes);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto current_join_method = chooseMethod(key_columns, key_sizes.emplace_back());
            if (low_cardinality_method)
            {
                current_join_method = *low_cardinality_method;
                LOG_TRACE(log, "Using a dictionary-aware hash map for the single LowCardinality join key");
            }
            set_join_method(current_join_method);
        }
    }

    if (!selected_join_method)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot choose JOIN method without keys");

    data->type = *selected_join_method;

    LOG_TRACE(log, "Join hash table type: {}", data->type);
    LOG_TEST(
        log,
        "Keys: {}, datatype: {}, kind: {}, strictness: {}, right header: {}",
        TableJoin::formatClauses(table_join->getClauses(), true),
        data->type,
        kind,
        strictness,
        right_sample_block.dumpStructure());

    use_set_maps = canUseSetMaps();
    if (use_set_maps)
        LOG_TRACE(log, "Using key-only hash tables: the join never reads a right row");

    for (auto & maps : data->maps)
        dataMapInit(maps);

    recomputeMapsBytes();

    if (table_join->getMixedJoinExpression())
    {
        const auto & required_cols = table_join->getMixedJoinExpression()->getRequiredColumnsWithTypes();
        size_t pos = 0;
        for (const auto & input : required_cols)
        {
            if (data->sample_block.has(input.name))
            {
                /// `buildAdditionalFilter` creates the column for this input from `input.type` and fills
                /// it from the stored right blocks, so resolving the input by name alone is not enough:
                /// a same-named column of a different type would be read through a mismatched
                /// `IColumn` interface. Fail here instead, where both types are still known.
                const auto & stored = data->sample_block.getByName(input.name);
                if (!stored.type->equals(*input.type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} required by the mixed JOIN ON condition has type {}, "
                        "but the stored right column of that name has type {}",
                        input.name,
                        input.type->getName(),
                        stored.type->getName());

                additional_filter_required_rhs_pos.emplace_back(
                    pos,
                    data->sample_block.getPositionByName(input.name));
            }
            ++pos;
        }
    }

    if (!HashJoinTableMaps::isSupportedType(data->type))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "HashJoin was created for an unsupported map type {}; the plan-time gate must reject this shape",
            data->type);

    for (size_t clause_idx = 0; clause_idx < table_join->getClauses().size(); ++clause_idx)
        clauses.emplace_back(
            *this,
            *table_join,
            clause_idx,
            any_take_last_row,
            num_threads,
            max_bytes_before_external_join,
            build_blocks,
            accumulated_bytes,
            log);
    post_build_pools.resize(clauses.size());

    /// Per-row flags are allocated for the flagged (kind, strictness) pairs of `MapGetter`. The others
    /// mark nothing and read nothing.
    if (used_flags_per_row)
        joinDispatch(
            getKind(),
            getStrictness(),
            data->maps.front(),
            getMapsKind(),
            [&](auto kind_, auto strictness_, auto & map_)
            { allocate_per_row_flags = MapGetter<kind_, strictness_, mapsKindOf<decltype(map_)>()>::flagged; });

    /// Sized once and never resized, because the lock-free paths index them without synchronizing
    /// against growth. Twice the thread count leaves room for pipelines with more transforms than
    /// threads; a lane index past the table takes the mutexed fallback.
    fill_lane_slots = std::vector<std::atomic<FillLane *>>(2 * num_threads);
    probe_scratch_slots = std::vector<std::atomic<ProbeScratch *>>(2 * num_threads);

    if (table_join->collectAnalyzeStats())
        matched_rows_stats = std::make_unique<MatchedRowsStats>(getKind(), getStrictness(), table_join->analyzeMode());

    if (join_table_mode)
    {
        /// `StorageJoin` accepts one key clause, no mixed ON condition and no ASOF at `CREATE`, so these
        /// are not user errors.
        if (clauses.size() != 1 || used_flags_per_row)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: a Join table has exactly one key clause and no mixed ON condition");
        if (getStrictness() == JoinStrictness::Asof)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: a Join table cannot be ASOF");
        clauses.front().createJoinTable();
    }

    /// Charge the constructor's allocations before the plan creates another join.
    CurrentThread::flushUntrackedMemory();
    CurrentMemoryTracker::check();
}

static HashJoin::Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    using Type = HashJoin::Type;

    size_t keys_size = key_columns.size();

    if (keys_size == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot choose JOIN method without keys");

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        /// The loop above bails out before assigning `key_sizes` for a `LowCardinality` column.
        key_sizes[0] = size_of_field;
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 4)
        return Type::keys32;
    if (all_fixed && keys_bytes <= 8)
        return Type::keys64;
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1)
    {
        auto is_string_column = [](const IColumn * column_ptr) -> bool
        {
            if (const auto * lc_column_ptr = typeid_cast<const ColumnLowCardinality *>(column_ptr))
                return typeid_cast<const ColumnString *>(lc_column_ptr->getDictionary().getNestedColumn().get());
            return typeid_cast<const ColumnString *>(column_ptr);
        };

        const auto * key_column = key_columns[0];
        if (is_string_column(key_column)
            || (isColumnConst(*key_column) && is_string_column(assert_cast<const ColumnConst *>(key_column)->getDataColumnPtr().get())))
            return Type::key_string;
    }

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

/// If the column is a single non-nullable LowCardinality key, return the dictionary-aware map type
/// to use for it. LowCardinality(Nullable(T)) and wide numeric dictionaries fall back to the regular
/// (materialized) path. Mirrors the single-LowCardinality branch of AggregatedDataVariants::chooseMethod.
static std::optional<HashJoin::Type> tryGetLowCardinalityMethod(const ColumnPtr & column)
{
    using Type = HashJoin::Type;

    const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column.get());
    if (!low_cardinality_column)
        return {};

    if (low_cardinality_column->getDictionary().nestedColumnIsNullable())
        return {};

    const auto * nested = low_cardinality_column->getDictionary().getNestedNotNullableColumn().get();

    /// Numeric keys are intentionally not routed here. A materialized numeric key uses the key* maps,
    /// which (with `enable_join_fixed_hash_table_conversion`) convert a dense small range to a
    /// `range*_key*` FixedHashMap after build and can publish the shared fixed-hash-table runtime
    /// filter; the dictionary-aware map skips both for no measurable gain. The benefit of the
    /// dictionary-aware map is concentrated on variable-length string keys.
    if (typeid_cast<const ColumnString *>(nested))
        return Type::low_cardinality_key_string;
    if (typeid_cast<const ColumnFixedString *>(nested))
        return Type::low_cardinality_key_fixed_string;

    return {};
}

HashJoin::~HashJoin()
{
    /// Table first: cells point into the arenas and the row store.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinTeardownMicroseconds);

    /// The matched count serves the row store decision of the next run, see `onProbePhaseFinish`.
    if (build_phase_finished && probe_phase_finished && hash_table_matches.has_value()
        && match_stats_collecting_params.isCollectionAndUseEnabled())
    {
        try
        {
            getHashTablesStatistics<HashJoinMatchEntry>().update({.matches = *hash_table_matches}, match_stats_collecting_params);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    for (auto & clause : clauses)
        clause.releaseTable();
    destroyStoredBlocksInParallel();
    if (data)
        LOG_TEST(log, "Join data is being destroyed, {} bytes and {} rows in hash table", getTotalByteCountUnchecked(), getKeysToJoin());
    else
        LOG_TEST(log, "Join data has been already released");
    /// `data` and `used_flags` are declared before most members, so they would be destroyed after them. Their
    /// build-sized state goes here, in the same order and under the same timer. Nothing destroyed later reads it.
    data.reset();
    used_flags.reset();
    probe_scratch_pool.clear();
    for (auto & slot : probe_scratch_slots)
        delete slot.load(std::memory_order_acquire);
}

void HashJoin::destroyStoredBlocksInParallel()
{
    /// The table went with `clause.releaseTable()`, and the arena is one allocation. What is left to
    /// free is the stored right blocks: hundreds of megabytes to gigabytes on a large build. Several
    /// threads free them. A Join table's per-query instances share the storage's `data`, so only its
    /// last owner destroys it.
    static constexpr size_t PARALLEL_DESTROY_THRESHOLD_BYTES = 100 * 1024 * 1024;

    if (num_threads <= 1 || !data || data.use_count() != 1
        || data->allocated_size.load(std::memory_order_relaxed) < PARALLEL_DESTROY_THRESHOLD_BYTES)
        return;

    try
    {
        HashJoin::StoredBlocksList blocks = std::move(storedBlocks());
        const size_t num_tasks = std::min(num_threads, blocks.size());
        if (num_tasks <= 1)
            return;

        /// Runs of consecutive blocks spliced off the one list: a list node keeps its address, and no cell
        /// refers to a block any more.
        std::vector<HashJoin::StoredBlocksList> slices(num_tasks);
        const size_t blocks_per_task = (blocks.size() + num_tasks - 1) / num_tasks;
        for (auto & slice : slices)
        {
            auto end = blocks.begin();
            std::advance(end, std::min(blocks_per_task, blocks.size()));
            slice.splice(slice.end(), blocks, blocks.begin(), end);
        }

        ThreadPool pool(
            CurrentMetrics::HashJoinDestroyThreads,
            CurrentMetrics::HashJoinDestroyThreadsActive,
            CurrentMetrics::HashJoinDestroyThreadsScheduled,
            num_tasks);
        for (auto & slice : slices)
            pool.scheduleOrThrowOnError(
                [&slice, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::HASH_JOIN_DESTRUCTION);
                    slice.clear();
                });
        pool.wait();
    }
    catch (...)
    {
        /// Unscheduled blocks are freed when the local lists unwind.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool HashJoin::isSupported(const TableJoin & table_join)
{
    /// Everything `HashJoin` serves: INNER, LEFT, RIGHT and FULL with ALL, ANY, RightAny, SEMI, ANTI
    /// and ASOF. Null maps, ON filters, mixed non-equi conditions, USING, several disjuncts and a Join
    /// table or a key-value storage on the right side all pass. Cross, Comma, Paste and ON-constant
    /// joins are routed before the algorithm loop. The ASOF shapes `HashJoin` rejects (not INNER or
    /// LEFT, or without an equality key) throw its errors from the constructor, before the clauses
    /// are built. A memory limit is no reason to decline: the planner wraps this join
    /// in `SpillingHashJoin` instead.
    const JoinKind kind = table_join.kind();
    const JoinStrictness strictness = table_join.strictness();

    if (!isInner(kind) && !isLeft(kind) && !isRight(kind) && !isFull(kind))
        return false;

    switch (strictness)
    {
        case JoinStrictness::All:
        case JoinStrictness::Any:
        case JoinStrictness::RightAny:
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
        case JoinStrictness::Asof: return true;
        default: return false;
    }
}

const TableJoin & HashJoin::getTableJoin() const
{
    return *table_join;
}

void HashJoin::shareJoinTable(const HashJoin & source)
{
    if (!join_table_mode || !source.join_table_mode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: only the instances of a Join table share its table");
    /// `StorageJoin` checked the kind and strictness; the map type follows from the key columns, which
    /// are the storage's. Both have to agree, or the probe would read the cells through the wrong layout.
    if (clauses.front().mapsVariantIndex() != source.clauses.front().mapsVariantIndex() || data->type != source.data->type)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "HashJoin: the query's join shape (maps {}, type {}) does not match the Join table's (maps {}, type {})",
            clauses.front().mapsVariantIndex(),
            data->type,
            source.clauses.front().mapsVariantIndex(),
            source.data->type);

    reuseJoinedData(source);
    clauses.front().shareTable(source.clauses.front());
    shared_from_join_table = true;
    /// `reuseJoinedData` sized the flags to the empty map in `data`.
    reinitUsedFlags();
    /// The stored blocks are the storage's now, so the right-side flags of the statistics can be
    /// sized to them.
    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(storedBlocks());
}

DataTypePtr HashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    size_t num_keys = data_types.size();
    if (right_table_keys.columns() != num_keys)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of join_keys and number of right table key columns for function joinGet{} don't match: passed {}, should be equal to {}",
            toString(or_null ? "OrNull" : ""),
            toString(num_keys),
            toString(right_table_keys.columns()));

    for (size_t i = 0; i < num_keys; ++i)
    {
        const auto & left_type_origin = data_types[i];
        const auto & [c2, right_type_origin, right_name] = right_table_keys.safeGetByPosition(i);
        auto left_type = removeNullable(recursiveRemoveLowCardinality(left_type_origin));
        auto right_type = removeNullable(recursiveRemoveLowCardinality(right_type_origin));
        if (!left_type->equals(*right_type))
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Type mismatch in joinGet key {}: "
                "found type {}, while the needed type is {}",
                i,
                left_type->getName(),
                right_type->getName());
    }

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "StorageJoin doesn't contain column {}", column_name);

    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null && JoinCommon::canBecomeNullable(elem.type))
        elem.type = makeNullable(elem.type);
    return elem.type;
}

ColumnWithTypeAndName HashJoin::joinGet(const Block & block, const Block & block_with_columns_to_add)
{
    const bool is_valid = (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny) && getKind() == JoinKind::Left;
    if (!is_valid)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "joinGet only supports StorageJoin of type Left Any");

    /// The keys under the storage's names, which is how the probe reads a right-side key.
    const auto & key_names_right = table_join->getOnlyClause().key_names_right;
    Block keys;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto key = block.getByPosition(i);
        key.name = key_names_right[i];
        keys.insert(std::move(key));
    }

    /// Concurrent `joinGet` calls probe the storage's instance under its read lock; a flagged shape
    /// would write the shared used flags.
    static_assert(
        !MapGetter<JoinKind::Left, JoinStrictness::Any, JoinMapsKind::Default>::flagged,
        "joinGet is not protected from hash table changes between block processing");

    auto result = probeImpl<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsOne>(std::move(keys), invalid_lane, &block_with_columns_to_add);
    auto res = result->next();
    chassert(res.is_last);
    return res.block.getByPosition(res.block.columns() - 1);
}

void HashJoin::shrinkStoredBlocksToFit()
{
    size_t total_bytes = getTotalByteCount();
    compactStoredColumns(total_bytes);
}

HashJoin::FillLane & HashJoin::getFillLane()
{
    std::lock_guard lock(fill_mutex);
    auto [it, inserted] = lane_by_thread.try_emplace(std::this_thread::get_id(), nullptr);
    if (inserted)
        it->second = &lanes.emplace_back(clauses.size());
    return *it->second;
}

HashJoin::FillLane & HashJoin::getFillLane(size_t worker_id)
{
    if (worker_id >= fill_lane_slots.size())
        return getFillLane();

    if (FillLane * fast = fill_lane_slots[worker_id].load(std::memory_order_acquire))
        return *fast;

    /// First block of this lane: one mutexed emplace into the deque, whose elements are stable, and
    /// every later block takes the atomic load above. A worker id is unique per filling transform
    /// and a transform's work is serialized. The slot is therefore single-writer once published,
    /// even though executor threads migrate between transforms.
    std::lock_guard lock(fill_mutex);
    if (FillLane * raced = fill_lane_slots[worker_id].load(std::memory_order_relaxed))
        return *raced;
    FillLane * fresh = &lanes.emplace_back(clauses.size());
    fill_lane_slots[worker_id].store(fresh, std::memory_order_release);
    return *fresh;
}

bool HashJoin::addBlockToJoin(const Block & source_block, size_t /*num_rows*/, size_t worker_id, bool check_limits)
{
    /// `num_rows` only matters for the columnless CROSS blocks this algorithm never plans.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    if (build_phase_finished || stored_blocks_released)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: addBlockToJoin called after the build phase finished");

    /// Key preparation plus the per-row hash, route and sketch update. The partition plan comes later,
    /// at the barrier, so every plan pays exactly this much here.
    ProfileEventTimeIncrement<Microseconds> fill_watch(ProfileEvents::HashJoinPartitionedBuildFillMicroseconds);

    Block materialized = materializeColumnsFromRightBlock(source_block);
    const size_t rows = materialized.rows();
    if (rows == 0)
        return true;

    /// `RowRef::row_no` is 32-bit.
    if (rows > std::numeric_limits<UInt32>::max()) [[unlikely]]
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", rows);

    FillBlock fill;
    fill.rows = rows;
    /// Zeroed here, on the fill thread, rather than for every block at once on the thread that stores
    /// them.
    if (allocate_per_row_flags)
        fill.per_row_flags = JoinStuff::JoinUsedFlags::UsedFlagsForColumns(rows);

    fill.clauses.resize(clauses.size());
    for (const auto & clause : clauses)
        clause.prepareInput(materialized, fill);

    /// The payload in stored form. The constructor already decided the row store layout. The columns
    /// it admits are packed row-wise here. The probe then reads one row pointer per output row,
    /// instead of one random column read per output column. The remaining columns stay columnar.
    Block prepared = HashJoin::prepareRightBlock(materialized, savedBlockSample());
    assertBlocksHaveEqualStructureAllowReplicated(data->sample_block, prepared, "joined block");
    fill.stored = createStoredBlock(prepared, ScatteredBlock::Selector(rows));

    if (join_table_mode)
    {
        /// One block at a time under the storage's write lock: stored, then inserted straight into the
        /// table, which is probe-ready again when this returns. No routes, no sketch, no barrier. The
        /// limits are the storage's `max_rows_in_join` / `max_bytes_in_join`.
        const bool nullmap_saved = storeBlockInRowStore(fill);
        const bool any_row_stored = clauses.front().insertJoinTableBlock(fill);
        if (!any_row_stored && !nullmap_saved)
            dropLastStoredBlock();

        if (!check_limits)
            return true;
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }

    if (single_fill_thread)
    {
        /// One fill thread and no partition plan, so no routes and no sketch. The block is stored and
        /// its rows go into the table right away. The table grows like `hash`'s when it was sized low.
        /// The fill block stays alive through the insert because its key pointers point into its holders.
        accumulated_rows.fetch_add(rows, std::memory_order_relaxed);
        accumulated_bytes.fetch_add(fill.stored.allocatedBytes(), std::memory_order_relaxed);
        storeBlockInRowStore(fill);
        /// The table is sized before the first block from a distinct-key count, never from the row
        /// hint alone: the hint counts rows, and a build with many rows per key would get a table
        /// oversized by that multiplicity. The count is the one a previous run of this query left in
        /// the hash table statistics cache. Without one the table starts at the smallest degree and
        /// doubles as the keys arrive, as `hash`'s does. The row hint only caps the reserve: a table
        /// cannot hold more keys than rows. Every clause sizes its table from the one cached count.
        if (!clauses.front().hasTable())
        {
            const size_t keys = readDistinctKeysFromStatisticsCache() ? *cached_distinct_keys : 1;
            for (auto & clause : clauses)
                clause.beginSinglePartitionInsert(
                    clause.reserveFor(build_rows_hint.value_or(keys), static_cast<double>(keys)),
                    accumulated_rows.load(std::memory_order_relaxed),
                    /*grow_at_max_fill_=*/true);
        }
        for (auto & clause : clauses)
            clause.insertSingleLaneBlock(fill);

        return !check_limits || checkFillLimits();
    }

    FillLane & lane = getFillLane(worker_id);
    {
        /// A sketch merge reads `hll` under this lock, so it never sees a half-written register.
        /// One hash pass per clause.
        std::lock_guard hll_lock(lane.hll_mutex);
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
            clauses[clause_idx].computeRoutes(fill, lane.hll[clause_idx]);
    }

    accumulated_rows.fetch_add(rows, std::memory_order_relaxed);
    accumulated_bytes.fetch_add(fill.stored.allocatedBytes() + fill.routeBytes(), std::memory_order_relaxed);
    lane.blocks.push_back(std::move(fill));

    return !check_limits || checkFillLimits();
}

size_t HashJoin::rowCountForLimit(size_t max_rows) const
{
    /// One clause has at most one distinct key per input row. Merging its lane sketches is unnecessary
    /// until that bound reaches the limit. Multiple clauses count their keys separately.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    if (max_rows == 0 || (clauses.size() == 1 && rows < max_rows))
        return rows;
    return getTotalRowCount();
}

bool HashJoin::checkFillLimits()
{
    /// The parallel fill estimates the distinct keys; the exact count is checked after the build.
    const SizeLimits & limits = table_join->sizeLimits();
    if (!limits.hasLimits())
        return true;
    limits_requested = true;
    return limits.check(rowCountForLimit(limits.max_rows), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
    }
}

void HashJoin::setTotals(const Block & block)
{
    if (!block.empty())
    {
        std::lock_guard lock(totals_mutex);
        totals = block;
    }
}

const Block & HashJoin::getTotals() const
{
    return totals;
}

bool HashJoin::storeBlockInRowStore(FillBlock & fill)
{
    auto & right_data = *data;
    /// Registered and accounted while a local list still owns it:
    /// a registration that throws leaves no unregistered block behind, and `splice` cannot throw. A
    /// list node keeps its address across the splice.
    HashJoin::StoredBlocksList new_block;
    new_block.push_back(std::move(fill.stored));
    StoredBlock & stored = new_block.back();
    stored.block_no = right_data.stored_columns_index->add(&stored);
    right_data.addBytes(right_data.allocated_size, stored.allocatedBytes());
    right_data.rows_to_join.fetch_add(fill.rows, std::memory_order_relaxed);
    storedBlocks().splice(storedBlocks().end(), new_block);
    fill.block_no = stored.block_no;
    fill.stored = StoredBlock{};
    if (stored.hasRowStore())
    {
        ++row_store_blocks;
        ProfileEvents::increment(ProfileEvents::HashJoinRowStoreBlocks);
    }

    /// Per-row used flags cover every stored row, the ones that never enter a table included. A row
    /// nothing marks is emitted as non-joined, so no null map is kept for it.
    /// The flags are attached here, on the one thread that stores blocks, before any probe can read them.
    if (allocate_per_row_flags)
    {
        auto & flags = used_flags->per_row_flags;
        if (flags.size() <= stored.block_no)
            flags.resize(stored.block_no + 1);
        flags[stored.block_no] = std::move(fill.per_row_flags);
    }
    if (!isRightOrFull(getKind()) || used_flags_per_row)
        return false;

    /// RIGHT/FULL output needs the rows that never made it into the table - null keys and rows the
    /// ON condition filtered. They are saved from the one clause's null map and mask.
    HashJoinClause::Input & input = fill.clauses.front();
    bool save_nullmap = false;
    if (input.null_map)
        for (size_t i = 0; i < fill.rows && !save_nullmap; ++i)
            save_nullmap = (*input.null_map)[i];
    bool nullmap_saved = false;
    if (save_nullmap)
    {
        auto & holder = storedNullmaps().emplace_back(&stored, input.null_map_holder);
        right_data.addBytes(right_data.nullmaps_allocated_size, holder.allocatedBytes());
        nullmap_saved = true;
    }

    if (input.join_mask.hasData() && input.join_mask.getKind() != JoinCommon::JoinMask::Kind::AllTrue)
    {
        auto not_joined_map = ColumnUInt8::create(fill.rows, static_cast<UInt8>(0));
        bool has_right_not_joined = false;
        for (size_t i = 0; i < fill.rows; ++i)
        {
            if (!input.join_mask.isRowFiltered(i))
                continue;
            if (save_nullmap && (*input.null_map)[i])
                continue; /// already covered by the null-keys map
            not_joined_map->getData()[i] = 1;
            has_right_not_joined = true;
        }
        if (has_right_not_joined)
        {
            auto & holder = storedNullmaps().emplace_back(&stored, std::move(not_joined_map));
            right_data.addBytes(right_data.nullmaps_allocated_size, holder.allocatedBytes());
            nullmap_saved = true;
        }
    }
    return nullmap_saved;
}

void HashJoin::dropLastStoredBlock()
{
    /// The engine stores one block at a time, so the block just stored is the list's last. Its rows
    /// went through the insert, so the key count stands; the row count and the bytes must not include it.
    auto & right_data = *data;
    StoredBlock & stored = storedBlocks().back();
    right_data.subBytes(right_data.allocated_size, stored.allocatedBytes());
    right_data.rows_to_join.fetch_sub(stored.selector.size(), std::memory_order_relaxed);
    /// No cell refers to the block; the nulled entry makes a stale ref fail loudly.
    right_data.stored_columns_index->clearEntry(stored.block_no);
    storedBlocks().pop_back();
}

void HashJoin::onBuildPhaseFinish()
{
    /// A Join table's join is built at every point: the storage inserts under its write lock and the
    /// per-query instances only probe. There is nothing to finish.
    if (join_table_mode)
        return;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    if (single_fill_thread)
    {
        /// Everything was inserted as it arrived. A build that never saw a block still needs its tables,
        /// because the used flags and the probe are sized from them. The exact distinct count stands in
        /// for the sketch estimate the memory gate reads.
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        {
            auto & clause = clauses[clause_idx];
            if (!clause.hasTable())
                clause.beginSinglePartitionInsert(
                    clause.reserveFor(1, 1.0), accumulated_rows.load(std::memory_order_relaxed), /*grow_at_max_fill_=*/true);
            clause.setDistinctEstimate(static_cast<double>(clause.claimedTotal()));
            ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clause.partitionCount());
            LOG_TRACE(
                log,
                "Single fill thread: table of clause {} has 2^{} cells, {} rows in {} blocks inserted during the fill, {} distinct keys",
                clause_idx,
                clause.sizeDegree(),
                accumulated_rows.load(std::memory_order_relaxed),
                storedBlocks().size(),
                static_cast<size_t>(clause.hllEstimate()));
        }
        return;
    }

    /// Run once by the last fill thread, and deliberately cheap: concatenate the lanes, number the
    /// row-store blocks, merge the sketches, pick the plan. The scatter, allocation and inserts are
    /// `runPostBuildPhase`'s work. Every fill call has returned, so the lane locks are free.
    std::vector<DenseHyperLogLog> merged(clauses.size());
    size_t total_blocks = 0;
    {
        std::lock_guard lock(fill_mutex);
        for (const auto & lane : lanes)
            total_blocks += lane.blocks.size();
        build_blocks.reserve(total_blocks);
        for (auto & lane : lanes)
        {
            {
                std::lock_guard hll_lock(lane.hll_mutex);
                for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
                    merged[clause_idx].merge(lane.hll[clause_idx]);
            }
            for (auto & block : lane.blocks)
                build_blocks.push_back(std::move(block));
            lane.blocks.clear();
        }
        lanes.clear();
        lane_by_thread.clear();
    }

    /// A previous run's exact count replaces the sketch estimate. The table it sizes needs no safety
    /// margin and, when the data has not changed, no grow. The entry counts the keys of every clause
    /// together, and each clause sizes from it.
    const bool exact = readDistinctKeysFromStatisticsCache();
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        clauses[clause_idx].setDistinctEstimate(exact ? static_cast<double>(*cached_distinct_keys) : merged[clause_idx].estimate(), exact);
    for (auto & fill : build_blocks)
        storeBlockInRowStore(fill);
    /// Every estimate is set before the first plan: a clause's partition floor is guarded by what the
    /// other clauses' tables will take.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
        clauses[clause_idx].decidePartitionPlan(rows);
        ProfileEvents::increment(ProfileEvents::HashJoinPartitions, clauses[clause_idx].partitionCount());
    }
}

HashJoin::PostBuildPlan HashJoin::planPostBuild()
{
    if (max_bytes_before_external_join == 0)
        return PostBuildPlan::Fits;

    /// Everything is already resident: the stored blocks, the table and the duplicate runs.
    if (single_fill_thread)
        return getTotalByteCount() <= max_bytes_before_external_join ? PostBuildPlan::Fits : PostBuildPlan::MustSpill;

    /// The worst verdict over the clauses wins: `MustSpill` over `Grouped` over `Fits`, the enum's order.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    PostBuildPlan plan = PostBuildPlan::Fits;
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
        plan = std::max(plan, clauses[clause_idx].planPostBuild(rows, postBuildPool(clause_idx)));
    }
    return plan;
}

void HashJoin::runPostBuildPhase()
{
    if (join_table_mode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: a Join table's join has no post-build phase");
    chassert(!build_phase_finished);

    bool all_unique = true;
    if (single_fill_thread)
    {
        /// The rows went in during the fill; only the scratch finish and the publication remain.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);
        ProfileEventTimeIncrement<Microseconds> leaf_watch(ProfileEvents::HashJoinPartitionedBuildInsertMicroseconds);
        for (auto & clause : clauses)
            all_unique &= clause.finishSinglePartitionInsert();
    }
    else if (clauses.size() == 1 || max_bytes_before_external_join != 0)
    {
        /// One clause after another. A clause's scatter releases only its own inputs, so the next clause
        /// still finds its keys and routes in the fill blocks. A memory budget keeps this order. The gate
        /// counts one scatter transient at a time, and each build's budget counts the tables built before
        /// it and the ones predicted after it.
        const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        {
            clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
            all_unique &= clauses[clause_idx].postBuild(rows, postBuildPool(clause_idx));
        }
    }
    else
    {
        /// Without a budget the clauses build at once, each on a pool of its own. The barrier waves
        /// (histogram, allocate, scatter, owner, drain) are fixed costs that add up per clause on a small
        /// build. A clause reads and releases only its own inputs of the fill blocks and writes only its
        /// own table, arenas and counters. The join's byte count is atomic. The pools are created on this
        /// thread, before any worker could create one.
        const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
        std::vector<UInt8> clause_unique(clauses.size(), 1);
        for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        {
            clauses[clause_idx].setBytesReservedElsewhere(bytesReservedForOtherClauses(clause_idx));
            postBuildPool(clause_idx);
        }
        auto clause_pool = HashJoinClause::makePostBuildPool(clauses.size() - 1);
        try
        {
            for (size_t clause_idx = 1; clause_idx < clauses.size(); ++clause_idx)
                clause_pool->scheduleOrThrow(
                    [this, clause_idx, rows, &clause_unique, thread_group = CurrentThread::getGroup()]
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                        clause_unique[clause_idx] = clauses[clause_idx].postBuild(rows, *post_build_pools[clause_idx]);
                    });
            clause_unique[0] = clauses.front().postBuild(rows, *post_build_pools[0]);
            clause_pool->wait();
        }
        catch (...)
        {
            clause_pool->wait();
            throw;
        }
        for (UInt8 unique : clause_unique)
            all_unique &= unique != 0;
    }

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);

    /// The routes and prepared key columns were already dropped as the scatter consumed them; this
    /// is the block shells and the lane bookkeeping, freed before the probe starts.
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    /// From here the byte count tracks only the stored blocks.
    accumulated_bytes.store(data->allocated_size, std::memory_order_relaxed);

    UInt64 distinct_keys = 0;
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        const BuildStats built = clauses[clause_idx].buildStats();
        const size_t duplicate_run_bytes = built.owner_duplicates.arena_bytes + built.drain_duplicates.arena_bytes;
        distinct_keys += built.distinct_keys;
        ProfileEvents::increment(ProfileEvents::HashJoinTableBytes, built.ht_total_bytes);
        ProfileEvents::increment(ProfileEvents::HashJoinPartitionOverflowRows, built.overflow_rows);
        ProfileEvents::increment(ProfileEvents::HashJoinDuplicateRunBytes, duplicate_run_bytes);
        LOG_TRACE(
            log,
            "Built the hash table of clause {}: {} cells in {} partitions, {} keys from {} rows ({} committed, {} overflow rows "
            "drained, {} bytes of duplicate runs)",
            clause_idx,
            built.table_cells,
            built.partitions,
            built.distinct_keys,
            built.inserted_rows,
            ReadableSize(built.ht_total_bytes),
            built.overflow_rows,
            ReadableSize(duplicate_run_bytes));
        clauses[clause_idx].releaseBuildScratch();
    }
    for (auto & pool : post_build_pools)
        pool.reset();

    /// The entry is for the next run of this query. Join reordering, `rhs_size_estimation`,
    /// runtime-filter sizing and this join's own table size (`readDistinctKeysFromStatisticsCache`)
    /// read `HashJoinEntry`. `ht_size` is the exact distinct count, summed over the clauses. Nothing else
    /// writes this key for this join.
    if (stats_collecting_params.isCollectionAndUseEnabled() && distinct_keys)
        getHashTablesStatistics<HashJoinEntry>().update(
            {.ht_size = distinct_keys, .source_rows = data->rows_to_join}, stats_collecting_params);

    /// Only the table of a single clause is converted and published; several clauses keep their hash tables.
    const bool one_clause = clauses.size() == 1;
    if (one_clause)
        clauses.front().tryConvertToFixedHashMap();
    finishBuildPhase(all_unique);

    /// With the table settled and its key count published: the exact runtime filter of a fixed table
    /// (8- or 16-bit keys, or a range map the conversion built) replaces the planner's Bloom filter.
    /// Only from a table that holds the whole build side; a bucket's would drop the probe rows of every
    /// other bucket.
    if (one_clause && !partial_build)
        std::visit(
            [&](const auto & shape_maps)
            {
                publishSharedFixedHashTableFilters(
                    *table_join,
                    right_table_keys,
                    data->type,
                    data->key_range,
                    data->keys_to_join.load(std::memory_order_relaxed),
                    shape_maps);
            },
            clauses.front().tableMaps().maps);

    LOG_TRACE(
        log,
        "Built {} hash table(s) with {} keys, {} of right-table data including the tables",
        clauses.size(),
        getTotalRowCount(),
        ReadableSize(getTotalByteCount()));
}

void HashJoin::finishBuildPhase(bool all_values_unique_)
{
    /// `finishMapsBuild` sizes the flags to the empty maps in `data`, and ALL becomes RightAny when every
    /// key was unique. The probe dispatches on the promoted strictness. Flags are then resized to the whole table.
    all_values_unique = all_values_unique_;
    finishMapsBuild();
    reinitUsedFlags();
    /// Every stored block has its final number: the right-side flags of the statistics are sized
    /// per block.
    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(storedBlocks());
    build_phase_finished = true;
    /// Read after the flag, so the count is the table's. Before it, `getTotalRowCount` gives the fill
    /// sketches' estimate, and the barrier has already discarded the lanes with the sketches.
    data->keys_to_join = getTotalRowCount();

    /// The fill checked the sketch's estimate of the distinct keys; this is the exact count.
    if (limits_requested.load(std::memory_order_relaxed))
        table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

ThreadPool & HashJoin::postBuildPool(size_t clause_idx)
{
    auto & pool = post_build_pools[clause_idx];
    if (!pool)
        pool = HashJoinClause::makePostBuildPool(std::max<size_t>(1, std::min(num_threads, build_blocks.size())));
    return *pool;
}

void HashJoin::reinitUsedFlags()
{
    /// The per-row shape marks the flags of the stored rows and never reads a per-offset flag. The
    /// `cells + 1` space would be allocated and zeroed for nothing. `reinitUsedFlagsForMaps` skips it
    /// for the same shape.
    if (used_flags_per_row)
        return;

    /// One per-offset space of `cells + 1` (offset 0 is the zero-value cell). Must run after
    /// `finishMapsBuild`, which sized the flags to the empty maps in `data`. `reinit` only grows.
    const size_t flags = clauses.front().tableCells() + 1;
    joinDispatch(
        getKind(),
        getStrictness(),
        data->maps.front(),
        getMapsKind(),
        [&](auto kind_, auto strictness_, auto & map_)
        {
            used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map_)>()>(flags);
        });
}

JoinResultPtr HashJoin::joinBlock(Block block)
{
    return joinBlock(std::move(block), invalid_lane);
}

JoinResultPtr HashJoin::joinBlock(Block block, size_t lane)
{
    JoinResultPtr result;
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedProbeMicroseconds);
        result = probeDispatch(std::move(block), lane);
    }
    return std::make_unique<TimedJoinResult>(std::move(result), ProfileEvents::HashJoinPartitionedProbeMicroseconds);
}

size_t HashJoin::getTotalRowCount() const
{
    /// The distinct keys summed over the clauses' tables: `max_rows_in_join` and the `JoinSwitcher` limit
    /// count hash table rows, not input rows. A Join table's keys are shared with the per-query instances,
    /// so they are read from the table rather than from this instance's fill counter. Before the barrier
    /// the lanes only have the sketches, so the count is their estimate, never above the rows seen.
    if (join_table_mode || build_phase_finished)
    {
        size_t keys = 0;
        for (const auto & clause : clauses)
            keys += clause.tableRowCount();
        return keys;
    }

    /// A single fill thread inserts as it goes, but the table publishes its size only when the build
    /// finishes. Until then the claimed cells are the count. They are exact between blocks, which is
    /// when the spilling wrapper's row limit and `GraceHashJoin::hasMemoryOverflow` read it.
    if (single_fill_thread && clauses.front().hasTable())
    {
        size_t keys = 0;
        for (const auto & clause : clauses)
            keys += clause.claimedTotal();
        return keys;
    }

    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    if (rows == 0)
        return 0;
    size_t keys = 0;
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        keys += std::min(rows, liveDistinctEstimate(clause_idx));
    return keys;
}

size_t HashJoin::getTotalByteCount() const
{
    if (join_table_mode)
    {
        /// The storage's stored blocks (shared with the per-query instances), the table and its arena.
        const auto & right_data = storedData();
        return right_data.allocated_size + right_data.nullmaps_allocated_size + tablesAndArenasBytes();
    }

    return accumulated_bytes.load(std::memory_order_relaxed) + storedData().nullmaps_allocated_size + tablesAndArenasBytes();
}

size_t HashJoin::tablesAndArenasBytes() const
{
    size_t bytes = 0;
    for (const auto & clause : clauses)
        bytes += clause.tableAndArenaBytes();
    return bytes;
}

size_t HashJoin::bytesReservedForOtherClauses(size_t clause_idx) const
{
    /// A built table counts what it holds; one still to build counts its prediction. A table created but
    /// not yet committed holds little, so the larger of the two stands for it. The routes of every clause
    /// are in the join's byte count already.
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    size_t bytes = 0;
    for (size_t other = 0; other < clauses.size(); ++other)
    {
        if (other == clause_idx)
            continue;
        const auto & clause = clauses[other];
        bytes += std::max(
            clause.tableAndArenaBytes(), clause.predictedTableAndArenaBytes(rows, clause.distinctEstimate(), /*grouped=*/false));
    }
    return bytes;
}

size_t HashJoin::liveDistinctEstimate(size_t clause_idx) const
{
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    const size_t last_rows = distinct_estimate_at_rows.load(std::memory_order_acquire);
    const size_t cached = cached_distinct_estimates[clause_idx].load(std::memory_order_relaxed);

    if (cached != 0 && rows <= last_rows + last_rows / 16)
        return cached;

    std::lock_guard lock(fill_mutex);
    const size_t last_rows_locked = distinct_estimate_at_rows.load(std::memory_order_relaxed);
    const size_t cached_locked = cached_distinct_estimates[clause_idx].load(std::memory_order_relaxed);
    if (cached_locked != 0 && rows <= last_rows_locked + last_rows_locked / 16)
        return cached_locked;

    /// Every clause's sketches are merged in one refresh, lane by lane: a lane in the middle of a hash
    /// pass is waited for, the others keep filling.
    std::vector<DenseHyperLogLog> merged(clauses.size());
    for (const auto & lane : lanes)
    {
        std::lock_guard hll_lock(lane.hll_mutex);
        for (size_t other = 0; other < clauses.size(); ++other)
            merged[other].merge(lane.hll[other]);
    }
    /// Floor at 1 so a still-empty sketch does not size the prediction as a zero-byte table. The
    /// post-build gate uses the same floor on `hll_estimate`. The value is not kept monotone: an
    /// early small-sample HyperLogLog can overshoot, and locking that in would charge duplicate-run
    /// bytes for keys that do not exist.
    size_t result = 0;
    for (size_t other = 0; other < clauses.size(); ++other)
    {
        const size_t estimate = std::max(static_cast<size_t>(std::llround(merged[other].estimate())), 1uz);
        cached_distinct_estimates[other].store(estimate, std::memory_order_relaxed);
        if (other == clause_idx)
            result = estimate;
    }
    distinct_estimate_at_rows.store(rows, std::memory_order_release);
    return result;
}

bool HashJoin::readDistinctKeysFromStatisticsCache()
{
    /// The entry `runPostBuildPhase` publishes: the exact distinct count of the previous run of this
    /// query. The cache keeps it until a run finds less than half of it, so it overstates by at most 2x.
    /// It understates only when the data grew, and then the table grows during the build, as it did
    /// before. A table sized from it counts as a preallocation (`HashJoinPreallocatedElementsInHashTables`).
    const auto hint = getSizeHint(stats_collecting_params);
    if (!hint || hint->ht_size > stats_collecting_params.max_size_to_preallocate)
        return false;
    cached_distinct_keys = hint->ht_size;
    ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, hint->ht_size);
    return true;
}

size_t HashJoin::predictedResidentBytes(bool at_barrier) const
{
    if (single_fill_thread)
    {
        const HashJoin::Type type = data->type;

        /// At the barrier every row is in and the resident set is what it is; only a table past its
        /// maximum fill still has a doubling ahead of it, in place with both buffers alive.
        if (at_barrier)
        {
            size_t doublings = 0;
            for (const auto & clause : clauses)
                if (clause.hasTable() && clause.claimedTotal() > clause.tableMaps().maxFill(type))
                    doublings += 2 * clause.tableMaps().getBufferSizeInBytes(type);
            return getTotalByteCount() + doublings;
        }

        /// Before the barrier the prediction follows the rows, as the partitioned branch does: the stored
        /// bytes plus the table and arenas `predictedTableAndArenaBytes` sizes for the keys seen so far.
        /// The keys are the exact claimed count, or the cached count the table was sized for when that
        /// is larger. The table this thread holds is not the measure: it starts at the smallest degree
        /// and would charge three buffers to a handful of rows. The row hint takes no part either, since
        /// it would charge a duplicate-heavy build the table of its row count. A table whose claimed keys
        /// come within one block of its maximum fill doubles on the next block, so the doubled table is
        /// the prediction then.
        const auto & right_data = storedData();
        const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
        const size_t blocks = storedBlocks().size();
        const size_t rows_per_block = blocks == 0 ? rows : rows / blocks;
        size_t predicted = right_data.allocated_size + right_data.nullmaps_allocated_size;
        for (const auto & clause : clauses)
        {
            const size_t claimed = clause.hasTable() ? clause.claimedTotal() : 0;
            const size_t keys = std::max(claimed, cached_distinct_keys.value_or(0));
            size_t table = clause.predictedTableAndArenaBytes(std::max(rows, keys), keys, /*grouped=*/false);
            if (clause.hasTable() && claimed + rows_per_block > clause.tableMaps().maxFill(type))
                table = std::max(table, 2 * clause.tableMaps().getBufferSizeInBytes(type));
            predicted += table;
        }
        return predicted;
    }

    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    size_t predicted = accumulated_bytes.load(std::memory_order_relaxed);
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
        predicted += clauses[clause_idx].predictedTableAndArenaBytes(rows, liveDistinctEstimate(clause_idx), /*grouped=*/false);
    return predicted;
}

size_t HashJoin::graceInMemoryEstimateBytes() const
{
    const auto & right_data = storedData();
    const size_t rows = accumulated_rows.load(std::memory_order_relaxed);
    size_t bytes = right_data.allocated_size + right_data.nullmaps_allocated_size;
    for (const auto & clause : clauses)
        bytes += clause.predictedTableAndArenaBytes(rows, clause.distinctEstimate(), /*grouped=*/false);
    return bytes;
}

StepAnalysisReport HashJoin::getAnalysisReport() const
{
    /// A Join table's rows are the storage's, shared with every per-query instance.
    const size_t right_rows = join_table_mode ? storedData().rows_to_join.load() : accumulated_rows.load(std::memory_order_relaxed);

    StepAnalysisReport report;
    if (matched_rows_stats)
    {
        report = buildMatchedRowsReport(
            {.left_rows = matched_rows_stats->getInputLeft(),
             .matched_left = matched_rows_stats->getMatchedLeft(),
             .right_rows = right_rows,
             .matched_right = matched_rows_stats->getMatchedRight(right_rows)});
    }
    else
    {
        MetricList right_metrics;
        right_metrics.emplace_back(MetricKey::Rows, right_rows);
        report.push_back({MetricGroupKey::Right, std::move(right_metrics)});
    }

    MetricList hash_table_metrics;
    hash_table_metrics.emplace_back(MetricKey::UniqueKeys, getTotalRowCount());
    hash_table_metrics.emplace_back(MetricKey::Memory, getTotalByteCount());
    report.push_back({MetricGroupKey::HashTable, std::move(hash_table_metrics)});

    return report;
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    /// A Join table's rows are the storage's, shared with every per-query instance.
    const size_t rows = join_table_mode ? storedData().rows_to_join.load() : accumulated_rows.load(std::memory_order_relaxed);
    const bool empty_for_empty_right
        = isInnerOrRight(table_join->kind()) || (isLeft(table_join->kind()) && table_join->strictness() == JoinStrictness::Semi);
    return empty_for_empty_right && rows == 0;
}

HashJoin::BuildStats HashJoin::getBuildStats(size_t clause_idx) const
{
    BuildStats res = clauses[clause_idx].buildStats();
    res.row_store_blocks = row_store_blocks;
    return res;
}

std::unique_ptr<HashJoin::ProbeScratch> HashJoin::acquireProbeScratch(size_t lane)
{
    if (lane < probe_scratch_slots.size())
        if (ProbeScratch * parked = probe_scratch_slots[lane].exchange(nullptr, std::memory_order_acquire))
            return std::unique_ptr<ProbeScratch>(parked);

    {
        std::lock_guard lock(probe_scratch_mutex);
        if (!probe_scratch_pool.empty())
        {
            auto scratch = std::move(probe_scratch_pool.back());
            probe_scratch_pool.pop_back();
            return scratch;
        }
    }
    return std::make_unique<ProbeScratch>();
}

void HashJoin::releaseProbeScratch(std::unique_ptr<ProbeScratch> scratch, size_t lane)
{
    /// Park it back when the slot is free. A collision or an out-of-range lane falls through to the
    /// pool. The scratch is then neither lost nor doubly owned.
    if (lane < probe_scratch_slots.size())
    {
        ProbeScratch * expected = nullptr;
        if (probe_scratch_slots[lane].compare_exchange_strong(expected, scratch.get(), std::memory_order_release))
        {
            scratch.release(); /// NOLINT(bugprone-unused-return-value): ownership moved into the slot
            return;
        }
    }

    std::lock_guard lock(probe_scratch_mutex);
    probe_scratch_pool.push_back(std::move(scratch));
}

bool HashJoin::isCloneSupported() const
{
    return getTotals().empty() && getTotalRowCount() == 0;
}

std::shared_ptr<IJoin>
HashJoin::clone(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const
{
    return cloneWithBuildRowsHint(table_join_, std::move(right_sample_block_), build_rows_hint);
}

std::shared_ptr<IJoin> HashJoin::cloneWithBuildRowsHint(
    const std::shared_ptr<TableJoin> & table_join_, SharedHeader right_sample_block_, std::optional<size_t> build_rows_hint_) const
{
    /// Every reachable clone path preserves a supported shape; re-checked so that a future caller
    /// which does not surfaces as an exception instead of wrong results.
    if (!isSupported(*table_join_))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: attempt to clone with a join shape the algorithm does not support");
    return std::make_shared<HashJoin>(
        table_join_,
        right_sample_block_,
        num_threads,
        any_take_last_row,
        HashJoinStatsCollectingParams{.build = stats_collecting_params, .match = match_stats_collecting_params},
        max_bytes_before_external_join,
        build_rows_hint_);
}

std::shared_ptr<IJoin>
HashJoin::cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_, SharedHeader, SharedHeader right_sample_block_) const
{
    if (!isSupported(*table_join_))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: attempt to clone with a join shape the algorithm does not support");
    auto shard_join = std::make_shared<HashJoin>(
        table_join_, right_sample_block_, /*num_threads_=*/1, any_take_last_row, HashJoinStatsCollectingParams{}, /*max_bytes_before_external_join_=*/0, build_rows_hint);
    shard_join->parallel_non_joined_allowed = false;
    shard_join->partial_build = true;
    return shard_join;
}

void HashJoin::setEnableLazyColumnsIndexing(bool value)
{
    enable_lazy_columns_indexing = value;
}

size_t HashJoin::getNumFillLanes() const
{
    return lanes.size();
}

void HashJoin::dropFillAuxiliary()
{
    for (auto & lane : lanes)
        for (auto & fill : lane.blocks)
            accumulated_bytes.fetch_sub(fill.releaseInputs(), std::memory_order_relaxed);
    for (auto & fill : build_blocks)
        accumulated_bytes.fetch_sub(fill.releaseInputs(), std::memory_order_relaxed);
}

Block HashJoin::releaseNextFillLaneBlock(size_t lane)
{
    chassert(lane < lanes.size());
    auto & blocks = lanes[lane].blocks;
    if (blocks.empty())
        return {};

    FillBlock fill = std::move(blocks.back());
    blocks.pop_back();
    if (blocks.empty())
        blocks.shrink_to_fit();

    /// `dropFillAuxiliary` has usually released the routes already. `routeBytes` counts only what is
    /// really held.
    accumulated_bytes.fetch_sub(fill.stored.allocatedBytes() + fill.routeBytes(), std::memory_order_relaxed);
    return storedBlockToBlock(std::move(fill.stored));
}

Block HashJoin::storedBlockToBlock(StoredBlock && stored) const
{
    const auto & right_data = *data;
    return right_data.sample_block.cloneWithColumns(HashJoin::materializeStoredBlock(stored, right_data.column_access_indexes));
}

void HashJoin::beginStoredBlockDrain()
{
    stored_blocks_released = true;
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    for (auto & pool : post_build_pools)
        pool.reset();
    for (auto & clause : clauses)
        clause.releaseTable();
}

Block HashJoin::releaseNextStoredBlock()
{
    if (!data)
        return {};
    if (storedBlocks().empty())
    {
        data.reset();
        return {};
    }

    auto & right_data = *data;
    auto & blocks = storedBlocks();
    StoredBlock stored = std::move(blocks.front());
    blocks.pop_front();
    right_data.subBytes(right_data.allocated_size, stored.allocatedBytes());

    Block out = storedBlockToBlock(std::move(stored));

    if (blocks.empty())
        data.reset();
    return out;
}

BlocksList HashJoin::releaseJoinedBlocks(bool restructure)
{
    if (build_phase_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin: the right blocks were asked for after the build phase finished");

    dropFillAuxiliary();
    BlocksList blocks;
    for (size_t lane = 0; lane < lanes.size(); ++lane)
        for (Block block = releaseNextFillLaneBlock(lane); !block.empty(); block = releaseNextFillLaneBlock(lane))
            blocks.push_back(std::move(block));
    /// A single fill thread stores as it goes and keeps no lanes.
    beginStoredBlockDrain();
    for (Block block = releaseNextStoredBlock(); !block.empty(); block = releaseNextStoredBlock())
        blocks.push_back(std::move(block));

    /// Restoring the blocks allocates, so it can throw here with the join's data already gone.
    fiu_do_on(FailPoints::hash_join_throw_after_data_release, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after the join data was released");
    });

    if (restructure)
        for (auto & block : blocks)
            block = HashJoin::restoreRightBlock(block, *right_input_header);
    return blocks;
}

size_t HashJoin::getRightTableRowCount() const
{
    return accumulated_rows.load(std::memory_order_relaxed);
}

void HashJoin::drainStoredBlocksInto(IJoin & target)
{
    chassert(stored_blocks_released);

    const size_t blocks = data ? storedBlocks().size() : 0;
    const size_t workers = std::min(num_threads, blocks);
    if (workers <= 1)
    {
        for (Block block = releaseNextStoredBlock(); !block.empty(); block = releaseNextStoredBlock())
            target.addBlockToJoin(block, block.rows(), /*worker_id=*/0, /*check_limits=*/false);
        return;
    }

    /// The pop is a list front and a few counters, so one mutex serializes it cheaply; the scatter,
    /// compression and write inside `target.addBlockToJoin` run in parallel, one target worker each.
    std::mutex pop_mutex;
    auto drain = [&](size_t worker_id)
    {
        while (true)
        {
            Block block;
            {
                std::lock_guard lock(pop_mutex);
                block = releaseNextStoredBlock();
            }
            if (block.empty())
                return;
            target.addBlockToJoin(block, block.rows(), worker_id, /*check_limits=*/false);
        }
    };

    auto pool = HashJoinClause::makePostBuildPool(workers);
    try
    {
        for (size_t w = 0; w < workers; ++w)
            pool->scheduleOrThrow(
                [&drain, w, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                    drain(w);
                });
        pool->wait();
    }
    catch (...)
    {
        pool->wait();
        throw;
    }
}

void HashJoin::dataMapInit(MapsVariant & map)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin::dataMapInit called with empty data");

    const auto maps_kind = getMapsKind();
    joinDispatchInit(kind, strictness, map, maps_kind);
    joinDispatch(kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { map_.create(data->type); });
}


bool HashJoin::preferUseMapsAll() const
{
    return all_join_was_promoted_to_right_any // It means that we built hash tables for ALL strictness, but upon finishing found out that we can switch to RIGHT ANY.
                                              // In this case we still have to use ALL maps.
        || table_join->getMixedJoinExpression() != nullptr;
}

/// A set map answers whether a key is present and nothing else.
/// It fits joins whose result can never contain a value taken from a right row.
bool HashJoin::canUseSetMaps() const
{
    if (!allow_set_maps || !table_join->enableJoinKeyOnlyHashTables())
        return false;

    /// A mixed join expression is evaluated against the right rows themselves.
    if (preferUseMapsAll() || table_join->getMixedJoinExpression())
        return false;

    /// `StorageJoin` reads its rows back out of the maps, both to `SELECT` from the table and for
    /// `joinGet`.
    if (table_join->isSpecialStorage())
        return false;

    if (kind != JoinKind::Left)
        return false;

    /// LEFT ANTI emits a left row only when its key is missing, and fills the right columns with
    /// defaults, so no right row is ever read whatever is selected from the right side.
    if (strictness == JoinStrictness::Anti)
        return true;

    /// LEFT SEMI emits the matched left row alone, so it qualifies when nothing of the right side
    /// besides the join keys - which are taken from the left row - is selected.
    return strictness == JoinStrictness::Semi && sample_block_with_columns_to_add.columns() == 0;
}

JoinMapsKind HashJoin::getMapsKind() const
{
    if (preferUseMapsAll())
        return JoinMapsKind::All;
    if (use_set_maps)
        return JoinMapsKind::Set;
    return JoinMapsKind::Default;
}

size_t HashJoin::getKeysToJoin() const
{
    if (!data)
        return 0;

    /// A running total, set from the tables by `finishBuildPhase` and after every block of a Join table.
    return data->keys_to_join.load(std::memory_order_relaxed);
}

void HashJoin::recomputeMapsBytes()
{
    if (!data)
        return;

    size_t res = data->pool.allocatedBytes();

    const auto maps_kind = getMapsKind();
    for (const auto & map : data->maps)
    {
        joinDispatch(
            kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
    }

    data->setBytes(data->maps_bytes, res);
}

void HashJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    size_t debug_allocated_size = 0;
    size_t debug_nullmaps_allocated_size = 0;
    for (const auto & columns : data->columns)
        debug_allocated_size += columns.allocatedBytes();
    for (const auto & nullmap : data->nullmaps)
        debug_nullmaps_allocated_size += nullmap.allocatedBytes();

    if (data->allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->allocated_size != debug_allocated_size ({} != {})",
            data->allocated_size.load(std::memory_order_relaxed),
            debug_allocated_size);

    if (data->nullmaps_allocated_size != debug_nullmaps_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->nullmaps_allocated_size != debug_nullmaps_allocated_size ({} != {})",
            data->nullmaps_allocated_size.load(std::memory_order_relaxed),
            debug_nullmaps_allocated_size);

    const size_t accounted = data->allocated_size.load(std::memory_order_relaxed)
        + data->nullmaps_allocated_size.load(std::memory_order_relaxed) + data->maps_bytes.load(std::memory_order_relaxed);
    if (data->total_bytes.load(std::memory_order_relaxed) != accounted)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->total_bytes != allocated + nullmaps + maps_bytes ({} != {})",
            data->total_bytes.load(std::memory_order_relaxed),
            accounted);
#endif
}

size_t HashJoin::getTotalByteCountUnchecked() const
{
    if (!data)
        return 0;

    return data->total_bytes.load(std::memory_order_relaxed);
}

bool HashJoin::isUsedByAnotherAlgorithm() const
{
    return JoinCommon::isUsedByAnotherAlgorithm(*table_join);
}

bool HashJoin::canRemoveColumnsFromLeftBlock() const
{
    return JoinCommon::canRemoveColumnsFromLeftBlock(*table_join);
}

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = isUsedByAnotherAlgorithm() ||
                            isRightOrFull(kind) ||
                            multiple_disjuncts ||
                            table_join->getMixedJoinExpression();

    if (save_key_columns)
    {
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (strictness == JoinStrictness::Asof)
    {
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    for (auto & column : sample_block_with_columns_to_add)
    {
        if (auto * col = saved_block_sample.findByName(column.name))
            *col = column;
        else
            saved_block_sample.insert(column);
    }
}

void HashJoin::materializeColumnsFromLeftBlock(Block & block) const
{
    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if (kind == JoinKind::Right || kind == JoinKind::Full)
    {
        materializeBlockInplace(block);
    }
}

Block HashJoin::materializeColumnsFromRightBlock(Block block) const
{
    return JoinCommon::materializeColumnsFromRightBlock(std::move(block), savedBlockSample());
}

void HashJoin::initRowStore(const Block & block)
{
    /// Skip initializing if it's already initialized or disabled.
    if (data->row_store_state != RowStoreState::Enabled)
        return;

    /// Extract columns suitable for row store.
    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    const auto & columns = block_to_save.getColumns();
    const auto types = block_to_save.getDataTypes();
    ColumnAccessIndexes access_indexes;
    access_indexes.reserve(columns.size());
    Columns row_store_columns;
    DataTypes row_store_types;
    size_t remaining_columns = 0;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (isRowStorageUseful(columns[i]))
        {
            access_indexes.push_back({ColumnAccessIndex::Type::RowStore, row_store_columns.size()});
            row_store_columns.push_back(columns[i]);
            row_store_types.push_back(types[i]);
        }
        else
            access_indexes.push_back({ColumnAccessIndex::Type::Columns, remaining_columns++});
    }

    /// Disable row store if it would be built from a single column.
    if (row_store_columns.size() <= 1)
    {
        data->row_store_state = RowStoreState::Disabled;
        return;
    }

    /// Add each field's offset, size and nullability to the row store access indexes.
    RowDataStore::RowLayoutPtr layout = RowDataStore::computeLayout(row_store_columns, row_store_types);
    for (auto & access_index : access_indexes)
    {
        if (access_index.type != ColumnAccessIndex::Type::RowStore)
            continue;
        const auto & field = (*layout)[access_index.index];
        access_index.field_offset = field.offset;
        access_index.field_size = field.size;
        access_index.is_nullable = field.is_nullable;
    }

    data->row_store_layout = std::move(layout);
    data->column_access_indexes = std::move(access_indexes);
    data->row_store_state = RowStoreState::Initialized;

    LOG_DEBUG(log, "Initialized Row store with {} columns", row_store_columns.size());
}

StoredBlock
HashJoin::createStoredBlock(const Block & block_to_save, ScatteredBlock::Selector selector, RowDataStorePtr row_store) const
{
    if (data->row_store_state != RowStoreState::Initialized)
        return StoredBlock(block_to_save.getColumns(), std::move(selector), std::move(row_store));

    auto [row_store_columns, remaining_columns] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
    if (!row_store)
        row_store = RowDataStore::create(data->row_store_layout, row_store_columns);
    return StoredBlock(std::move(remaining_columns), std::move(selector), std::move(row_store));
}

Block HashJoinTypes::prepareRightBlock(const Block & block, const Block & saved_block_sample_)
{
    Block prepared_block = JoinCommon::materializeColumnsFromRightBlock(block, saved_block_sample_);
    return filterColumnsPresentInSampleBlock(prepared_block, saved_block_sample_);
}

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareRightBlock(block, savedBlockSample());
}

void HashJoin::compactStoredColumns(size_t & total_bytes_in_join)
{
    const auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    const Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(current_memory_usage));

    /// `cloneResized` replaces the column objects, so an emit table's raw pointers and those in
    /// `replicated_columns` go stale. A throw part-way through still leaves some replaced.
    bool any_column_replaced = false;
    SCOPE_EXIT({
        if (any_column_replaced)
            data->stored_columns_index->invalidateEmitTable();
    });

    for (auto & stored_columns : data->columns)
    {
        size_t old_size = stored_columns.allocatedBytes();

        try
        {
            for (auto & column : stored_columns.columns)
            {
                column = column->cloneResized(column->size());
                any_column_replaced = true;
            }

            stored_columns.rebuildReplicatedColumns();
        }
        catch (...)
        {
            stored_columns.rebuildReplicatedColumns();
            size_t partial_new_size = stored_columns.allocatedBytes();
            if (old_size >= partial_new_size)
                data->subBytes(data->allocated_size, old_size - partial_new_size);
            else
                data->addBytes(data->allocated_size, partial_new_size - old_size);
            throw;
        }

        size_t new_size = stored_columns.allocatedBytes();

        if (old_size >= new_size)
        {
            if (data->allocated_size.load(std::memory_order_relaxed) < old_size - new_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Blocks allocated size value is broken: "
                    "blocks_allocated_size = {}, old_size = {}, new_size = {}",
                    data->allocated_size.load(std::memory_order_relaxed),
                    old_size,
                    new_size);

            data->subBytes(data->allocated_size, old_size - new_size);
        }
        else
            /// Sometimes after clone resized block can be bigger than original
            data->addBytes(data->allocated_size, new_size - old_size);
    }

    auto new_total_bytes_in_join = getTotalByteCountUnchecked();
    Int64 new_current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrunk stored blocks {} freed ({} by memory tracker), new memory consumption is {} ({} by memory tracker)",
        ReadableSize(total_bytes_in_join - new_total_bytes_in_join),
        ReadableSize(current_memory_usage - new_current_memory_usage),
        ReadableSize(new_total_bytes_in_join),
        ReadableSize(new_current_memory_usage));

    total_bytes_in_join = new_total_bytes_in_join;
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    if (flag_per_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin with ORs is not supported");

    const auto maps_kind = getMapsKind();
    for (auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            maps_kind,
            [this](auto kind_, auto strictness_, auto & map_)
            {
                used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map_)>()>(
                    map_.getBufferSizeInCells(data->type) + 1);
            });
    }

    used_flags->setUnsetOffsetCount(data->keys_to_join.load(std::memory_order_relaxed));
}

const ColumnWithTypeAndName & HashJoin::rightAsofKeyColumn() const
{
    /// It should be nullable when right side is nullable
    return savedBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

void HashJoin::validateAdditionalFilterExpression(ExpressionActionsPtr additional_filter_expression)
{
    if (!additional_filter_expression)
        return;

    Block expression_sample_block = additional_filter_expression->getSampleBlock();

    if (expression_sample_block.columns() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected single column, got '{}', expression:\n{}",
            expression_sample_block.dumpStructure(),
            additional_filter_expression->dumpActions());
    }

    auto type = removeNullable(expression_sample_block.getByPosition(0).type);
    if (!type->equals(*std::make_shared<DataTypeUInt8>()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected boolean (UInt8), got '{}'. expression:\n{}",
            expression_sample_block.getByPosition(0).type->getName(),
            additional_filter_expression->dumpActions());
    }

    bool is_supported = ((strictness == JoinStrictness::All) && (isInnerOrLeft(kind) || isRightOrFull(kind)))
        || ((strictness == JoinStrictness::Semi || strictness == JoinStrictness::Any || strictness == JoinStrictness::Anti)
            && (isLeft(kind) || isRight(kind)))
        || (strictness == JoinStrictness::Any && (isInner(kind)));

    if (!is_supported)
    {
        throw Exception(
            ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "Non equi condition '{}' from JOIN ON section is supported only for ALL INNER/LEFT/FULL/RIGHT JOINs",
            expression_sample_block.getByPosition(0).name);
    }

    /// `arrayJoin` changes the number of rows, but `buildAdditionalFilter` evaluates this expression
    /// per probe batch and `joinRightColumnsWithAdditionalFilter` indexes the result by row position,
    /// so the expression must preserve the number of rows.
    if (additional_filter_expression->hasArrayJoin())
    {
        throw Exception(
            ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "Non equi condition '{}' from JOIN ON section contains 'arrayJoin', which changes the number of rows. "
            "If the expansion depends on one side only, use ARRAY JOIN in a subquery before the JOIN",
            expression_sample_block.getByPosition(0).name);
    }
}

bool HashJoin::isUsed(size_t off) const
{
    return used_flags->getUsedSafe(off);
}

bool HashJoin::isUsed(UInt32 block_no, size_t row_idx) const
{
    return used_flags->getUsedSafe(block_no, row_idx);
}

bool HashJoin::needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const
{
    if (!table_join_->oneDisjunct())
        return true;
    /// If it'a a all right join with inequal conditions, we need to mark each row
    if (table_join_->getMixedJoinExpression() && isRightOrFull(table_join_->kind()))
        return true;
    return false;
}

void HashJoin::reinitUsedFlagsForMaps()
{
    if (needUsedFlagsForPerRightTableRow(table_join))
        return;

    const auto maps_kind = getMapsKind();
    for (auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            maps_kind,
            [this](auto kind_, auto strictness_, auto & map_)
            {
                used_flags->reinitAllowShrinking<kind_, strictness_, mapsKindOf<decltype(map_)>()>(
                    map_.getBufferSizeInCells(data->type) + 1);
            });
    }

    used_flags->setUnsetOffsetCount(data->keys_to_join.load(std::memory_order_relaxed));
}

bool HashJoin::isRowStoreSupported() const
{
    /// ANY joins materialize eagerly and doesn't run the batched fill the row store accelerates.
    return kind != JoinKind::Cross
        && strictness != JoinStrictness::Any
        && !table_join->getClauses().empty()
        && !table_join->getMixedJoinExpression();
}

bool HashJoin::recordsRowRefsForStats() const
{
    return table_join->collectExactMatches() && table_join->getMixedJoinExpression() == nullptr;
}

void HashJoin::finishMapsBuild()
{
    ProfileEventTimeIncrement<Microseconds> build_watch(ProfileEvents::HashJoinBuildMicroseconds);

    reinitUsedFlagsForMaps();

    if (all_values_unique && strictness == JoinStrictness::All && isInnerOrLeft(kind) && data->maps.size() == 1)
    {
        strictness = JoinStrictness::RightAny;
        all_join_was_promoted_to_right_any = true;
        LOG_DEBUG(log, "Promoting join strictness to RightAny, because all values in the right table are unique");
    }

    doDebugAsserts();
    LOG_TRACE(log, "Join data is built, {} and {} rows in hash table", ReadableSize(getTotalByteCountUnchecked()), getKeysToJoin());
}

}
