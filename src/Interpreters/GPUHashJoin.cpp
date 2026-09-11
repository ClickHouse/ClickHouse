#include <Interpreters/GPUHashJoin.h>

#if USE_GPU

#include <GPU/GPUAggregation.h>
#include <GPU/GPUAggregationABI.h>

#include <Columns/ColumnsNumber.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <mutex>
#include <utility>

namespace ProfileEvents
{
    extern const Event GPUJoinBuildRows;
    extern const Event GPUJoinProbeRows;
    extern const Event GPUJoinMatchedRows;
    extern const Event GPUJoinMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int GPU_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

/// Room for a message coming back over the boundary. cuDF's are a line or two.
constexpr size_t error_buffer_size = 1024;

/// Whether the device would compare two keys of this element type the way ClickHouse does.
///
/// Only for the integers. ClickHouse compares a `Float64` key by its eight bytes, so `0.0` and
/// `-0.0` are different keys and two `NaN`s with different payloads are too; cuDF compares float
/// keys with IEEE equality extended to make `NaN` equal to itself, so it makes one key of the first
/// pair and one of the second. Either difference changes which rows join, which is a wrong answer
/// rather than a slower one - the same reason floats are not eligible as `GROUP BY` keys, see
/// `GPU::canGroupBySumOnDevice`.
bool isIntegerElementType(int element_type)
{
    switch (element_type)
    {
        case CLICKHOUSE_GPU_ELEMENT_UINT8:
        case CLICKHOUSE_GPU_ELEMENT_UINT16:
        case CLICKHOUSE_GPU_ELEMENT_UINT32:
        case CLICKHOUSE_GPU_ELEMENT_UINT64:
        case CLICKHOUSE_GPU_ELEMENT_INT8:
        case CLICKHOUSE_GPU_ELEMENT_INT16:
        case CLICKHOUSE_GPU_ELEMENT_INT32:
        case CLICKHOUSE_GPU_ELEMENT_INT64:
            return true;
        default:
            return false;
    }
}

/// Whether every column of `block` is one of the ten fixed-width numeric types the boundary carries.
bool everyColumnIsFixedWidthNumeric(const Block & block)
{
    for (const auto & column : block)
    {
        if (!GPU::elementTypeOf(*column.type))
            return false;
    }

    return true;
}

/// `key_names_right` may name the same column twice - `JOIN ON t1.a = t2.k AND t1.b = t2.k` - and
/// `JoinCommon::splitAdditionalColumns` deduplicates on the way, which is why the key block it
/// fills is read back rather than assumed to have one column per key name.
Block rightTableKeysOf(const TableJoin & table_join, const Block & right_sample_block, Block & payload)
{
    Block keys;
    JoinCommon::splitAdditionalColumns(table_join.getOnlyClause().key_names_right, right_sample_block, keys, payload);
    return keys;
}

}

bool GPUHashJoin::isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block)
{
    /// `ALL INNER` only. Every other kind needs a second pass over rows that did not match - the
    /// unmatched left rows of a `LEFT JOIN`, the unmatched right rows of a `RIGHT JOIN` - and every
    /// other strictness needs the matches per key reduced to one, or counted, or compared. cuDF has
    /// `left_join` and `full_join` next to `inner_join`, so the kinds are the smaller half of that
    /// work; the strictnesses are not.
    if (table_join.kind() != JoinKind::Inner || table_join.strictness() != JoinStrictness::All)
        return false;

    /// One `ON` clause of one equality and nothing else. Several disjuncts (`ON a = b OR c = d`), a
    /// filter condition on one side, and a residual mixed condition (`ON a = b AND l.x > r.y`) each
    /// need a mechanism of their own on top of the join.
    if (!table_join.oneDisjunct() || table_join.getMixedJoinExpression())
        return false;

    const auto & clause = table_join.getOnlyClause();
    if (clause.key_names_left.size() != 1 || clause.key_names_right.size() != 1)
        return false;
    if (clause.on_filter_condition_left || clause.on_filter_condition_right)
        return false;

    /// A `Join`-engine or key-value right side arrives already built and has its own join
    /// implementation; this one fills its hash table from a stream of blocks.
    if (table_join.isSpecialStorage())
        return false;

    if (!left_sample_block.has(clause.key_names_left[0]) || !right_sample_block.has(clause.key_names_right[0]))
        return false;

    const auto & left_key_type = left_sample_block.getByName(clause.key_names_left[0]).type;
    const auto & right_key_type = right_sample_block.getByName(clause.key_names_right[0]).type;

    /// The device compares the two keys as values of one element type, so the two sides have to
    /// agree on that type exactly. The planner's converting actions normally cast both to a common
    /// type before an algorithm is picked, so a pair still different here is one they could not
    /// unify - `UInt64` against `Int64`, which have no common type that holds both - and casting it
    /// here would be this algorithm quietly answering a different question.
    if (!left_key_type->equals(*right_key_type))
        return false;

    const auto key_element_type = GPU::elementTypeOf(*left_key_type);
    if (!key_element_type || !isIntegerElementType(*key_element_type))
        return false;

    /// Every column of either side has to be a fixed-width numeric one. The right side's are sent
    /// over, gathered on the device and read back, and the left side's are indexed on the host - and
    /// while `IColumn::index` would handle any column at all, keeping both sides to the same set of
    /// types keeps one eligibility rule instead of two. Floats are fine here: a payload column is
    /// carried, never compared.
    return everyColumnIsFixedWidthNumeric(left_sample_block) && everyColumnIsFixedWidthNumeric(right_sample_block);
}

GPUHashJoin::GPUHashJoin(
    std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_)
    : table_join(std::move(table_join_))
    , left_sample_block(*left_sample_block_)
    , right_sample_block(*right_sample_block_)
    , key_name_left(table_join->getOnlyClause().key_names_left[0])
    , key_name_right(table_join->getOnlyClause().key_names_right[0])
    , log(getLogger("GPUHashJoin"))
{
    /// `isSupported` is asked before this is constructed, so a join that does not fit is a mistake
    /// in the caller rather than an unsupported query.
    if (!isSupported(*table_join, left_sample_block, right_sample_block))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot execute this {} {} JOIN on a GPU",
            toString(table_join->strictness()),
            toString(table_join->kind()));

    const Block right_table_keys = rightTableKeysOf(*table_join, right_sample_block, build_payload_header);
    required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);

    /// A column of the right table the query asks for but the right side never produced - it
    /// cannot happen for this join, whose right header is the right side's own output, but the
    /// check costs nothing and the alternative is a null column reaching `assembleOutputBlock`.
    JoinCommon::createMissedColumns(build_payload_header);

    key_element_type = *GPU::elementTypeOf(*right_sample_block.getByName(key_name_right).type);
    key_element_size = GPU::elementSizeOf(key_element_type);

    payload_element_types.reserve(build_payload_header.columns());
    payload_element_sizes.reserve(build_payload_header.columns());
    for (const auto & column : build_payload_header)
    {
        payload_element_types.push_back(*GPU::elementTypeOf(*column.type));
        payload_element_sizes.push_back(GPU::elementSizeOf(payload_element_types.back()));
    }

    char error[error_buffer_size] = {};
    const int status = clickhouseGPUHashJoinCreate(
        key_element_type,
        payload_element_types.data(),
        payload_element_types.size(),
        &handle,
        error,
        sizeof(error));

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot set up a hash join on the device: {}", error);

    LOG_TRACE(
        log,
        "Joining on {} with {} column(s) of the right table on a GPU",
        right_sample_block.getByName(key_name_right).type->getName(),
        build_payload_header.columns());
}

GPUHashJoin::~GPUHashJoin()
{
    clickhouseGPUHashJoinDestroy(handle);
}

void GPUHashJoin::checkTypesOfKeys(const Block & block) const
{
    JoinCommon::checkTypesOfKeys(
        block, table_join->getOnlyClause().key_names_left, right_sample_block, table_join->getOnlyClause().key_names_right);
}

bool GPUHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    const size_t num_rows = block.rows();

    std::lock_guard lock(device_mutex);

    if (build_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of the right table arrived after the GPU hash table was built");

    /// An empty block carries no rows to join on, and the boundary refuses one - so it is dropped
    /// here rather than sent.
    if (num_rows == 0)
        return true;

    /// The columns are made full and held for the length of the call: the device copies from their
    /// own memory, so nothing is staged in host memory on the way - which is the point, since a
    /// host-to-host copy of a column costs about as much as sending it.
    Columns held;
    held.reserve(build_payload_header.columns() + 1);

    const auto rawValuesToSend = [&](const String & name, size_t element_size)
    {
        held.push_back(block.getByName(name).column->convertToFullIfWrapped());
        return GPU::rawValuesOf(*held.back(), num_rows, element_size).data();
    };

    const void * key_data = rawValuesToSend(key_name_right, key_element_size);

    std::vector<const void *> payload_data(build_payload_header.columns());
    for (size_t i = 0; i < payload_data.size(); ++i)
        payload_data[i] = rawValuesToSend(build_payload_header.getByPosition(i).name, payload_element_sizes[i]);

    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status
        = clickhouseGPUHashJoinAddBuildBlock(handle, key_data, payload_data.data(), num_rows, error, sizeof(error));
    const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();

    if (status != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Cannot send {} rows of the right table to the device: {}", num_rows, error);

    ProfileEvents::increment(ProfileEvents::GPUJoinBuildRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, elapsed_microseconds);

    size_t row_bytes = key_element_size;
    for (const size_t element_size : payload_element_sizes)
        row_bytes += element_size;

    build_rows.fetch_add(num_rows, std::memory_order_relaxed);
    build_bytes.fetch_add(num_rows * row_bytes, std::memory_order_relaxed);

    if (!check_limits)
        return true;

    /// The right table lives in device memory, which no memory limit of the server's can see - so
    /// `max_rows_in_join` and `max_bytes_in_join` are the only bound on it, and they are counted
    /// here against the bytes it occupies there rather than against anything on the host.
    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void GPUHashJoin::finishBuildUnlocked()
{
    if (build_finished)
        return;

    build_finished = true;

    char error[error_buffer_size] = {};

    Stopwatch watch;
    const int status = clickhouseGPUHashJoinFinishBuild(handle, error, sizeof(error));
    ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());

    if (status != 0)
        throw Exception(
            ErrorCodes::GPU_ERROR,
            "Cannot build a hash table over {} rows of the right table on the device: {}",
            getTotalRowCount(),
            error);
}

void GPUHashJoin::onBuildPhaseFinish()
{
    std::lock_guard lock(device_mutex);
    finishBuildUnlocked();
}

Block GPUHashJoin::assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, Columns gathered_payloads) const
{
    /// The left table's columns, indexed on the host with the row indices the device handed back.
    /// This is the half of the join that never crosses the link: of the left block only the key
    /// column went over, and what came back was one index per matching pair.
    Block probe_part;
    for (const auto & column : probe_block)
    {
        ColumnWithTypeAndName output = column;
        output.column = column.column->index(*probe_indices, 0);
        probe_part.insert(std::move(output));
    }

    Block result = probe_part;

    /// Then the right table's payload columns, then the right key columns the query asks for. That
    /// is the order `HashJoin` produces - `HashJoinResult::generateBlock` inserts the columns the
    /// join adds first and `required_right_keys` after them - and this join follows it so that a
    /// plan reads the same whichever algorithm ran.
    ///
    /// The order is this join's own to choose, though, and what actually has to hold is only that
    /// it be the same for every block including the empty one: `JoinStep::updateOutputHeader` takes
    /// the step's output header to be whatever `JoiningTransform::transformHeader` gets back, and
    /// that is this function over the left header. A `ColumnPermuteTransform` right after the join
    /// then puts the columns into the order the rest of the query wants and drops the ones it does
    /// not, by name - see `getPermutationForBlock` in JoinStep.cpp, which is where this was
    /// verified.
    for (size_t i = 0; i < build_payload_header.columns(); ++i)
    {
        const auto & sample = build_payload_header.getByPosition(i);
        result.insert({gathered_payloads[i], sample.type, table_join->renamedRightColumnName(sample.name)});
    }

    for (size_t i = 0; i < required_right_keys.columns(); ++i)
    {
        const auto & right_key = required_right_keys.getByPosition(i);
        const ColumnWithTypeAndName & left_key = probe_part.getByName(required_right_keys_sources[i]);

        /// An `INNER JOIN` on equality makes a matched pair's two keys equal, and `isSupported`
        /// accepted this join only because the two key types are the same one - so the right key
        /// column is the left key column, and no part of it has to come back from the device. A
        /// type that is not the same one after all would mean the values are equal as the join
        /// compared them but not as this column claims to hold them.
        if (!right_key.type->equals(*left_key.type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The GPU hash join joined {} of {} to {} of {}",
                left_key.name,
                left_key.type->getName(),
                right_key.name,
                right_key.type->getName());

        result.insert(
            {left_key.column->convertToFullColumnIfConst(), right_key.type, table_join->renamedRightColumnName(right_key.name)});
    }

    return result;
}

JoinResultPtr GPUHashJoin::joinBlock(Block block)
{
    const size_t probe_rows = block.rows();
    probe_rows_total.fetch_add(probe_rows, std::memory_order_relaxed);

    /// The output's columns, empty until the device says how many matches there are. They are the
    /// columns the query returns, not staging buffers: the device copies its gathered payload
    /// straight into them.
    auto probe_index_column = ColumnUInt32::create();
    MutableColumns payload_columns;
    payload_columns.reserve(build_payload_header.columns());
    for (const auto & column : build_payload_header)
        payload_columns.push_back(column.type->createColumn());

    /// An empty left block has an empty result, and the device is not asked about it. The left
    /// header at planning time is such a block - that is how `JoinStep` learns this step's output
    /// header - so this is also the path that must produce exactly the structure every real block
    /// will have. It does, because the structure is `assembleOutputBlock`'s alone.
    if (probe_rows != 0)
    {
        std::lock_guard lock(device_mutex);

        /// Normally already done by `onBuildPhaseFinish`; done here for the pipelines that never
        /// call it, so that a probe can never run against a half-built hash table.
        finishBuildUnlocked();

        if (getTotalRowCount() != 0)
        {
            /// Held for the length of the two calls below: the device copies the keys out of this
            /// column's own memory.
            const ColumnPtr probe_key = block.getByName(key_name_left).column->convertToFullIfWrapped();
            const void * key_data = GPU::rawValuesOf(*probe_key, probe_rows, key_element_size).data();

            char error[error_buffer_size] = {};
            size_t num_matches = 0;

            Stopwatch watch;
            int status = clickhouseGPUHashJoinProbe(handle, key_data, probe_rows, &num_matches, error, sizeof(error));

            if (status != 0)
                throw Exception(
                    ErrorCodes::GPU_ERROR, "Cannot probe the device's hash table with {} rows of the left table: {}", probe_rows, error);

            if (num_matches != 0)
            {
                std::vector<void *> payload_data(payload_columns.size());
                for (size_t i = 0; i < payload_columns.size(); ++i)
                    payload_data[i] = GPU::resizeForElementType(*payload_columns[i], num_matches, payload_element_types[i]);

                probe_index_column->getData().resize(num_matches);

                status = clickhouseGPUHashJoinCopyProbeResultOut(
                    handle, probe_index_column->getData().data(), payload_data.data(), error, sizeof(error));

                if (status != 0)
                    throw Exception(
                        ErrorCodes::GPU_ERROR, "Cannot copy {} joined rows back from the device: {}", num_matches, error);
            }

            ProfileEvents::increment(ProfileEvents::GPUJoinMicroseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::GPUJoinProbeRows, probe_rows);
            ProfileEvents::increment(ProfileEvents::GPUJoinMatchedRows, num_matches);
        }
    }

    /// `IColumn::index` reads `data[index]` without a bounds check of its own, so this is the one
    /// place between what the device produced and how the host addresses memory with it where the
    /// indices can be checked - and they have to be, because everything past this point trusts
    /// them. An index out of range is a mistake in cuDF or here and not something a query can
    /// cause, hence `LOGICAL_ERROR`. The test also covers the device's indices being signed: a
    /// negative one read as a `UInt32` lands above 2^31 and so above any block's row count.
    for (const UInt32 index : probe_index_column->getData())
    {
        if (index >= probe_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The device returned row index {} for a block of {} rows of the left table",
                index,
                probe_rows);
    }

    Columns gathered_payloads;
    gathered_payloads.reserve(payload_columns.size());
    for (auto & column : payload_columns)
        gathered_payloads.push_back(std::move(column));

    return IJoinResult::createFromBlock(assembleOutputBlock(block, std::move(probe_index_column), std::move(gathered_payloads)));
}

StepAnalysisReport GPUHashJoin::getAnalysisReport() const
{
    StepAnalysisReport report;
    report.push_back({MetricGroupKey::Left, joinSideMetrics(probe_rows_total.load(std::memory_order_relaxed), std::nullopt)});
    report.push_back({MetricGroupKey::Right, joinSideMetrics(getTotalRowCount(), std::nullopt)});
    return report;
}

}

#endif
