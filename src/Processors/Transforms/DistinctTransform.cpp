#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NullableUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

namespace ProfileEvents
{
    extern const Event DistinctTransformsAbandonedDeduplication;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Retained set bytes added since the previous output chunk. The chunk's rows are the new keys.
struct DistinctSetSizeDelta : public ChunkInfoCloneable<DistinctSetSizeDelta>
{
    explicit DistinctSetSizeDelta(UInt64 bytes_)
        : bytes(bytes_)
    {
    }
    UInt64 bytes;
};

/// Mark rows whose `LowCardinality` index is the dictionary's NULL entry with 0 in `keep`, allocating
/// the filter lazily on the first such row.
void markLowCardinalityNullRows(const ColumnLowCardinality & column, IColumn::Filter & keep, size_t num_rows)
{
    const size_t null_index = column.getDictionary().getNullValueIndex();
    const IColumn & indexes_column = *column.getIndexesPtr();

    auto process = [&](const auto & indexes)
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (static_cast<size_t>(indexes[row]) == null_index)
            {
                if (keep.empty())
                    keep.assign(num_rows, static_cast<UInt8>(1));
                keep[row] = 0;
            }
        }
    };

    switch (column.getSizeOfIndexType())
    {
        case sizeof(UInt8): process(assert_cast<const ColumnUInt8 &>(indexes_column).getData()); break;
        case sizeof(UInt16): process(assert_cast<const ColumnUInt16 &>(indexes_column).getData()); break;
        case sizeof(UInt32): process(assert_cast<const ColumnUInt32 &>(indexes_column).getData()); break;
        case sizeof(UInt64): process(assert_cast<const ColumnUInt64 &>(indexes_column).getData()); break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }
}

}

void LCOptimizationController::update(size_t num_rows, size_t new_indices_in_chunk)
{
    if (state != State::Observing)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    new_indices_observed += new_indices_in_chunk;

    if (chunks_observed >= OBSERVATION_CHUNK_COUNT)
    {
        double new_index_rate = static_cast<double>(new_indices_observed) / static_cast<double>(rows_observed);

        /// Disable when the mask is almost a no-op: nearly every row introduces
        /// a new dictionary index, so the bitmap bookkeeping is pure overhead.
        if (new_index_rate >= NEW_INDEX_RATE_THRESHOLD)
            state = State::Disabled;
        else
            state = State::Enabled;
    }
}

void DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    if (abandoned)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    abandoned = unique_rate >= UNIQUE_RATE_THRESHOLD;
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool allow_abandoning_,
    bool skip_null_keys_,
    bool report_set_size_)
    : ISimpleTransform(header_, header_, true)
    , key_columns_pos(getNonConstantKeyColumnPositions(*header_, columns_))
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , report_set_size(report_set_size_)
    , skip_null_keys(skip_null_keys_)
{
    chassert(!report_set_size || (!allow_abandoning_ && !set_size_limits.hasLimits()));
    if (allow_abandoning_)
        abandon_controller.emplace();

    if (skip_null_keys)
    {
        for (const auto & name : columns_.empty() ? header_->getNames() : columns_)
        {
            const auto & column = header_->getByName(name).column;
            if (column && isColumnConst(*column) && column->isNullAt(0))
            {
                const_null_key = true;
                break;
            }
        }
    }
}

ColumnNumbers DistinctTransform::getNonConstantKeyColumnPositions(const Block & header, const Names & columns)
{
    const size_t num_columns = columns.empty() ? header.columns() : columns.size();
    ColumnNumbers positions;
    positions.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const size_t position = columns.empty() ? i : header.getPositionByName(columns[i]);
        const auto & column = header.getByPosition(position).column;
        if (column && !isColumnConst(*column))
            positions.push_back(position);
    }
    return positions;
}

template <typename Method>
void DistinctTransform::buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumn::Filter & filter,
    const size_t rows,
    SetVariants & variants,
    const IColumn::Filter * mask) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (!(*mask)[i])
            {
                /// Already known duplicate row (by LC index), skip insertion
                filter[i] = 0;
                continue;
            }

            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            filter[i] = emplace_result.isInserted();
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        }
    }
}

std::pair<IColumn::Filter, size_t> DistinctTransform::buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows)
{
    const auto & dictionary = column.getDictionary();
    const auto dict_size = dictionary.size();

    LCDictionaryKey dict_key;
    dict_key.hash = dictionary.getHash();
    dict_key.size = dict_size;

    auto & state = lc_dict_states[dict_key];

    /// The first time we see this dictionary, initialize the seen_indices array to keep track which entries
    /// in the dictionary have been seen.
    chassert(state.seen_count <= dict_size);
    if (state.seen_indices.size() != dict_size)
    {
        chassert(state.seen_indices.empty());
        chassert(state.seen_count == 0);
        state.seen_indices.resize_fill(dict_size);
    }

    /// If we've already seen all dictionary indices for this dictionary,
    /// then no row in this chunk (and also other chunks with the same dictionary) can produce a new distinct value.
    if (state.seen_count == dict_size)
        return {{}, 0}; /// empty mask == no candidates

    const auto seen_count_before = state.seen_count;
    auto & seen = state.seen_indices;

    const auto index_type_size = column.getSizeOfIndexType();
    const IColumn & indexes_column = *column.getIndexesPtr();

    IColumn::Filter mask;

    auto handle_index = [&](size_t idx, size_t row)
    {
        chassert(idx < dict_size);
        if (!seen[idx])
        {
            seen[idx] = 1;
            ++state.seen_count;

            if (mask.empty())
                mask.resize_fill(num_rows);

            mask[row] = 1; /// first time we see this dictionary index for this dictionary
        }
    };

    switch (index_type_size)
    {
        case sizeof(UInt8):
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt16):
        {
            const auto & col = assert_cast<const ColumnUInt16 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt32):
        {
            const auto & col = assert_cast<const ColumnUInt32 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt64):
        {
            const auto & col = assert_cast<const ColumnUInt64 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }

    return {std::move(mask), state.seen_count - seen_count_before};
}

void DistinctTransform::maybeAbandonDeduplication(size_t num_rows, size_t num_unique_rows)
{
    if (!abandon_controller)
        return;

    abandon_controller->update(num_rows, num_unique_rows, data->getTotalByteCount());
    if (abandon_controller->isAbandoned())
    {
        data.reset();
        lc_dict_states.clear();
        ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    if (abandon_controller && abandon_controller->isAbandoned())
        return;

    if (const_null_key)
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// Special case, - only const columns, return single row
    if (unlikely(key_columns_pos.empty()))
    {
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        if (report_set_size)
            chunk.getChunkInfos().add(std::make_shared<DistinctSetSizeDelta>(0));
        stopReading();
        return;
    }

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    /// The consumer skips rows with a NULL in any key component (a set fill with
    /// `transform_null_in = 0` strips `LowCardinality` and then drops such rows), so they carry no
    /// value downstream: drop them before deduplication and before the abandon accounting. Plain
    /// `Nullable` keys are then hashed by their nested columns, the same way the set fill hashes them.
    ColumnPtr null_map_holder;
    if (skip_null_keys)
    {
        ConstNullMapPtr null_map = nullptr;
        null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);

        IColumn::Filter keep;
        if (null_map && !memoryIsZero(null_map->data(), 0, num_rows))
        {
            keep.resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                keep[i] = !(*null_map)[i];
        }

        for (const auto * column : column_ptrs)
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
                low_cardinality && low_cardinality->nestedIsNullable())
                markLowCardinalityNullRows(*low_cardinality, keep, num_rows);

        if (!keep.empty())
        {
            const auto num_kept = countBytesInFilter(keep);
            for (auto & column : columns)
                column = column->filter(keep, num_kept);
            num_rows = num_kept;

            if (num_rows == 0)
            {
                chunk.setColumns(std::move(columns), 0);
                return;
            }

            column_ptrs.clear();
            for (auto pos : key_columns_pos)
                column_ptrs.emplace_back(columns[pos].get());
            null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);
        }
    }

    std::optional<IColumn::Filter> lc_mask;

    if (lc_optimization_controller.isEnabled() && key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            auto [mask, new_indices_count] = buildLowCardinalityMask(*lc, num_rows);
            lc_optimization_controller.update(num_rows, new_indices_count);
            lc_mask.emplace(std::move(mask));

            /// Empty mask -> no candidate rows in this chunk, emit nothing. The chunk is fully
            /// duplicate, which is the strongest evidence in favor of keeping the deduplication, so
            /// the abandon accounting must see it.
            if (lc_mask->empty())
            {
                maybeAbandonDeduplication(num_rows, 0);
                return;
            }
        }
    }

    if (data->empty())
        data->init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    const auto old_set_size = data->getTotalRowCount();
    IColumn::Filter filter(num_rows);

    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildFilter(*data->NAME, column_ptrs, filter, num_rows, *data, lc_mask ? &*lc_mask : nullptr); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    const auto new_set_size = data->getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    maybeAbandonDeduplication(num_rows, num_selected);

    /// Just go to the next chunk if there isn't any new record in the current one.
    if (num_selected == 0)
        return;

    /// Only retained allocations count towards the byte limit; preliminary deduplication may have
    /// released its set. Parallel final transforms keep disjoint sets whose sizes add up.
    const auto new_set_bytes = data ? data->getTotalByteCount() : 0;
    if (report_set_size)
    {
        chassert(new_set_bytes >= reported_set_bytes);
        chunk.getChunkInfos().add(std::make_shared<DistinctSetSizeDelta>(new_set_bytes - reported_set_bytes));
        reported_set_bytes = new_set_bytes;
    }
    else if (!set_size_limits.check(new_set_size, new_set_bytes, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
    {
        /// In `BREAK` mode, `SizeLimits::check` returns false instead of throwing. The new keys from this
        /// chunk are already in the set, so emit their rows before stopping. This returns a partial result
        /// as if the input had run out, including the chunk that reached the limit.
        stopReading();
    }

    if (num_selected == num_rows)
    {
        /// Every row is a new distinct value: keep the chunk unchanged, without copying it.
        chunk.setColumns(std::move(columns), num_rows);
    }
    else
    {
        for (auto & column : columns)
            column = column->filter(filter, -1);

        chunk.setColumns(std::move(columns), num_selected);
    }

    /// Stop reading if we already reach the limit
    if (limit_hint && new_set_size >= limit_hint)
        stopReading();
}

DistinctLimitTransform::DistinctLimitTransform(const SharedHeader & header, const SizeLimits & size_limits_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header), OutputPorts(num_streams, header))
    , size_limits(size_limits_)
{
    port_pairs.reserve(num_streams);
    port_to_pair.reserve(2 * num_streams);
    auto output = outputs.begin();
    for (auto & input : inputs)
    {
        auto & pair = port_pairs.emplace_back(input, *output++);
        port_to_pair.emplace(&pair.input, &pair);
        port_to_pair.emplace(&pair.output, &pair);
    }
}

IProcessor::Status DistinctLimitTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    bool has_full_port = false;
    auto prepare_ports = [&](const auto & updated_ports)
    {
        for (const auto * port : updated_ports)
        {
            /// `BREAK` emits the chunk that reaches the limit and stops processing further port updates.
            if (limit_reached)
                break;

            auto & pair = *port_to_pair.at(port);
            const auto status = preparePair(pair);
            if (status == Status::Finished && !pair.is_finished)
            {
                pair.is_finished = true;
                ++num_finished_port_pairs;
            }
            has_full_port |= status == Status::PortFull;
        }
    };

    prepare_ports(updated_inputs);
    prepare_ports(updated_outputs);

    if (limit_reached)
    {
        for (auto & input : inputs)
            input.close();
        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    if (num_finished_port_pairs == port_pairs.size())
        return Status::Finished;

    return has_full_port ? Status::PortFull : Status::NeedData;
}

IProcessor::Status DistinctLimitTransform::prepare()
{
    chassert(port_pairs.size() == 1);
    return prepare({&port_pairs.front().input}, {&port_pairs.front().output});
}

IProcessor::Status DistinctLimitTransform::preparePair(PortPair & pair)
{
    auto & input = pair.input;
    auto & output = pair.output;

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto data_chunk = input.pullData(true);
    if (data_chunk.chunk.hasRows())
    {
        auto set_size_delta = data_chunk.chunk.getChunkInfos().extract<DistinctSetSizeDelta>();
        chassert(set_size_delta);
        rows += data_chunk.chunk.getNumRows();
        bytes += set_size_delta->bytes;
        limit_reached = !size_limits.check(rows, bytes, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
    output.pushData(std::move(data_chunk));
    return Status::PortFull;
}
}
