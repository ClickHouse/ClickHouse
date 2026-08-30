#include <Processors/Transforms/DistinctSetFilter.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/NullableUtils.h>
#include <Common/ColumnsHashing.h>
#include <Common/assert_cast.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

ColumnNumbers calculateDistinctKeyColumnsPositions(const Block & header, const Names & columns)
{
    const size_t num_columns = columns.empty() ? header.columns() : columns.size();
    ColumnNumbers key_columns_pos;
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns.empty() ? i : header.getPositionByName(columns[i]);
        const auto & col = header.getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
    }
    return key_columns_pos;
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

struct DistinctLowCardinalityFilter::DictionariesState
{
    using LCDictionaryKey = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKey;
    using LCDictionaryKeyHash = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKeyHash;

    struct LCDictState
    {
        /// seen_indices[idx] == 1 means dictionary index `idx` has been seen
        /// at least once for this dictionary identity.
        PaddedPODArray<UInt8> seen_indices;

        /// Number of dictionary indices we have seen at least once. When this
        /// reaches the dictionary size, any future row for the parent chunk cannot
        /// introduce a new distinct value.
        UInt64 seen_count = 0;
    };

    /// Per-dictionary state which may cover multiple IColumns.
    std::unordered_map<LCDictionaryKey, LCDictState, LCDictionaryKeyHash> lc_dict_states;
};

DistinctLowCardinalityFilter::DistinctLowCardinalityFilter()
    : dictionaries_state(std::make_unique<DictionariesState>())
{
}

DistinctLowCardinalityFilter::~DistinctLowCardinalityFilter() = default;

void DistinctLowCardinalityFilter::clear()
{
    dictionaries_state->lc_dict_states.clear();
}

std::optional<IColumn::Filter> DistinctLowCardinalityFilter::buildMaskIfApplicable(const IColumn & column, size_t num_rows)
{
    if (!lc_optimization_controller.isEnabled())
        return std::nullopt;

    const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column);
    if (!lc)
        return std::nullopt;

    auto [mask, new_indices_count] = buildMask(*lc, num_rows);
    lc_optimization_controller.update(num_rows, new_indices_count);
    return std::optional<IColumn::Filter>(std::move(mask));
}

std::pair<IColumn::Filter, size_t> DistinctLowCardinalityFilter::buildMask(const ColumnLowCardinality & column, size_t num_rows)
{
    const auto & dictionary = column.getDictionary();
    const auto dict_size = dictionary.size();

    DictionariesState::LCDictionaryKey dict_key;
    dict_key.hash = dictionary.getHash();
    dict_key.size = dict_size;

    auto & state = dictionaries_state->lc_dict_states[dict_key];

    /// The first time we see this dictionary, initialize the seen_indices array to keep track which entries
    /// in the dictionary have been seen.
    chassert(state.seen_count <= dict_size);
    if (state.seen_indices.size() != dict_size)
    {
        chassert(state.seen_indices.empty());
        chassert(state.seen_count == 0);
        state.seen_indices.resize_fill(dict_size);
    }

    /// If we've already seen all dictionary indices for this dictionary, then no row in this chunk
    /// (and also other chunks with the same dictionary) can produce a new distinct value.
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctLowCardinalityFilter");
    }

    return {std::move(mask), state.seen_count - seen_count_before};
}

namespace
{

/// Builds the DISTINCT filter for a chunk: filter[i] == 1 for rows whose key was not in the set yet
/// (the rows are inserted into the set). mask[i] == 0 marks rows excluded from the deduplication -
/// known duplicates by the LowCardinality dictionary index, or NULL-key rows in the skip_null_keys
/// mode - which are never inserted; mask may be nullptr.
template <typename Method>
void buildDistinctFilter(
    Method & method,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    IColumn::Filter & filter,
    const size_t rows,
    SetVariants & variants,
    const IColumn::Filter * mask)
{
    typename Method::State state(key_columns, key_sizes, nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (!(*mask)[i])
            {
                /// The row is excluded from the deduplication, skip insertion.
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctSetFilter");
    }
}

}

DistinctSetFilter::DistinctSetFilter(const Block & header, const Names & columns, const SizeLimits & set_size_limits_, bool skip_null_keys_)
    : key_columns_pos(calculateDistinctKeyColumnsPositions(header, columns))
    , data(std::make_unique<SetVariants>())
    , set_size_limits(set_size_limits_)
    , skip_null_keys(skip_null_keys_)
{
    key_types.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        key_types.push_back(header.getByPosition(pos).type);

    if (skip_null_keys)
    {
        /// A constant NULL key component is not a key column (constants are excluded above), but it
        /// makes every key contain a NULL, so with the skipping enabled nothing can be emitted at all.
        const size_t num_columns = columns.empty() ? header.columns() : columns.size();
        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto pos = columns.empty() ? i : header.getPositionByName(columns[i]);
            const auto & col = header.getByPosition(pos).column;
            if (col && isColumnConst(*col) && col->isNullAt(0))
                has_const_null_key = true;
        }
    }
}

size_t DistinctSetFilter::getTotalRowCount() const
{
    chassert(data);
    return data->getTotalRowCount();
}

size_t DistinctSetFilter::getTotalByteCount() const
{
    chassert(data);
    return data->getTotalByteCount();
}

bool DistinctSetFilter::supportsKeyExtraction() const
{
    chassert(data);
    /// In the skip_null_keys mode the set stores the nested (non-nullable) key representations, which
    /// do not match the (nullable) key column types, so the keys cannot be materialized back.
    return !skip_null_keys && data->type != SetVariants::Type::EMPTY && data->type != SetVariants::Type::hashed;
}

std::vector<MutableColumns> DistinctSetFilter::extractKeyColumns(size_t max_batch_rows) const
{
    chassert(supportsKeyExtraction());
    chassert(max_batch_rows > 0);

    std::vector<MutableColumns> batches;
    std::vector<IColumn *> current_batch_raw;
    size_t rows_in_batch = max_batch_rows;

    auto insert_into_batch = [&](auto && insert_key)
    {
        if (rows_in_batch == max_batch_rows)
        {
            MutableColumns batch;
            batch.reserve(key_types.size());
            current_batch_raw.clear();
            for (const auto & type : key_types)
            {
                batch.push_back(type->createColumn());
                current_batch_raw.push_back(batch.back().get());
            }
            batches.push_back(std::move(batch));
            rows_in_batch = 0;
        }

        insert_key(current_batch_raw);
        ++rows_in_batch;
    };

    /// All the hash tables (including the fixed ones, where the key is the cell index) support
    /// iteration with cell.getValue() returning the key.
    auto extract = [&](const auto & method)
    {
        for (const auto & cell : method.data)
        {
            insert_into_batch([&](std::vector<IColumn *> & batch)
            {
                std::decay_t<decltype(method)>::insertKeyIntoColumns(cell.getValue(), batch, key_sizes);
            });
        }
    };

    auto extract_fixed_keys = [&](const auto & method)
    {
        using Method = std::decay_t<decltype(method)>;

        /// The prepared-keys optimization packs the columns grouped by their size instead of the
        /// original order.
        const auto order = Method::State::packedKeysOrder(key_sizes);
        const std::vector<size_t> * unpack_order = order ? &*order : nullptr;

        for (const auto & cell : method.data)
        {
            insert_into_batch([&](std::vector<IColumn *> & batch)
            {
                Method::insertKeyIntoColumns(cell.getValue(), batch, key_sizes, unpack_order);
            });
        }
    };

    switch (data->type)
    {
        case SetVariants::Type::key8:
            extract(*data->key8);
            break;
        case SetVariants::Type::key16:
            extract(*data->key16);
            break;
        case SetVariants::Type::key32:
            extract(*data->key32);
            break;
        case SetVariants::Type::key64:
            extract(*data->key64);
            break;
        case SetVariants::Type::key_string:
            extract(*data->key_string);
            break;
        case SetVariants::Type::key_fixed_string:
            extract(*data->key_fixed_string);
            break;
        case SetVariants::Type::keys32:
            extract_fixed_keys(*data->keys32);
            break;
        case SetVariants::Type::keys64:
            extract_fixed_keys(*data->keys64);
            break;
        case SetVariants::Type::keys128:
            extract_fixed_keys(*data->keys128);
            break;
        case SetVariants::Type::keys256:
            extract_fixed_keys(*data->keys256);
            break;
        case SetVariants::Type::nullable_keys128:
            extract_fixed_keys(*data->nullable_keys128);
            break;
        case SetVariants::Type::nullable_keys256:
            extract_fixed_keys(*data->nullable_keys256);
            break;
        case SetVariants::Type::EMPTY:
        case SetVariants::Type::hashed:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Keys cannot be extracted from this DISTINCT set variant");
    }

    return batches;
}

void DistinctSetFilter::clear()
{
    data.reset();
    lc_filter.clear();
}

Chunk DistinctSetFilter::filter(Chunk chunk)
{
    chassert(data);
    chassert(!key_columns_pos.empty());

    /// Convert to full columns, because SetVariants for sparse and const columns is not implemented.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    const auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    /// The consumer skips rows with a NULL in any key component, so they carry no value downstream.
    /// Instead of pre-filtering the chunk, the NULL rows are masked out of the deduplication: they are
    /// neither inserted into the set nor selected for the output, and they leave the chunk together
    /// with the duplicates in the single filtering at the end. extractNestedColumnsAndNullMap also
    /// replaces the nullable key pointers with their nested columns, so the keys are hashed by the
    /// nested values, the same way the set fill hashes them (the values at the masked rows are never
    /// read).
    ColumnPtr null_map_holder;

    /// Declared outside of the branch: the deduplication mask below may point at it.
    IColumn::Filter keep;
    if (skip_null_keys)
    {
        ConstNullMapPtr null_map = nullptr;
        null_map_holder = extractNestedColumnsAndNullMap(column_ptrs, null_map);

        if (null_map && !memoryIsZero(null_map->data(), 0, num_rows))
        {
            keep.resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                keep[i] = !(*null_map)[i];
        }

        /// `LowCardinality(Nullable)` keys are not unwrapped by extractNestedColumnsAndNullMap: their
        /// NULL rows are the rows referencing the dictionary's NULL entry.
        for (const auto * column : column_ptrs)
            if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column);
                low_cardinality && low_cardinality->nestedIsNullable())
                markLowCardinalityNullRows(*low_cardinality, keep, num_rows);
    }

    std::optional<IColumn::Filter> lc_mask;

    if (key_columns_pos.size() == 1)
    {
        lc_mask = lc_filter.buildMaskIfApplicable(*column_ptrs[0], num_rows);

        /// Empty mask -> no candidate rows in this chunk.
        if (lc_mask && lc_mask->empty())
            return {};
    }

    /// The NULL-key rows and the rows that are known duplicates by their LowCardinality index are
    /// masked out of the deduplication the same way.
    const IColumn::Filter * mask = nullptr;
    if (lc_mask && !keep.empty())
    {
        for (size_t i = 0; i < num_rows; ++i)
            (*lc_mask)[i] &= keep[i];
        mask = &*lc_mask;
    }
    else if (lc_mask)
        mask = &*lc_mask;
    else if (!keep.empty())
        mask = &keep;

    if (data->empty())
        data->init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    const auto old_set_size = data->getTotalRowCount();
    IColumn::Filter filter_values(num_rows);

    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            buildDistinctFilter(*data->NAME, column_ptrs, key_sizes, filter_values, num_rows, *data, mask); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    const auto new_set_size = data->getTotalRowCount();
    const size_t num_selected = new_set_size - old_set_size;

    /// There isn't any new record in the chunk.
    if (num_selected == 0)
        return {};

    /// With the 'throw' overflow mode `check` throws; with 'break' it returns false: the limit is
    /// recorded (see isLimitReached), but the new rows of the current chunk are still returned - their
    /// keys are already in the set, and 'break' means return a partial result as if the source data
    /// ran out, not discard it.
    if (!set_size_limits.check(new_set_size, data->getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        limit_reached = true;

    /// When every row is a new distinct value, the columns are kept unchanged, without copying.
    if (num_selected != num_rows)
    {
        for (auto & column : columns)
            column = column->filter(filter_values, num_selected);
    }

    chunk.setColumns(std::move(columns), num_selected);
    return chunk;
}

}
