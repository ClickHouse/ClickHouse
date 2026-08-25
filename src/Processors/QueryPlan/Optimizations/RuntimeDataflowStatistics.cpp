#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>


namespace ProfileEvents
{
extern const Event RuntimeDataflowStatisticsInputBytes;
extern const Event RuntimeDataflowStatisticsOutputBytes;
}

namespace DB
{

std::optional<RuntimeDataflowStatisticsCache::Entry> RuntimeDataflowStatisticsCache::getStats(size_t key) const
{
    if (const auto entry = stats_cache->get(key))
        return *entry;
    return std::nullopt;
}

void RuntimeDataflowStatisticsCache::update(size_t key, RuntimeDataflowStatistics stats)
{
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputBytes, stats.input_bytes);
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputBytes, stats.output_bytes);
    stats_cache->set(key, std::make_shared<RuntimeDataflowStatistics>(stats));
}

RuntimeDataflowStatisticsCacheUpdater::~RuntimeDataflowStatisticsCacheUpdater()
{
    if (unsupported_case)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "Unsupported case encountered, skipping statistics update.");
        return;
    }

    auto log_stats = [](const auto & stats, auto type) TSA_REQUIRES(stats.mutex)
    {
        LOG_TEST(
            getLogger("RuntimeDataflowStatisticsCacheUpdater"),
            "{} bytes={}, sample_bytes={}, compressed_bytes={}, compression_ratio={}, elapsed_microseconds={}",
            type,
            stats.bytes,
            stats.sample_bytes,
            stats.compressed_bytes,
            static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes),
            stats.elapsed_microseconds);
    };

    RuntimeDataflowStatistics res{.total_rows_to_read = total_rows_to_read};
    for (size_t i = 0; i < InputStatisticsType::MaxInputType; ++i)
    {
        const auto & stats = input_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<InputStatisticsType>(i)));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.input_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
        }
    }
    for (size_t i = 0; i < OutputStatisticsType::MaxOutputType; ++i)
    {
        const auto & stats = output_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<OutputStatisticsType>(i)));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.output_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
        }
    }

    LOG_DEBUG(
        getLogger("RuntimeDataflowStatisticsCacheUpdater"),
        "Collected statistics: input bytes={}, output bytes={}",
        res.input_bytes,
        res.output_bytes);

    if (res.input_bytes == 0 && res.output_bytes == 0)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "No statistics collected, skipping statistics update.");
        return;
    }

    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(cache_key, res);
}

/// Tries to estimate compressed size of a column by serializing a sample of it.
static std::pair<size_t, size_t> estimateCompressedColumnSize(const ColumnWithTypeAndName & column)
{
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf);
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    // To avoid spending too much time on serialization, we limit the number of rows to serialize.
    const auto limit = std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);
    NativeWriter::writeData(*serialization, column_to_write, compressed_buf, std::nullopt, 0, limit, DBMS_TCP_PROTOCOL_VERSION);
    compressed_buf.finalize();
    // Return pair of (sample size, compressed size), note that both sizes are based on limited number of rows.
    return std::make_pair(compressed_buf.count(), null_buf.count());
}

/// Same as `estimateCompressedColumnSize`, for a payload the wire carries `repetitions` times in a row:
/// everything below the stored row of a `ColumnConst` that cannot be put back into a constant itself - e.g.
/// the elements of a constant array, whose logical size stays above one after the walk recurses below the
/// constant row. `NativeWriter::writeData` materializes the constant, so those copies land in one compressed
/// stream back to back; identical copies compress to almost nothing while a copy fits the codec's match
/// window, and not at all once it outgrows it, so the repetitions have to be measured rather than scaled -
/// scaling the one-copy figures would pin the repeated payload's compression ratio to one copy's, which is
/// exactly wrong for a payload that is incompressible on its own. Serialize the sample enough times to
/// fill one compressed block and extrapolate the remaining copies from it, the same way
/// `ColumnAggregateFunction::sampledStateSizes` measures the repetitions of a constant state.
static std::pair<size_t, size_t> estimateRepeatedCompressedColumnSize(const ColumnWithTypeAndName & column, size_t repetitions)
{
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    // To avoid spending too much time on serialization, we limit the number of rows to serialize.
    const auto limit = std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);

    auto serialize_copies = [&](size_t copies)
    {
        NullWriteBuffer null_buf;
        CompressedWriteBuffer compressed_buf(null_buf);
        for (size_t copy = 0; copy < copies; ++copy)
            NativeWriter::writeData(*serialization, column_to_write, compressed_buf, std::nullopt, 0, limit, DBMS_TCP_PROTOCOL_VERSION);
        compressed_buf.finalize();
        /// Pair of (sample size, compressed size), like `estimateCompressedColumnSize` returns.
        return std::make_pair(compressed_buf.count(), null_buf.count());
    };

    const auto [one_copy_sample_bytes, one_copy_compressed_bytes] = serialize_copies(1);
    const size_t estimated_one_copy_bytes = limit == column_to_write->size()
        ? one_copy_sample_bytes
        : static_cast<size_t>(
              static_cast<double>(one_copy_sample_bytes) * static_cast<double>(column_to_write->size()) / static_cast<double>(limit));

    static constexpr size_t max_repeated_sample_bytes = 1024 * 1024;
    const size_t measured_repetitions
        = std::min(repetitions, std::max<size_t>(1, max_repeated_sample_bytes / std::max<size_t>(one_copy_sample_bytes, 1)));

    size_t compressed_bytes = one_copy_compressed_bytes;
    /// A truncated sample can fit in LZ4's 64 KiB match window even when a materialized copy cannot.
    /// Measuring repeated samples in that shape would falsely carry their cross-copy compression over to
    /// the actual output, so retain the one-copy ratio instead.
    static constexpr size_t lz4_match_window = 64 * 1024;
    if (estimated_one_copy_bytes > lz4_match_window || measured_repetitions == 1)
    {
        /// A single copy already exhausts the measurement budget. Do not serialize another giant
        /// payload merely to estimate its marginal compressed size; conservatively assume that the
        /// remaining copies do not compress against it.
        compressed_bytes *= repetitions;
    }
    else
    {
        const size_t measured_compressed_bytes = serialize_copies(measured_repetitions).second;
        compressed_bytes = measured_compressed_bytes;
        if (measured_repetitions != repetitions)
        {
            /// The compressed stream starts a new block - and with it a new match window - every
            /// `DBMS_DEFAULT_BUFFER_SIZE` of uncompressed data, which is the measurement budget above, so the
            /// copies beyond the measured ones cannot compress against them: the wire repeats a block of the
            /// measured shape. Scale the measured figure by the ratio of the uncompressed sizes rather than
            /// extrapolating the marginal cost of one more copy within the block, which would assume a window
            /// spanning the whole output.
            compressed_bytes = static_cast<size_t>(
                static_cast<double>(measured_compressed_bytes) * static_cast<double>(repetitions)
                / static_cast<double>(measured_repetitions));
        }
    }

    /// The uncompressed side is exact: the same payload, `repetitions` times. A sample too small to outweigh
    /// the compressed format's per-block framing must read as incompressible rather than as expanding - when
    /// the data is actually sent, that framing is amortized over `min_compress_block_size`.
    const size_t sample_bytes = one_copy_sample_bytes * repetitions;
    return std::make_pair(sample_bytes, std::min(sample_bytes, compressed_bytes));
}

/// Final `-State` results can reach the output wrapped in carrier columns - `prepareOutputBlockColumns`
/// recurses through the subcolumns of `isState()` results to attach the shared arenas to nested
/// `ColumnAggregateFunction` leaves, so e.g. `SELECT tuple(uniqExactState(x))` emits a `ColumnTuple` around
/// an aggregate-state leaf. Visit every such leaf, whether the column is one itself or wraps some.
///
/// `carrier_byte_size` is how many bytes the visited column's own `byteSize` attributes to that leaf. It is
/// the leaf's `byteSize` everywhere except below a `ColumnSparse`, where the leaf is a `cut` view of the
/// sparse `values`: `cut` shares the states through `src`, so the view's `byteSize` counts only its pointer
/// array while the sparse carrier still counts the original `values` column with the arena it owns.
/// Callers that size the carrier's own payload as `column->byteSize()` minus the leaves must subtract this
/// figure, otherwise the whole state payload stays behind as plain bytes and is counted a second time by
/// the serialized-state measurement.
static void forEachAggregateStateLeaf(
    const IColumn & column,
    const std::function<void(const ColumnAggregateFunction &, size_t skip_rows, const ColumnPtr & owner, size_t carrier_byte_size)> &
        callback,
    const ColumnPtr & owner = {})
{
    if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&column))
    {
        callback(*aggregate_column, 0, owner, aggregate_column->byteSize());
        return;
    }
    if (const auto * sparse_column = typeid_cast<const ColumnSparse *>(&column))
    {
        /// `ColumnSparse::values` keeps an implicit default at row zero for in-memory use. The sparse
        /// serialization does not write it, so remove that row before visiting nested state leaves. This
        /// matters for row-expanding carriers such as `Array` and `Map`: their default row has no nested
        /// elements, so their aggregate-state leaves already begin with the first serialized value.
        const auto & original_values = sparse_column->getValuesPtr();
        const auto values = original_values->cut(1, original_values->size() - 1);
        if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(values.get()))
        {
            callback(*aggregate_column, 0, values, original_values->byteSize());
            return;
        }
        /// The cut preserves the structure, so the original values column has the same leaves in the same
        /// order; pair them up positionally to recover each leaf's share of the carrier's `byteSize`.
        std::vector<size_t> original_leaf_bytes;
        original_values->forEachSubcolumnRecursively(
            [&](const IColumn & subcolumn)
            {
                if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&subcolumn))
                    original_leaf_bytes.push_back(aggregate_column->byteSize());
            });
        size_t leaf_index = 0;
        values->forEachSubcolumnRecursively(
            [&](const IColumn & subcolumn)
            {
                if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&subcolumn))
                {
                    callback(*aggregate_column, 0, values, original_leaf_bytes.at(leaf_index));
                    ++leaf_index;
                }
            });
        return;
    }
    column.forEachSubcolumnRecursively(
        [&](const IColumn & subcolumn)
        {
            if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&subcolumn))
                callback(*aggregate_column, 0, owner, aggregate_column->byteSize());
        });
}

static bool hasAggregateStateLeaf(const IColumn & column)
{
    bool has_leaf = false;
    forEachAggregateStateLeaf(column, [&](const ColumnAggregateFunction &, size_t, const ColumnPtr &, size_t) { has_leaf = true; });
    return has_leaf;
}

/// Samples the compression of everything in a state-bearing column except the aggregate-state leaves
/// themselves: the carriers' own payload - null maps, array offsets, sibling tuple elements, ... - and any
/// state-free subtrees. The leaves are sized and compression-sampled from their serialized states in
/// `recordColumns`, and Native-serializing the whole column would write every state a second time; but the
/// non-state payload still needs a compression sample of its own, otherwise the leaf-only ratio is applied
/// to it - e.g. `tuple(groupArrayState(x), s)` would estimate the sibling string's bytes with the states'
/// compression ratio.
///
/// `repetitions` is how many times each row of `column` appears on the wire: a state-bearing `ColumnConst`
/// is taken apart into its one-row payload, which `NativeWriter::writeData` materializes back to the
/// column's row count, so everything below such a carrier counts that many times.
static void sampleNonStatePartsCompression(
    const ColumnPtr & column, const DataTypePtr & type, size_t repetitions, size_t & sample_bytes, size_t & compressed_bytes)
{
    /// The leaf itself is measured from its serialized states by the caller.
    if (typeid_cast<const ColumnAggregateFunction *>(column.get()))
        return;

    /// A state-free subtree is a plain column; serialize a sample of it as a whole.
    if (!hasAggregateStateLeaf(*column))
    {
        /// A one-row subtree of a `ColumnConst` is put back into a constant of the original row count, so
        /// that the sample measures the repeated payload the wire carries, compressibility included.
        /// Deeper than the constant's own row - e.g. the elements of a constant array of states - the
        /// payload cannot be wrapped in a constant, so its copies are serialized and measured explicitly
        /// instead; either way the repetitions are measured rather than scaled from one copy, which would
        /// pin their compression ratio to one copy's.
        const auto [sample, compressed] = repetitions > 1 && column->size() != 1
            ? estimateRepeatedCompressedColumnSize({column, type, {}}, repetitions)
            : estimateCompressedColumnSize(
                  {repetitions > 1 ? ColumnPtr(ColumnConst::create(column, repetitions)) : column, type, {}});
        sample_bytes += sample;
        compressed_bytes += compressed;
        return;
    }

    /// A carrier with state leaves somewhere below: take it apart and recurse, sampling its own payload.
    if (const auto * const_column = typeid_cast<const ColumnConst *>(column.get()))
    {
        /// Materialize the carrier before looking at its descendants. In particular, the outer offsets of
        /// a constant `Array` become cumulative (`n, 2n, ...`) on the wire; wrapping an offsets subcolumn
        /// in another `ColumnConst` would incorrectly serialize the same offset repeatedly.
        sampleNonStatePartsCompression(
            const_column->convertToFullColumn(), type, repetitions, sample_bytes, compressed_bytes);
        return;
    }
    if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(column.get()))
    {
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            sampleNonStatePartsCompression(
                nullable_column->getNullMapColumnPtr(), std::make_shared<DataTypeUInt8>(), repetitions, sample_bytes, compressed_bytes);
            sampleNonStatePartsCompression(
                nullable_column->getNestedColumnPtr(), nullable_type->getNestedType(), repetitions, sample_bytes, compressed_bytes);
            return;
        }
    }
    else if (const auto * tuple_column = typeid_cast<const ColumnTuple *>(column.get()))
    {
        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
            tuple_type && tuple_type->getElements().size() == tuple_column->tupleSize())
        {
            for (size_t i = 0; i < tuple_column->tupleSize(); ++i)
                sampleNonStatePartsCompression(
                    tuple_column->getColumnPtr(i), tuple_type->getElements()[i], repetitions, sample_bytes, compressed_bytes);
            return;
        }
    }
    else if (const auto * array_column = typeid_cast<const ColumnArray *>(column.get()))
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            sampleNonStatePartsCompression(
                array_column->getOffsetsPtr(), std::make_shared<DataTypeUInt64>(), repetitions, sample_bytes, compressed_bytes);
            sampleNonStatePartsCompression(
                array_column->getDataPtr(), array_type->getNestedType(), repetitions, sample_bytes, compressed_bytes);
            return;
        }
    }
    else if (const auto * map_column = typeid_cast<const ColumnMap *>(column.get()))
    {
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        {
            sampleNonStatePartsCompression(
                map_column->getNestedColumnPtr(), map_type->getNestedType(), repetitions, sample_bytes, compressed_bytes);
            return;
        }
    }
    else if (const auto * variant_column = typeid_cast<const ColumnVariant *>(column.get()))
    {
        /// `Variant` allows `AggregateFunction` alternatives, so a state leaf can sit next to plain ones -
        /// e.g. `if(cond, groupArrayState(x), CAST(s, 'Variant(...)'))`. The discriminators, offsets and
        /// the state-free alternatives are plain payload; sample each, and recurse into the state-bearing
        /// alternatives. The alternatives are visited by their global discriminators, which is the order
        /// `DataTypeVariant::getVariants` lists the types in.
        if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get());
            variant_type && variant_type->getVariants().size() == variant_column->getNumVariants())
        {
            sampleNonStatePartsCompression(
                variant_column->getLocalDiscriminatorsPtr(), std::make_shared<DataTypeUInt8>(), repetitions, sample_bytes, compressed_bytes);
            sampleNonStatePartsCompression(
                variant_column->getOffsetsPtr(), std::make_shared<DataTypeUInt64>(), repetitions, sample_bytes, compressed_bytes);
            for (size_t i = 0; i < variant_column->getNumVariants(); ++i)
                sampleNonStatePartsCompression(
                    variant_column->getVariantPtrByGlobalDiscriminator(i),
                    variant_type->getVariants()[i],
                    repetitions,
                    sample_bytes,
                    compressed_bytes);
            return;
        }
    }
    else if (const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(column.get()))
    {
        /// `Dynamic` also allows `AggregateFunction` alternatives - e.g. `CAST(state, 'Dynamic')` - and
        /// stores its values in a nested `ColumnVariant`. The column tracks that variant's type itself
        /// (`getVariantInfo().variant_type`, always in sync with the nested column), so recurse into the
        /// variant with it and let the arm above take the alternatives apart.
        sampleNonStatePartsCompression(
            dynamic_column->getVariantColumnPtr(),
            dynamic_column->getVariantInfo().variant_type,
            repetitions,
            sample_bytes,
            compressed_bytes);
        return;
    }
    else if (const auto * sparse_column = typeid_cast<const ColumnSparse *>(column.get()))
    {
        sampleNonStatePartsCompression(
            sparse_column->getOffsetsPtr(), std::make_shared<DataTypeUInt64>(), repetitions, sample_bytes, compressed_bytes);
        sampleNonStatePartsCompression(
            sparse_column->getValuesPtr()->cut(1, sparse_column->getValuesPtr()->size() - 1),
            type,
            repetitions,
            sample_bytes,
            compressed_bytes);
        return;
    }

    /// A state-bearing carrier this walk does not know how to take apart (or whose type does not match its
    /// structure): count its own payload as incompressible rather than applying the leaves' ratio to it.
    size_t leaves_byte_size = 0;
    forEachAggregateStateLeaf(
        *column,
        [&](const ColumnAggregateFunction &, size_t, const ColumnPtr &, size_t carrier_byte_size)
        { leaves_byte_size += carrier_byte_size; });
    const size_t carrier_bytes = (column->byteSize() - leaves_byte_size) * repetitions;
    sample_bytes += carrier_bytes;
    compressed_bytes += carrier_bytes;
}

bool RuntimeDataflowStatisticsCacheUpdater::shouldSampleBlock(Statistics & statistics, size_t block_rows)
{
    // Empty blocks produced during planning, when we calculate output headers. Skip them.
    if (!block_rows)
        return false;
    const auto counter = statistics.counter.fetch_add(1, std::memory_order_relaxed);
    return counter % 5 == 0 && counter < 25;
}

void RuntimeDataflowStatisticsCacheUpdater::recordColumns(
    Statistics & statistics, size_t num_rows, const ColumnsWithTypeAndName & cols, std::optional<size_t> full_bytes)
{
    Stopwatch watch;

    const bool sample_block = shouldSampleBlock(statistics, num_rows);

    /// The uncompressed size of a column for the purpose of the estimate. `byteSize` of a
    /// `ColumnAggregateFunction` counts the state pointers plus the arena the column owns, and a column of
    /// states assembled by `AggregatingInOrderTransform` (see `addArenasToAggregateColumns`) does not own
    /// the arena its states live in, so `byteSize` degenerates to one pointer per row there. Size such
    /// columns from their serialized states instead - only the states, which is what
    /// `SerializationAggregateFunction` puts on the wire and what `Aggregator::estimateSizeOfCompressedState`
    /// measures - sampling as many of them as that function does per hash table, so that both producers of
    /// the `AggregationState` statistic measure the same thing. Serializing states is not cheap, and when
    /// `aggregation_in_order_max_block_bytes` splits large states into many small blocks, doing it per block
    /// would serialize every state of every block; so only the sampled blocks serialize, and the rest are
    /// extrapolated from the per-state-value figure the sampled blocks give, like the compression ratio is.
    /// Aggregate-state leaves of every column, top-level or wrapped: the wrappers' `byteSize` just sums the
    /// nested `byteSize`, so a wrapped leaf drops its shared-arena payload the same way a top-level one does.
    /// Every aggregate-state leaf with the number of times each of its rows appears on the wire.
    struct StateLeaf
    {
        const ColumnAggregateFunction * column;
        ColumnPtr owner;
        size_t repetitions;
        size_t skip_rows;
    };
    std::vector<StateLeaf> state_leaves;
    /// Whether cols[i] contains (or is) an aggregate-state leaf.
    std::vector<UInt8> col_has_states(cols.size(), 0);
    size_t plain_bytes = 0;
    for (size_t i = 0; i < cols.size(); ++i)
    {
        /// `NativeWriter::writeData` materializes constants - the data type cannot serialize them - so a
        /// constant column puts its single stored row on the wire once per row of the block, while
        /// `ColumnConst::byteSize` counts that row once. Size the stored payload and repeat it instead.
        ColumnPtr column = cols[i].column;
        size_t repetitions = 1;
        if (const auto * const_column = typeid_cast<const ColumnConst *>(column.get()))
        {
            repetitions = column->size();
            column = const_column->getDataColumnPtr();
        }
        size_t state_leaves_byte_size = 0;
        forEachAggregateStateLeaf(
            *column,
            [&](const ColumnAggregateFunction & leaf, size_t skip_rows, const ColumnPtr & owner, size_t carrier_byte_size)
            {
                col_has_states[i] = 1;
                state_leaves.emplace_back(&leaf, owner, repetitions, skip_rows);
                state_leaves_byte_size += carrier_byte_size;
            });
        /// The carrier's own payload (null maps, sibling tuple elements, array offsets, ...) is sized by
        /// `byteSize` like any plain column; the state leaves are sized from their serialized states below.
        plain_bytes += (column->byteSize() - state_leaves_byte_size) * repetitions;
    }
    const bool has_aggregate_states = !state_leaves.empty();

    /// Until the first sampled block with aggregate-state values lands there is no per-value figure to extrapolate from, so blocks
    /// racing with the first sampled one serialize their states too and count as extra samples.
    const bool serialize_states
        = has_aggregate_states && (sample_block || statistics.serialized_state_values.load(std::memory_order_relaxed) == 0);
    size_t serialized_state_bytes = 0;
    size_t serialized_state_values = 0;
    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    if (serialize_states)
    {
        /// The same ~1000-sample target as `Aggregator::estimateSizeOfCompressedState` uses for a
        /// single-level hash table: serialized state sizes can be arbitrarily skewed, and a sample of 100
        /// swings the extrapolation several-fold depending on whether the few giant states land on the
        /// sampled positions. Blocks of up to 1000 states are measured exactly.
        static constexpr size_t max_states_to_serialize = 1000;
        for (const auto & [aggregate_column, owner, repetitions, skip_rows] : state_leaves)
        {
            static_cast<void>(owner);
            serialized_state_values += (aggregate_column->size() - skip_rows) * repetitions;
            /// One periodic sample yields both the uncompressed figure and the compression sample, so
            /// the `bytes / (sample_bytes / compressed_bytes)` estimate is derived from a single
            /// population of states even when state size or compressibility changes with key order
            /// inside the block, and the compression side stays behind the same cap instead of
            /// serializing a prefix of up to `min(8192, rows)` states a second time. The clamp of the
            /// compressed size to the sample size - a sample of tiny states must read as
            /// incompressible, not as expanding - lives in `sampledStateSizes`, and so does the
            /// handling of the repetitions a constant carrier puts on the wire: identical copies
            /// compress far better than one copy suggests, so the repeated payload is measured there
            /// instead of scaling the one-copy figures, which would keep the one-copy ratio.
            const auto sizes = aggregate_column->sampledStateSizes(max_states_to_serialize, repetitions, skip_rows);
            serialized_state_bytes += sizes.bytes;
            sample_bytes += sizes.sample_bytes;
            compressed_bytes += sizes.compressed_bytes;
        }
    }

    if (sample_block || serialize_states)
    {
        for (size_t i = 0; i < cols.size(); ++i)
        {
            /// Columns holding aggregate-state leaves are measured above, from the same sample as their
            /// uncompressed figure; serializing them whole here would write every state a second time. But
            /// the leaves' sample says nothing about the rest of such a column - the carriers' own payload
            /// and any non-state siblings, which `plain_bytes` counts - so those parts get a compression
            /// sample of their own instead of inheriting the leaf-only ratio. A block whose states are
            /// serialized only because no sampled block has committed a state value yet (`serialize_states`
            /// without `sample_block`) samples them too: its states enter `sample_bytes`/`compressed_bytes`
            /// and its wrapper payload enters `plain_bytes`, so skipping the wrapper's sample here would
            /// derive the compression ratio from a different population of bytes than the total it divides.
            if (col_has_states[i])
            {
                sampleNonStatePartsCompression(cols[i].column, cols[i].type, 1, sample_bytes, compressed_bytes);
                continue;
            }
            if (!sample_block)
                continue;
            auto [sample, compressed] = estimateCompressedColumnSize(cols[i]);
            sample_bytes += sample;
            compressed_bytes += compressed;
        }
    }

    std::lock_guard lock(statistics.mutex);
    /// `full_bytes` is an exact size supplied by aggregation, so preserve it over the estimate below.
    size_t block_bytes = full_bytes.value_or(plain_bytes);
    if (serialize_states)
    {
        statistics.serialized_state_bytes += serialized_state_bytes;
        statistics.serialized_state_values += serialized_state_values;
        if (!full_bytes)
            block_bytes += serialized_state_bytes;
    }
    else if (has_aggregate_states && !full_bytes)
    {
        /// Aggregate-state leaves can have a different number of values than the outer block has rows:
        /// arrays and maps contain a value per nested element, while a variant alternative can be empty.
        /// Extrapolate from the values that the Native serialization writes, not outer rows.
        size_t block_state_values = 0;
        for (const auto & [aggregate_column, owner, repetitions, skip_rows] : state_leaves)
        {
            static_cast<void>(owner);
            block_state_values += (aggregate_column->size() - skip_rows) * repetitions;
        }
        block_bytes += static_cast<size_t>(
            static_cast<double>(statistics.serialized_state_bytes) * static_cast<double>(block_state_values)
            / static_cast<double>(statistics.serialized_state_values.load(std::memory_order_relaxed)));
    }
    statistics.bytes += block_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordOutputChunk(const Chunk & chunk, const Block & header)
{
    chassert(chunk.getNumColumns() == header.columns());
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        cols.emplace_back(columns[i], header.getByPosition(i).type, "");
    recordColumns(output_bytes_statistics[OutputStatisticsType::OutputChunk], chunk.getNumRows(), cols);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket)
{
    Stopwatch watch;

    /// We want to avoid situations when there is a single very large state (think of `SELECT uniqExact(col) FROM t`).
    /// Then we will spend a lot of time serializing it, and the overhead will be too high.
    if (variant.type == AggregatedDataVariants::Type::without_key
        && std::ranges::any_of(
            variant.aggregator->getParams().aggregates, [](auto agg_func) { return !agg_func.function->hasTrivialDestructor(); }))
    {
        markUnsupportedCase();
        return;
    }

    const auto estimate = variant.aggregator->estimateSizeOfCompressedState(variant, bucket);

    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationState];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += estimate.bytes;
    statistics.sample_bytes += estimate.sample_bytes;
    statistics.compressed_bytes += estimate.compressed_bytes;
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types)
{
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
        cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
    recordColumns(output_bytes_statistics[OutputStatisticsType::AggregationKeys], chunk.getNumRows(), cols);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types, size_t full_key_bytes)
{
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
        cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
    recordColumns(output_bytes_statistics[OutputStatisticsType::AggregationKeys], chunk.getNumRows(), cols, full_key_bytes);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateColumnSizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const Block & header)
{
    const auto & columns = chunk.getColumns();

    /// Mark key columns so we can skip them — only non-key columns are aggregate states.
    std::vector<bool> is_key(columns.size(), false);
    for (auto pos : keys_positions)
        is_key[pos] = true;

    ColumnsWithTypeAndName cols;
    cols.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (is_key[i])
            continue;
        cols.emplace_back(columns[i], header.getByPosition(i).type, "");
    }
    recordColumns(output_bytes_statistics[OutputStatisticsType::AggregationState], chunk.getNumRows(), cols);
}

void RuntimeDataflowStatisticsCacheUpdater::recordInputColumns(
    const ColumnsWithTypeAndName & input_columns,
    const NameSet & partially_read_columns,
    const NamesAndTypesList & part_columns,
    const ColumnSizeByName & column_sizes,
    size_t read_bytes,
    std::optional<bool> & should_continue_sampling)
{
    Stopwatch watch;

    const auto type = read_bytes ? InputStatisticsType::WithByteHint : InputStatisticsType::WithoutByteHint;
    if (type == InputStatisticsType::WithoutByteHint)
    {
        for (const auto & column : input_columns)
            read_bytes += column.column->byteSize();
    }

    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = input_bytes_statistics[type];
    if (read_bytes && !input_columns.empty())
    {
        if (!column_sizes.empty())
        {
            for (const auto & column : input_columns)
            {
                if (column_sizes.contains(column.name))
                {
                    const auto compressed_ratio = column_sizes.at(column.name).data_uncompressed
                        ? (static_cast<double>(column_sizes.at(column.name).data_compressed)
                           / static_cast<double>(column_sizes.at(column.name).data_uncompressed))
                        : 1.0;
                    sample_bytes += column.column->byteSize();
                    compressed_bytes += static_cast<size_t>(static_cast<double>(column.column->byteSize()) * compressed_ratio);
                }
            }
        }
        else if (std::ranges::any_of(
                     input_columns, [&](const auto & column) { return partially_read_columns.contains(column.name); }))
        {
            /// Partially read columns (e.g. only the offsets of an array whose data is missing from the part)
            /// are internally inconsistent until `fillMissingColumns` completes them, so they cannot be
            /// serialized for the sample below. Excluding just those columns would poison the statistics:
            /// the compression ratio would be derived from the surviving columns only, but applied to
            /// `read_bytes` of the whole block, which includes the bytes of the skipped column. There is no
            /// per-column byte split to subtract here (unlike the `column_sizes` branch above, which never
            /// serializes and handles such columns fine), so give up on the statistics for this query.
            markUnsupportedCase();
        }
        else
        {
            if (!should_continue_sampling.has_value())
                should_continue_sampling = shouldSampleBlock(statistics, input_columns[0].column->size());

            // We don't have individual column size info, likely because it is a compact part. Let's try to estimate it.
            if (*should_continue_sampling)
            {
                for (const auto & column : input_columns)
                {
                    // Paranoid check in case some, e.g., prewhere filter columns are present among the input columns
                    if (part_columns.contains(column.name))
                    {
                        const auto [sample, compressed] = estimateCompressedColumnSize(column);
                        sample_bytes += sample;
                        compressed_bytes += compressed;
                    }
                }
            }
        }
    }

    std::lock_guard lock(statistics.mutex);
    statistics.bytes += read_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache()
{
    static RuntimeDataflowStatisticsCache stats_cache;
    return stats_cache;
}

RuntimeDataflowStatisticsCollector::RuntimeDataflowStatisticsCollector(
    SharedHeader header_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/false)
    , updater(std::move(updater_))
{
}

void RuntimeDataflowStatisticsCollector::transform(Chunk & chunk)
{
    if (updater)
        updater->recordOutputChunk(chunk, getOutputPort().getHeader());
}
}
