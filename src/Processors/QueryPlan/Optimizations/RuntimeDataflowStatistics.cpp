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
#include <Core/SortDescription.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/sortBlock.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>


namespace ProfileEvents
{
extern const Event RuntimeDataflowStatisticsInputBytes;
extern const Event RuntimeDataflowStatisticsOutputBytes;
extern const Event RuntimeDataflowStatisticsInputNanoseconds;
extern const Event RuntimeDataflowStatisticsOutputNanoseconds;
}

namespace DB
{

std::optional<RuntimeDataflowStatisticsCache::Entry> RuntimeDataflowStatisticsCache::getStats(size_t key) const
{
    if (const auto entry = stats_cache->get(key))
        return *entry;
    return std::nullopt;
}

std::optional<RuntimeDataflowStatisticsCache::Boundary> RuntimeDataflowStatisticsCache::getBoundary(UInt64 root_key) const
{
    if (const auto boundary = boundary_cache->get(root_key))
        return *boundary;
    return std::nullopt;
}

void RuntimeDataflowStatisticsCache::setBoundary(UInt64 root_key, Boundary boundary)
{
    boundary_cache->set(root_key, std::make_shared<Boundary>(boundary));
}

void RuntimeDataflowStatisticsCache::update(size_t key, RuntimeDataflowStatistics stats)
{
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputBytes, stats.input_bytes);
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputBytes, stats.output_bytes);
    stats_cache->set(key, std::make_shared<RuntimeDataflowStatistics>(stats));
}

RuntimeDataflowStatisticsBlock::~RuntimeDataflowStatisticsBlock()
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
    for (size_t i = 0; i < InputStatisticsType::MaxInputType; ++i)
    {
        const auto & stats = duplicated_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, fmt::format("Duplicated{}", toString(static_cast<InputStatisticsType>(i))));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.duplicated_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
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
        "Collected statistics: input bytes={}, duplicated bytes={}, output bytes={}",
        res.input_bytes,
        res.duplicated_bytes,
        res.output_bytes);

    if (res.input_bytes == 0 && res.output_bytes == 0)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "No statistics collected, skipping statistics update.");
        return;
    }

    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(cache_key, res);
}

bool isSerializedAsSingleStreamOfColumnType(const ISerialization & serialization, const DataTypePtr & type)
{
    size_t num_streams = 0;
    bool stream_is_column_itself = false;
    serialization.enumerateStreams(
        [&](const auto & substream_path)
        {
            ++num_streams;
            const auto & substream_type = substream_path.back().data.type;
            stream_is_column_itself
                = ISerialization::isSpecialCompressionAllowed(substream_path) && substream_type && substream_type->equals(*type);
        },
        type);
    return num_streams == 1 && stream_is_column_itself;
}

/// The codec the sample of `column` is to be compressed with.
///
/// What the sample buffer holds is only known here: the serialization is picked from the column at hand, so
/// a column stored `Sparse` (or `Replicated`) arrives as several substreams funnelled into the one buffer,
/// and the column read from the part keeps the pre-`ALTER` type until the mutation that rewrites it has
/// run. A type-specific codec then measures a stream it was never resolved for. Some codecs merely
/// mismeasure it, but one that validates its input rejects it outright - `ALP` throws `CANNOT_COMPRESS` on
/// a byte count that is not a whole number of floats.
static const CompressionCodecPtr & chooseSampleCodec(
    const ISerialization & serialization, const ColumnWithTypeAndName & column, const ColumnCodecs & codecs)
{
    const bool sample_matches_type_specific_codec = codecs.type_specific && codecs.type_specific_for->equals(*column.type)
        && isSerializedAsSingleStreamOfColumnType(serialization, column.type);
    return sample_matches_type_specific_codec ? codecs.type_specific : codecs.generic;
}

/// How many leading rows of a column of `rows` rows the compression estimates below serialize: to avoid
/// spending too much time on serialization, the sample is limited.
static size_t sampleRowLimit(size_t rows)
{
    return std::max<size_t>(std::min<size_t>(8192, rows), rows / 10);
}

/// Tries to estimate compressed size of a column by serializing a sample of it.
static std::pair<size_t, size_t> estimateCompressedColumnSize(const ColumnWithTypeAndName & column, const ColumnCodecs & codecs)
{
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    const auto & codec = chooseSampleCodec(*serialization, column, codecs);
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf, codec);
    const auto limit = sampleRowLimit(column_to_write->size());
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
/// exactly wrong for a payload that is incompressible on its own. Serialize as many copies as the codec's
/// compressed block holds at full size and extrapolate the remaining copies from them, the same way
/// `ColumnAggregateFunction::sampledStateSizes` measures the repetitions of a constant state.
static std::pair<size_t, size_t>
estimateRepeatedCompressedColumnSize(const ColumnWithTypeAndName & column, size_t repetitions, const ColumnCodecs & codecs)
{
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    const auto & codec = chooseSampleCodec(*serialization, column, codecs);
    const auto limit = sampleRowLimit(column_to_write->size());

    auto serialize_copies = [&](size_t copies)
    {
        NullWriteBuffer null_buf;
        CompressedWriteBuffer compressed_buf(null_buf, codec);
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

    /// How many copies can compress against each other is a property of the codec and of the *full* copy,
    /// not of the sample: a new compressed block starts every `DBMS_DEFAULT_BUFFER_SIZE`, and a copy larger
    /// than the codec's match distance (64 KiB for `LZ4`) cannot reference the previous one at all, see
    /// `compressionCopiesPerBlock`. So measure exactly as many copies as one block holds at full size and
    /// scale the measured figure by the ratio of the uncompressed sizes: the wire repeats a block of the
    /// measured shape. Measuring more copies of a *truncated* sample than the full payload can pack into
    /// one block would invent cross-copy compression the actual output cannot have, and extrapolating the
    /// marginal cost of one more copy within a block would assume a block spanning the whole output.
    const size_t copies_within_window = compressionCopiesPerBlock(*codec, estimated_one_copy_bytes);
    /// Serializing a column is not free, so the measurement gets a budget of its own on top of that.
    static constexpr size_t max_repeated_sample_bytes = 1024 * 1024;
    const size_t measured_repetitions = std::min(
        {repetitions,
         copies_within_window,
         std::max<size_t>(max_repeated_sample_bytes / std::max<size_t>(one_copy_sample_bytes, 1), 1)});

    size_t compressed_bytes = one_copy_compressed_bytes;
    if (measured_repetitions <= 1)
    {
        /// A single copy already fills the window (or the measurement budget). Do not serialize another
        /// giant payload merely to estimate its marginal compressed size; the copies do not compress
        /// against each other.
        compressed_bytes *= repetitions;
    }
    else
    {
        const size_t measured_compressed_bytes = serialize_copies(measured_repetitions).second;
        compressed_bytes = measured_repetitions == repetitions
            ? measured_compressed_bytes
            : static_cast<size_t>(
                  static_cast<double>(measured_compressed_bytes) * static_cast<double>(repetitions)
                  / static_cast<double>(measured_repetitions));
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
///
/// `path` identifies the leaf within the visited column structurally, the same way in every block of the
/// same type: the position of each subcolumn on the way down, except that the alternatives of a `Variant`
/// are named by their global discriminators and those of a `Dynamic` by their type names. A traversal
/// ordinal would not do: `ColumnVariant::forEachSubcolumn` visits the alternatives in the *local* storage
/// order, which may differ between two columns of the same type.
///
/// `below_dynamic` tells whether the leaf is an alternative of a `ColumnDynamic`. `NativeWriter` pins the
/// state versions of the types in the block header to the peer's revision
/// (`NativeWriter::setAggregateFunctionStateVersions`), but a `Dynamic` type has no type children to pin:
/// `SerializationDynamic` serializes its alternatives with the variant type the column carries, so the
/// states below it are sent in the column's own version.
using AggregateStateLeafCallback = std::function<void(
    const ColumnAggregateFunction &,
    size_t skip_rows,
    const ColumnPtr & owner,
    size_t carrier_byte_size,
    bool below_dynamic,
    const String & path)>;

static void forEachAggregateStateLeaf(
    const IColumn & column,
    const AggregateStateLeafCallback & callback,
    const ColumnPtr & owner = {},
    bool below_dynamic = false,
    const String & path = {})
{
    if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&column))
    {
        callback(*aggregate_column, 0, owner, aggregate_column->byteSize(), below_dynamic, path);
        return;
    }
    if (const auto * sparse_column = typeid_cast<const ColumnSparse *>(&column))
    {
        /// `ColumnSparse::values` keeps an implicit default at row zero for in-memory use. The sparse
        /// serialization does not write it, so remove that row before visiting nested state leaves. This
        /// matters for row-expanding carriers such as `Array` and `Map`: their default row has no nested
        /// elements, so their aggregate-state leaves already begin with the first serialized value.
        const auto & original_values = sparse_column->getValuesPtr();
        /// The cut below copies the whole payload of `values` - every non-default string of a
        /// `ColumnSparse(String)`, say - and this runs for every column of every chunk, sampled or not. So
        /// look for the leaves on the original column first, which allocates nothing, and leave a sparse
        /// column without aggregate states alone. The cut preserves the structure, so the original values
        /// column has the same leaves in the same order; pair them up positionally to recover each leaf's
        /// share of the carrier's `byteSize`.
        std::vector<size_t> original_leaf_bytes;
        forEachAggregateStateLeaf(
            *original_values,
            [&](const ColumnAggregateFunction &, size_t, const ColumnPtr &, size_t carrier_byte_size, bool, const String &)
            { original_leaf_bytes.push_back(carrier_byte_size); },
            {},
            below_dynamic,
            path);
        if (original_leaf_bytes.empty())
            return;

        const ColumnPtr values = original_values->cut(1, original_values->size() - 1);
        size_t leaf_index = 0;
        forEachAggregateStateLeaf(
            *values,
            [&](const ColumnAggregateFunction & leaf, size_t skip_rows, const ColumnPtr &, size_t, bool leaf_below_dynamic, const String & leaf_path)
            {
                callback(leaf, skip_rows, values, original_leaf_bytes.at(leaf_index), leaf_below_dynamic, leaf_path);
                ++leaf_index;
            },
            values,
            below_dynamic,
            path);
        return;
    }
    /// Recurse structurally rather than with `forEachSubcolumnRecursively`, so that a `ColumnSparse` wrapped
    /// by another carrier (`Tuple`, `Nullable`, `Variant`, ...) is intercepted by the branch above too:
    /// otherwise its leaf is reported with the implicit default row of `values` still present, and an
    /// all-default block would commit the empty default state as a serialized state value.
    if (const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(&column))
    {
        /// The global discriminators of a `Dynamic`'s variant depend on which types the column has seen,
        /// so name the alternatives by their types.
        const auto & variant_column = dynamic_column->getVariantColumn();
        const auto & variant_names = dynamic_column->getVariantInfo().variant_names;
        for (size_t i = 0; i < variant_column.getNumVariants(); ++i)
            forEachAggregateStateLeaf(
                variant_column.getVariantByGlobalDiscriminator(i),
                callback,
                owner,
                /*below_dynamic=*/true,
                fmt::format("{}/dynamic {}", path, variant_names.at(i)));
        return;
    }
    if (const auto * variant_column = typeid_cast<const ColumnVariant *>(&column))
    {
        /// The discriminators and offsets hold no states; visit the alternatives in the global order.
        for (size_t i = 0; i < variant_column->getNumVariants(); ++i)
            forEachAggregateStateLeaf(
                variant_column->getVariantByGlobalDiscriminator(i), callback, owner, below_dynamic, fmt::format("{}/variant {}", path, i));
        return;
    }
    size_t subcolumn_index = 0;
    column.forEachSubcolumn(
        [&](const IColumn::WrappedPtr & subcolumn)
        {
            forEachAggregateStateLeaf(*subcolumn, callback, owner, below_dynamic, fmt::format("{}/{}", path, subcolumn_index));
            ++subcolumn_index;
        });
}

static bool hasAggregateStateLeaf(const IColumn & column)
{
    bool has_leaf = false;
    forEachAggregateStateLeaf(
        column, [&](const ColumnAggregateFunction &, size_t, const ColumnPtr &, size_t, bool, const String &) { has_leaf = true; });
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
    const ColumnPtr & column,
    const DataTypePtr & type,
    size_t repetitions,
    const ColumnCodecs & codecs,
    size_t & sample_bytes,
    size_t & compressed_bytes)
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
        const bool repeated = repetitions > 1 && column->size() != 1;
        const auto [sample, compressed] = repeated
            ? estimateRepeatedCompressedColumnSize({column, type, {}}, repetitions, codecs)
            : estimateCompressedColumnSize(
                  {repetitions > 1 ? ColumnPtr(ColumnConst::create(column, repetitions)) : column, type, {}}, codecs);
        /// The subtrees of one carrier can have very different row counts - the elements of an array next
        /// to a sibling of the outer rows - and the estimates sample at most `sampleRowLimit` of them, so
        /// their raw samples represent different fractions of their payloads. The caller pools the samples
        /// into one compression ratio, so scale each to the whole subtree it represents, keeping its own
        /// ratio; otherwise a fully sampled subtree would outweigh a partially sampled one of the same size.
        const size_t serialized_rows = repeated ? column->size() : column->size() * repetitions;
        const size_t sampled_rows = sampleRowLimit(serialized_rows);
        const double scale = sampled_rows < serialized_rows && sampled_rows
            ? static_cast<double>(serialized_rows) / static_cast<double>(sampled_rows)
            : 1.0;
        sample_bytes += static_cast<size_t>(static_cast<double>(sample) * scale);
        compressed_bytes += static_cast<size_t>(static_cast<double>(compressed) * scale);
        return;
    }

    /// A carrier with state leaves somewhere below: take it apart and recurse, sampling its own payload.
    if (const auto * const_column = typeid_cast<const ColumnConst *>(column.get()))
    {
        /// Materialize the carrier before looking at its descendants. In particular, the outer offsets of
        /// a constant `Array` become cumulative (`n, 2n, ...`) on the wire; wrapping an offsets subcolumn
        /// in another `ColumnConst` would incorrectly serialize the same offset repeatedly.
        sampleNonStatePartsCompression(
            const_column->convertToFullColumn(), type, repetitions, codecs, sample_bytes, compressed_bytes);
        return;
    }
    if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(column.get()))
    {
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            sampleNonStatePartsCompression(
                nullable_column->getNullMapColumnPtr(), std::make_shared<DataTypeUInt8>(), repetitions, codecs, sample_bytes, compressed_bytes);
            sampleNonStatePartsCompression(
                nullable_column->getNestedColumnPtr(), nullable_type->getNestedType(), repetitions, codecs, sample_bytes, compressed_bytes);
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
                    tuple_column->getColumnPtr(i), tuple_type->getElements()[i], repetitions, codecs, sample_bytes, compressed_bytes);
            return;
        }
    }
    else if (const auto * array_column = typeid_cast<const ColumnArray *>(column.get()))
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            sampleNonStatePartsCompression(
                array_column->getOffsetsPtr(), std::make_shared<DataTypeUInt64>(), repetitions, codecs, sample_bytes, compressed_bytes);
            sampleNonStatePartsCompression(
                array_column->getDataPtr(), array_type->getNestedType(), repetitions, codecs, sample_bytes, compressed_bytes);
            return;
        }
    }
    else if (const auto * map_column = typeid_cast<const ColumnMap *>(column.get()))
    {
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        {
            sampleNonStatePartsCompression(
                map_column->getNestedColumnPtr(), map_type->getNestedType(), repetitions, codecs, sample_bytes, compressed_bytes);
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
                variant_column->getLocalDiscriminatorsPtr(), std::make_shared<DataTypeUInt8>(), repetitions, codecs, sample_bytes, compressed_bytes);
            sampleNonStatePartsCompression(
                variant_column->getOffsetsPtr(), std::make_shared<DataTypeUInt64>(), repetitions, codecs, sample_bytes, compressed_bytes);
            for (size_t i = 0; i < variant_column->getNumVariants(); ++i)
                sampleNonStatePartsCompression(
                    variant_column->getVariantPtrByGlobalDiscriminator(i),
                    variant_type->getVariants()[i],
                    repetitions,
                    codecs,
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
            codecs,
            sample_bytes,
            compressed_bytes);
        return;
    }
    else if (const auto * sparse_column = typeid_cast<const ColumnSparse *>(column.get()))
    {
        sampleNonStatePartsCompression(
            sparse_column->getOffsetsPtr(), std::make_shared<DataTypeUInt64>(), repetitions, codecs, sample_bytes, compressed_bytes);
        sampleNonStatePartsCompression(
            sparse_column->getValuesPtr()->cut(1, sparse_column->getValuesPtr()->size() - 1),
            type,
            repetitions,
            codecs,
            sample_bytes,
            compressed_bytes);
        return;
    }

    /// A state-bearing carrier this walk does not know how to take apart (or whose type does not match its
    /// structure): count its own payload as incompressible rather than applying the leaves' ratio to it.
    size_t leaves_byte_size = 0;
    forEachAggregateStateLeaf(
        *column,
        [&](const ColumnAggregateFunction &, size_t, const ColumnPtr &, size_t carrier_byte_size, bool, const String &)
        { leaves_byte_size += carrier_byte_size; });
    const size_t carrier_bytes = (column->byteSize() - leaves_byte_size) * repetitions;
    sample_bytes += carrier_bytes;
    compressed_bytes += carrier_bytes;
}

/// The replicas merge their aggregation output with memory-bound merging, which sorts it by the group
/// by keys; the single-node merge this sample is taken from leaves it in hash-table order. So what a
/// replica puts on the wire is a sorted block, and pricing an unsorted sample of the very same rows
/// misses by the whole difference between the two - a monotonic `UInt64` key column compresses 7.8x
/// sorted and 3.0-3.9x in hash order.
///
/// The description is rebuilt ascending here instead of carrying the replica's own
/// `group_by_sort_description` down: what the ratio responds to is equal and neighbouring keys
/// ending up adjacent, which every direction and null placement gives equally.
static IColumn::Permutation keyOrderPermutation(
    const Columns & columns, const ColumnNumbers & keys_positions, const DataTypes & key_types)
{
    ColumnsWithTypeAndName key_columns;
    SortDescription description;
    key_columns.reserve(keys_positions.size());
    description.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
    {
        auto name = "__key_" + toString(i);
        key_columns.emplace_back(columns[keys_positions[i]], key_types[i], name);
        description.emplace_back(std::move(name), 1);
    }

    IColumn::Permutation permutation;
    stableGetPermutation(Block(key_columns), description, permutation);
    return permutation;
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
    Statistics & statistics,
    size_t num_rows,
    const ColumnsWithTypeAndName & cols,
    const CompressionCodecPtr & wire_codec,
    std::optional<size_t> full_bytes,
    const KeyOrderProvider & key_order)
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
        bool below_dynamic;
        /// Identifies the leaf across blocks, see `Statistics::serialized_states`.
        String key;
    };
    /// The key of a leaf: the output column, the structural path of the leaf in it (see
    /// `forEachAggregateStateLeaf`), and the state type. `getStateType` builds the type with the function's
    /// default version, so the version the states are serialized in is added separately: below `Dynamic`
    /// the wire keeps the column's own version, and the versions of a function can differ in size
    /// several-fold (`sumMap` writes 4-byte `Decimal32` values in version 0 and 16-byte `Decimal128` in
    /// version 1). Elsewhere `NativeWriter` pins the version to the peer's revision, so the column's
    /// version does not matter there.
    auto leaf_key = [](size_t column_index, const ColumnAggregateFunction & leaf, bool below_dynamic, const String & path)
    {
        String state_type = leaf.getAggregateFunction()->getStateType()->getName();
        if (below_dynamic)
        {
            const auto version = leaf.getVersion();
            state_type += version ? fmt::format("\tversion {}", *version) : String("\tdefault version");
        }
        return fmt::format("{}\t{}\t{}", column_index, path, state_type);
    };
    std::vector<StateLeaf> state_leaves;
    /// Whether cols[i] contains (or is) an aggregate-state leaf.
    std::vector<UInt8> col_has_states(cols.size(), 0);
    /// The bytes of cols[i] other than its aggregate-state leaves, as counted in `plain_bytes`.
    std::vector<size_t> col_plain_bytes(cols.size(), 0);
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
            [&](const ColumnAggregateFunction & leaf,
                size_t skip_rows,
                const ColumnPtr & owner,
                size_t carrier_byte_size,
                bool below_dynamic,
                const String & path)
            {
                col_has_states[i] = 1;
                state_leaves.emplace_back(&leaf, owner, repetitions, skip_rows, below_dynamic, leaf_key(i, leaf, below_dynamic, path));
                state_leaves_byte_size += carrier_byte_size;
            });
        /// The carrier's own payload (null maps, sibling tuple elements, array offsets, ...) is sized by
        /// `byteSize` like any plain column; the state leaves are sized from their serialized states below.
        col_plain_bytes[i] = (column->byteSize() - state_leaves_byte_size) * repetitions;
        plain_bytes += col_plain_bytes[i];
    }
    const bool has_aggregate_states = !state_leaves.empty();

    /// Aggregate-state leaves can have a different number of values than the outer block has rows:
    /// arrays and maps contain a value per nested element, while a variant alternative can be empty.
    /// Count the values that the Native serialization writes, not outer rows.
    auto leaf_values = [](const StateLeaf & leaf) { return (leaf.column->size() - leaf.skip_rows) * leaf.repetitions; };

    /// Until a sampled block with values of a leaf lands there is no per-value figure to extrapolate that leaf
    /// from, so a block carrying values of a leaf that no sample has covered yet serializes its states too and
    /// counts as an extra sample. This is tracked per leaf: the states of different leaves - e.g. the
    /// `count` and `groupArray` alternatives of a `Variant` - can differ in size by orders of magnitude, so
    /// one leaf's figure says nothing about another. Only leaves that do carry values take that path: a
    /// `Variant`/`Dynamic` output whose blocks hold only plain alternatives contributes no state bytes, and
    /// forcing every such block through it would sample the whole output.
    bool has_unsampled_leaf = false;
    if (has_aggregate_states && !sample_block)
    {
        std::lock_guard lock(statistics.mutex);
        for (const auto & leaf : state_leaves)
        {
            if (leaf_values(leaf) == 0)
                continue;
            const auto it = statistics.serialized_states.find(leaf.key);
            if (it == statistics.serialized_states.end() || it->second.values == 0)
            {
                has_unsampled_leaf = true;
                break;
            }
        }
    }
    const bool serialize_states = has_aggregate_states && (sample_block || has_unsampled_leaf);
    /// The serialized states of the block per leaf key, committed to `Statistics::serialized_states`.
    std::unordered_map<String, Statistics::SerializedStates> block_serialized_states;
    size_t serialized_state_bytes = 0;
    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    /// The pooled `sample_bytes / compressed_bytes` ratio is applied to the whole `bytes` total, so every
    /// component must enter it with the weight it has in that total. The components are sampled
    /// independently - a state leaf serializes at most `max_states_to_serialize` states, a plain column
    /// `estimateCompressedColumnSize`'s own prefix - so their raw samples represent different fractions of
    /// their bytes; pooled as they are, a component sampled more fully would dominate the ratio. Scale each
    /// sample to the bytes it represents, keeping its compression ratio.
    auto add_component_sample = [&](size_t represented_bytes, size_t component_sample_bytes, size_t component_compressed_bytes)
    {
        if (component_sample_bytes == 0)
            return;
        sample_bytes += represented_bytes;
        compressed_bytes += static_cast<size_t>(
            static_cast<double>(component_compressed_bytes) * static_cast<double>(represented_bytes)
            / static_cast<double>(component_sample_bytes));
    };
    /// Only output columns get here, and they model what a replica sends to the initiator rather than
    /// anything stored in a part, so there is no column `CODEC` to resolve as in `recordInputColumns`.
    /// The transfer codec is what `network_compression_method` resolves to, threaded in from the
    /// optimization settings. It is generic, so it applies to any serialization layout.
    const ColumnCodecs codecs{.generic = wire_codec ? wire_codec : CompressionCodecFactory::instance().getDefaultCodec()};
    /// The compression is sampled in the order the replicas send the rows in. Built only for a block that is
    /// actually sampled - one in five, and at most five per query. The uncompressed figures above do not
    /// depend on the order, so they are taken from the original columns.
    ColumnsWithTypeAndName sorted_cols;
    std::vector<StateLeaf> sorted_state_leaves;
    if (key_order && (sample_block || serialize_states))
    {
        const auto permutation = key_order();
        sorted_cols = cols;
        for (auto & col : sorted_cols)
            col.column = col.column->permute(permutation, 0);
        if (serialize_states)
        {
            size_t column_index = 0;
            for (const auto & col : sorted_cols)
            {
                ColumnPtr column = col.column;
                size_t repetitions = 1;
                if (const auto * const_column = typeid_cast<const ColumnConst *>(column.get()))
                {
                    repetitions = column->size();
                    column = const_column->getDataColumnPtr();
                }
                forEachAggregateStateLeaf(
                    *column,
                    [&](const ColumnAggregateFunction & leaf,
                        size_t skip_rows,
                        const ColumnPtr & owner,
                        size_t,
                        bool below_dynamic,
                        const String & path)
                    {
                        sorted_state_leaves.emplace_back(
                            &leaf, owner ? owner : column, repetitions, skip_rows, below_dynamic, leaf_key(column_index, leaf, below_dynamic, path));
                    });
                ++column_index;
            }
        }
    }
    const auto & sampled_cols = key_order ? sorted_cols : cols;
    const auto & sampled_state_leaves = key_order ? sorted_state_leaves : state_leaves;
    if (serialize_states)
    {
        /// The same ~1000-sample target as `Aggregator::estimateSizeOfCompressedState` uses for a
        /// single-level hash table: serialized state sizes can be arbitrarily skewed, and a sample of 100
        /// swings the extrapolation several-fold depending on whether the few giant states land on the
        /// sampled positions. Blocks of up to 1000 states are measured exactly.
        static constexpr size_t max_states_to_serialize = 1000;
        for (const auto & leaf : sampled_state_leaves)
        {
            const auto & [aggregate_column, owner, repetitions, skip_rows, below_dynamic, key] = leaf;
            static_cast<void>(owner);
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
            /// The states are compressed with the same codec as the rest of the output: the sample and the
            /// block its repetitions are judged against must both describe the wire.
            /// A leaf below `Dynamic` is sent in the column's own state version, see `forEachAggregateStateLeaf`.
            const auto sizes = aggregate_column->sampledStateSizes(
                max_states_to_serialize, repetitions, skip_rows, codecs.generic, /*keep_column_version=*/below_dynamic);
            serialized_state_bytes += sizes.bytes;
            auto & block_leaf_states = block_serialized_states[key];
            block_leaf_states.bytes += sizes.bytes;
            block_leaf_states.values += leaf_values(leaf);
            add_component_sample(sizes.bytes, sizes.sample_bytes, sizes.compressed_bytes);
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
            /// For the same reason such a block samples its state-free sibling columns as well: their bytes
            /// enter `plain_bytes`, and leaving them out would price them with the state-only ratio.
            if (col_has_states[i])
            {
                size_t non_state_sample_bytes = 0;
                size_t non_state_compressed_bytes = 0;
                sampleNonStatePartsCompression(
                    sampled_cols[i].column, sampled_cols[i].type, 1, codecs, non_state_sample_bytes, non_state_compressed_bytes);
                add_component_sample(col_plain_bytes[i], non_state_sample_bytes, non_state_compressed_bytes);
                continue;
            }
            auto [sample, compressed] = estimateCompressedColumnSize(sampled_cols[i], codecs);
            add_component_sample(col_plain_bytes[i], sample, compressed);
        }
    }

    std::lock_guard lock(statistics.mutex);
    /// `full_bytes` is an exact size supplied by aggregation, so preserve it over the estimate below.
    size_t block_bytes = full_bytes.value_or(plain_bytes);
    if (serialize_states)
    {
        for (const auto & [key, block_leaf_states] : block_serialized_states)
        {
            auto & leaf_states = statistics.serialized_states[key];
            leaf_states.bytes += block_leaf_states.bytes;
            leaf_states.values += block_leaf_states.values;
        }
        if (!full_bytes)
            block_bytes += serialized_state_bytes;
    }
    else if (!full_bytes)
    {
        /// Extrapolate each leaf from the per-value figure of its own serialized samples. A leaf with values
        /// gets here only after a sample has committed some of them (the committed counts only grow), so
        /// its entry exists and is not zero.
        for (const auto & leaf : state_leaves)
        {
            const size_t values = leaf_values(leaf);
            if (values == 0)
                continue;
            const auto & leaf_states = statistics.serialized_states.at(leaf.key);
            block_bytes += static_cast<size_t>(
                static_cast<double>(leaf_states.bytes) * static_cast<double>(values) / static_cast<double>(leaf_states.values));
        }
    }
    statistics.bytes += block_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    /// Reported in nanoseconds: a call often takes well under a microsecond, and rounding each one down would add up to nothing.
    const auto elapsed_nanoseconds = watch.elapsedNanoseconds();
    statistics.elapsed_microseconds += elapsed_nanoseconds / 1000;
    /// Only output columns get here, see above.
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputNanoseconds, elapsed_nanoseconds);
}

static DataTypes getKeyTypesFrom(const Block & header, const ColumnNumbers & keys_positions)
{
    DataTypes types;
    types.reserve(keys_positions.size());
    for (auto pos : keys_positions)
        types.push_back(header.getByPosition(pos).type);
    return types;
}

RuntimeDataflowStatisticsCacheUpdater::KeyOrderProvider RuntimeDataflowStatisticsCacheUpdater::keyOrderProviderFor(
    const Columns & columns, const ColumnNumbers & keys_positions, const DataTypes & key_types) const
{
    if (!replicas_send_output_in_key_order || keys_positions.empty())
        return {};
    return [&columns, &keys_positions, key_types] { return keyOrderPermutation(columns, keys_positions, key_types); };
}

void RuntimeDataflowStatisticsCacheUpdater::recordOutputChunk(const Chunk & chunk, const Block & header)
{
    chassert(chunk.getNumColumns() == header.columns());
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        cols.emplace_back(columns[i], header.getByPosition(i).type, "");
    recordColumns(block->output_bytes_statistics[OutputStatisticsType::OutputChunk], chunk.getNumRows(), cols, block->wire_codec);
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

    const auto estimate = variant.aggregator->estimateSizeOfCompressedState(variant, bucket, block->wire_codec);

    auto & statistics = block->output_bytes_statistics[OutputStatisticsType::AggregationState];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += estimate.bytes;
    statistics.sample_bytes += estimate.sample_bytes;
    statistics.compressed_bytes += estimate.compressed_bytes;
    const auto elapsed_nanoseconds = watch.elapsedNanoseconds();
    statistics.elapsed_microseconds += elapsed_nanoseconds / 1000;
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputNanoseconds, elapsed_nanoseconds);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types)
{
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
        cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
    recordColumns(
        block->output_bytes_statistics[OutputStatisticsType::AggregationKeys],
        chunk.getNumRows(),
        cols,
        block->wire_codec,
        /*full_bytes=*/{},
        keyOrderProviderFor(columns, keys_positions, key_types));
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk,
    const ColumnNumbers & keys_positions,
    const DataTypes & key_types,
    size_t full_key_bytes,
    const Columns & untruncated_sample_columns)
{
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    size_t num_rows = chunk.getNumRows();
    /// Which columns carry the keys below, and where: the sample holds the key columns alone, the chunk
    /// holds them at their own positions. The key order the replicas send in is taken from whichever of
    /// the two is priced.
    KeyOrderProvider key_order;
    ColumnNumbers sample_positions;
    if (!untruncated_sample_columns.empty())
    {
        /// The byte count describes the untruncated keys, so the ratio it is divided by has to come from
        /// them as well - the chunk holds the kept groups only, and their keys compress differently.
        /// When every group was rejected the chunk holds nothing at all, and without a ratio the byte
        /// count is dropped rather than estimated. The conversion kept a bounded sample for both cases.
        chassert(untruncated_sample_columns.size() == keys_positions.size());
        for (size_t i = 0; i < untruncated_sample_columns.size(); ++i)
            cols.emplace_back(untruncated_sample_columns[i], key_types[i], "");
        num_rows = untruncated_sample_columns.front()->size();

        sample_positions.resize(untruncated_sample_columns.size());
        for (size_t i = 0; i < sample_positions.size(); ++i)
            sample_positions[i] = i;
        key_order = keyOrderProviderFor(untruncated_sample_columns, sample_positions, key_types);
    }
    else
    {
        const auto & columns = chunk.getColumns();
        for (size_t i = 0; i < keys_positions.size(); ++i)
            cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
        key_order = keyOrderProviderFor(columns, keys_positions, key_types);
    }

    recordColumns(
        block->output_bytes_statistics[OutputStatisticsType::AggregationKeys], num_rows, cols, block->wire_codec, full_key_bytes, key_order);
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
    recordColumns(
        block->output_bytes_statistics[OutputStatisticsType::AggregationState],
        chunk.getNumRows(),
        cols,
        block->wire_codec,
        /*full_bytes=*/{},
        keyOrderProviderFor(columns, keys_positions, getKeyTypesFrom(header, keys_positions)));
}

void RuntimeDataflowStatisticsCacheUpdater::recordInputColumns(
    const ColumnsWithTypeAndName & input_columns,
    const NameSet & partially_read_columns,
    const NamesAndTypesList & part_columns,
    const ColumnSizeByName & column_sizes,
    const ColumnCodecByName & column_codecs,
    const CompressionCodecPtr & default_codec,
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
    auto & statistics = duplicated ? block->duplicated_bytes_statistics[type] : block->input_bytes_statistics[type];
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
                        const auto codec_it = column_codecs.find(column.name);
                        /// A column with no `CODEC` of its own is written with the part's default codec,
                        /// which is generic, so it applies to any serialization layout.
                        const auto [sample, compressed] = estimateCompressedColumnSize(
                            column,
                            codec_it == column_codecs.end() ? ColumnCodecs{.generic = default_codec} : codec_it->second);
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
    const auto elapsed_nanoseconds = watch.elapsedNanoseconds();
    statistics.elapsed_microseconds += elapsed_nanoseconds / 1000;
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputNanoseconds, elapsed_nanoseconds);
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
