#include <cstdio>
#include <Formats/FormatSettings.h>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/TypeTree.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/assert_cast.h>

namespace DB
{
struct Settings;

namespace Setting
{
extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
}

/// Defined with the conversion functions, declared here as in `CastOverloadResolver.cpp`.
struct FunctionConvertSettings;
using FunctionConvertSettingsPtr = std::shared_ptr<const FunctionConvertSettings>;
FunctionConvertSettingsPtr createFunctionConvertSettings(const ContextPtr & context, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior);
FunctionBasePtr createFunctionBaseCast(
    const FunctionConvertSettingsPtr & settings,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type);

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_QUERY_PARAMETER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{


struct AggregationFunctionEstimateCompressionRatioData
{
    UInt64 merged_compressed_size = 0;
    UInt64 merged_uncompressed_size = 0;

    std::unique_ptr<CompressedSizeCalculator> calculator;

    [[maybe_unused]] ~AggregationFunctionEstimateCompressionRatioData()
    {
        /// Real cancellation can happen only in case of exception
        /// In other cases the data will be read via finalizeAndGetSizes()
        if (calculator)
            calculator->cancel();
    }
};

class AggregateFunctionEstimateCompressionRatio final
    : public IAggregateFunctionDataHelper<AggregationFunctionEstimateCompressionRatioData, AggregateFunctionEstimateCompressionRatio>
{
private:
    SerializationPtr serialization;
    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;


    void resetCalculatorIfNeeded(AggregateDataPtr __restrict place) const
    {
        Data & data_ref = data(place);

        /// When aggregating on windows transformed columns, the function WindowTransform::appendChunk
        /// calls updateAggregationState + writeOutCurrentRow in a loop.
        /// writeOutCurrentRow finalizes the buffer to flush and compute sizes, but doesn't deletes it.
        /// Ideally on finalized buffers we could "reinitialize" without reconstructing the whole object buffer.
        if (!data_ref.calculator || data_ref.calculator->isFinalized())
            data_ref.calculator = std::make_unique<CompressedSizeCalculator>(
                getCodecOrDefault(),
                roundCompressBlockSizeToWholeValues(block_size_bytes.value_or(DBMS_DEFAULT_BUFFER_SIZE), *argument_types[0]));
    }

    std::pair<UInt64, UInt64> finalizeAndGetSizes(ConstAggregateDataPtr __restrict place) const
    {
        const Data & data_ref = data(place);

        UInt64 uncompressed_size = data_ref.merged_uncompressed_size;
        UInt64 compressed_size = data_ref.merged_compressed_size;

        if (data_ref.calculator)
        {
            data_ref.calculator->finalize();

            uncompressed_size += data_ref.calculator->getUncompressedBytes();
            compressed_size += data_ref.calculator->getCompressedBytes();
        }

        return {uncompressed_size, compressed_size};
    }

    CompressionCodecPtr getCodecOrDefault() const
    {
        if (codec.has_value())
        {
            ParserCodec codec_parser;
            auto ast
                = parseQuery(codec_parser, "(" + codec.value() + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            /// The aggregate compresses the serialized values directly, so a declarative codec would measure nothing.
            CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(ast);
            return CompressionCodecFactory::instance().get(ast, argument_types[0]);
        }
        return CompressionCodecFactory::instance().getDefaultCodec();
    }

public:
    [[maybe_unused]] explicit AggregateFunctionEstimateCompressionRatio(
        const DataTypes & arguments, const Array & params, std::optional<String> codec_, std::optional<UInt64> block_size_bytes_)
        : IAggregateFunctionDataHelper(arguments, params, createResultType())
        , serialization(this->result_type->getDefaultSerialization())
        , codec(codec_)
        , block_size_bytes(block_size_bytes_)
    {
    }


    String getName() const override { return "estimateCompressionRatio"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = columns[0];

        resetCalculatorIfNeeded(place);

        DataTypePtr type_ptr = argument_types[0];
        SerializationInfoPtr info = type_ptr->getSerializationInfo(*column);
        SerializationPtr type_serialization_ptr = type_ptr->getSerialization(*info);

        type_serialization_ptr->serializeBinary(*column, row_num, *data(place).calculator, {});
    }

    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena) const override
    {
        addBatchSinglePlace(row_begin, row_end, place, columns, arena, -1);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 *,
        Arena * arena,
        ssize_t) const override
    {
        addBatchSinglePlace(row_begin, row_end, place, columns, arena, -1);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t) const override
    {
        const auto & column = columns[0];

        resetCalculatorIfNeeded(place);

        DataTypePtr type_ptr = argument_types[0];
        SerializationInfoPtr info = type_ptr->getSerializationInfo(*column);
        SerializationPtr type_serialization_ptr = type_ptr->getSerialization(*info);

        ISerialization::SerializeBinaryBulkSettings settings;

        settings.getter = [place](ISerialization::SubstreamPath) -> WriteBuffer * { return data(place).calculator.get(); };

        ISerialization::SerializeBinaryBulkStatePtr state;
        type_serialization_ptr->serializeBinaryBulkStatePrefix(*column, settings, state);
        type_serialization_ptr->serializeBinaryBulkWithMultipleStreams(*column, row_begin, row_end - row_begin, settings, state);
        type_serialization_ptr->serializeBinaryBulkStateSuffix(settings, state);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto [uncompressed_size, compressed_size] = finalizeAndGetSizes(rhs);

        data(place).merged_uncompressed_size += uncompressed_size;
        data(place).merged_compressed_size += compressed_size;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        auto [uncompressed_size, compressed_size] = finalizeAndGetSizes(place);

        writeVarUInt(uncompressed_size, buf);
        writeVarUInt(compressed_size, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        readVarUInt(data(place).merged_uncompressed_size, buf);
        readVarUInt(data(place).merged_compressed_size, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto [uncompressed_size, compressed_size] = finalizeAndGetSizes(place);

        /// Persist finalized sizes so the next add()/resetCalculatorIfNeeded() cycle
        /// preserves all previously accumulated data. Without this, window functions
        /// with growing frames (e.g. UNBOUNDED PRECEDING AND CURRENT ROW) lose all
        /// prior data when the buffer is recreated after finalization.
        data(place).merged_uncompressed_size = uncompressed_size;
        data(place).merged_compressed_size = compressed_size;

        /// Reset the calculator so that a repeated insertResultInto without an
        /// intervening add (unchanged window frame) does not re-count the
        /// already-persisted finalized bytes.
        data(place).calculator.reset();

        Float64 ratio = 0;
        if (compressed_size > 0)
            ratio = static_cast<Float64>(uncompressed_size) / static_cast<double>(compressed_size);

        assert_cast<ColumnFloat64 &>(to).getData().push_back(ratio);
    }
};


/// With a `target_type`, the function divides the bytes of the rows in their own type by the bytes they would take compressed
/// as a column of `target_type` in a `MergeTree` table with the default settings. The numerator depends only on the rows, so
/// the ratios of several targets for the same rows rank the targets.

/// The defaults of `MergeTree` the estimate follows.
constexpr size_t GRANULE_ROWS = 8192;
/// `index_granularity_bytes`: the writer ends a granule earlier when its rows take this much (over all columns of the table).
constexpr size_t GRANULE_BYTES = 10 * 1024 * 1024;
/// A state holds at most this many bytes of rows. A wider granule is written in pieces; a piece writes what a serialization
/// writes per call, which the writer writes once per granule: the header of a `LowCardinality` column (16 bytes), the end of a
/// granule of a sparse column, the format of compact `Variant` discriminators.
constexpr size_t PENDING_BYTES = DBMS_DEFAULT_BUFFER_SIZE;
constexpr size_t DEFAULT_MIN_COMPRESS_BLOCK_SIZE = 65536;
constexpr size_t DEFAULT_MAX_COMPRESS_BLOCK_SIZE = DBMS_DEFAULT_BUFFER_SIZE;

/// The serialization `MergeTreeDataWriter` chooses for a new part: `String` with a separate size stream, sparse columns.
const SerializationInfoSettings & mergeTreeInfoSettings()
{
    static const SerializationInfoSettings settings(
        0.9375,
        /*choose_kind_=*/true,
        /*compute_exact_num_defaults_=*/true,
        MergeTreeSerializationInfoVersion::WITH_TYPES,
        MergeTreeStringSerializationVersion::WITH_SIZE_STREAM,
        MergeTreeNullableSerializationVersion::BASIC,
        MergeTreeMapSerializationVersion::BASIC,
        /*propagate_types_serialization_versions_to_nested_types_=*/true);
    return settings;
}

/// The settings `MergeTreeDataPartWriterWide` serializes with, as far as they matter for the types estimated here.
ISerialization::SerializeBinaryBulkSettings mergeTreeBulkSettings()
{
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.low_cardinality_max_dictionary_size = 8192;
    settings.low_cardinality_use_single_dictionary_for_part = false;
    settings.use_compact_variant_discriminators_serialization = true;
    return settings;
}

String streamName(const ISerialization::SubstreamPath & path)
{
    return ISerialization::getFileNameForStream("c", path, ISerialization::StreamFileNameSettings{});
}

/// The column of one part as the target type: a compressed stream per substream and one serialization state for all of its
/// granules, as the writer has them.
struct TargetPart
{
    SerializationPtr serialization;
    ISerialization::SerializeBinaryBulkSettings settings;
    ISerialization::SerializeBinaryBulkStatePtr state;
    UnorderedMapWithMemoryTracking<String, std::unique_ptr<CompressedSizeCalculator>> streams;

    ~TargetPart()
    {
        for (auto & [_, stream] : streams)
            stream->cancel();
    }
};

/// How the rows of a state are converted to the target type: the cast and the type validation of the query that created the
/// function, with its settings copied (nothing of the query is kept). A state keeps the conversion of the function that added
/// its pending rows, so that a function created by another query that reads the state (a `-Merge` of a live state kept in a
/// `Memory` table) converts them as the query that added them would have.
struct Conversion
{
    /// Null when the target type is the source type, or when the function was created without any context.
    FunctionBasePtr cast;
    std::optional<DataTypeValidationSettings> validation_settings;
};

struct EstimateCompressionRatioWithTargetData
{
    /// The bytes of the rows in the source type: the granules written and the merged states.
    UInt64 source_bytes = 0;
    /// The compressed bytes in the target type: the finished parts and the merged states.
    UInt64 compressed_bytes = 0;
    /// Rows of the source type waiting to be written, and the conversion of the function that added them.
    MutableColumnPtr pending;
    std::shared_ptr<const Conversion> conversion;
    /// The rows and bytes of the open granule written so far, when it is written in pieces.
    size_t granule_rows = 0;
    size_t granule_bytes = 0;
    /// The part the granules go to, until the state is read.
    std::unique_ptr<TargetPart> part;
    /// Set while the part changes. It stays set if that fails, and the state cannot be read then.
    bool broken = false;
    /// A read changes the state (it finishes the part), and a state behind a const pointer can be read by several threads.
    std::mutex mutex;
};

class AggregateFunctionEstimateCompressionRatioWithTarget final
    : public IAggregateFunctionDataHelper<EstimateCompressionRatioWithTargetData, AggregateFunctionEstimateCompressionRatioWithTarget>
{
public:
    AggregateFunctionEstimateCompressionRatioWithTarget(
        const DataTypes & arguments,
        const Array & params,
        DataTypePtr target_type_,
        const std::optional<String> & codec,
        std::optional<UInt64> block_size_bytes,
        std::optional<DataTypeValidationSettings> validation_settings_)
        : IAggregateFunctionDataHelper(arguments, params, std::make_shared<DataTypeFloat64>())
        , source_type(arguments[0])
        , source_serialization(source_type->getDefaultSerialization())
        , target_type(std::move(target_type_))
        , converts(!source_type->equals(*target_type))
        , max_compress_block_size(block_size_bytes.value_or(DEFAULT_MAX_COMPRESS_BLOCK_SIZE))
        , min_compress_block_size(block_size_bytes.value_or(DEFAULT_MIN_COMPRESS_BLOCK_SIZE))
    {
        /// `Dynamic` and `JSON`, also nested. As a target, their substreams depend on the data and the writer has more rules for
        /// them. As a source, the rows waiting for a granule take the dynamic structure of the blocks they came in, which their
        /// serialization (the numerator) includes, so the result would depend on the blocks.
        const auto has_dynamic_structure = [](const IDataType & type) { return isDynamic(type) || isObject(type); };
        if (anyInTypeTree(*target_type, has_dynamic_structure))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} does not support target type {}", getName(), target_type->getName());
        /// Also a source with `LowCardinality`, which the factory removes from the arguments except inside a `Variant`: it writes
        /// a dictionary with each serialization, so the numerator would depend on where the target's granules end.
        if (anyInTypeTree(*source_type, [&](const IDataType & type) { return has_dynamic_structure(type) || type.lowCardinality(); }))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Aggregate function {} with a target type does not support argument type {}; convert the argument in the query first",
                getName(), source_type->getName());

        if (codec)
        {
            ParserCodec codec_parser;
            codec_description = parseQuery(
                codec_parser, "(" + *codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            /// The codec is applied to the serialized bytes directly, so a declarative codec would measure nothing.
            CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(codec_description);
        }
        else
        {
            codec_description = CompressionCodecFactory::instance().getDefaultCodec()->getFullCodecDescription();
        }

        /// Built as `accurateCast` builds it, with the settings of the query (enum values, date and time parsing and overflow;
        /// the cast copies them), and prepared here, so that a conversion the types do not allow is reported here and not at the
        /// first granule. `createInternalCast` would saturate dates and times that overflow whatever the settings say.
        /// A function created without a query (when a table is loaded: a state type, or an aggregate of a TTL `GROUP BY`, which
        /// background merges run on rows) converts with the settings of the server, never with none. A process without even
        /// those cannot aggregate rows; there the function gets no cast and converting is an error (`writePending`).
        auto own_conversion = std::make_shared<Conversion>();
        own_conversion->validation_settings = std::move(validation_settings_);
        ContextPtr settings_context = CurrentThread::tryGetQueryContext();
        if (!settings_context)
            settings_context = Context::getGlobalContextInstance();
        if (converts && settings_context)
        {
            ColumnsWithTypeAndName cast_arguments{{source_type->createColumn(), source_type, ""}, {nullptr, std::make_shared<DataTypeString>(), ""}};
            own_conversion->cast = createFunctionBaseCast(
                createFunctionConvertSettings(settings_context, settings_context->getSettingsRef()[Setting::date_time_overflow_behavior]),
                "accurateCast", cast_arguments, target_type, {}, CastType::accurate);
            own_conversion->cast->prepare({});
        }
        conversion = std::move(own_conversion);

        /// The codecs of the substreams are resolved here, so that a codec the target type cannot take is reported here too.
        ISerialization::EnumerateStreamsSettings enumerate_settings;
        auto serialization = target_type->getSerialization(mergeTreeInfoSettings());
        serialization->enumerateStreams(
            enumerate_settings,
            [this](const ISerialization::SubstreamPath & path)
            {
                if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                    return;
                String name = streamName(path);
                if (!stream_layouts.contains(name))
                    stream_layouts.emplace(std::move(name), resolveStream(path));
            },
            ISerialization::SubstreamData(serialization).withType(target_type));
    }

    String getName() const override { return "estimateCompressionRatio"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// NULL rows are measured (a null map, or an error for a type that cannot hold NULL), so the optimizer must not rewrite
    /// `f(if(c, NULL, x))` to `fIf(x, not c)`, which drops them. `RewriteAggregateFunctionWithIfPass` asks this.
    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes &, const Array &, const AggregateFunctionProperties &) const override
    {
        return nested_function;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data_ref = data(place);
        pendingOf(data_ref).insertFrom(*columns[0], row_num);
        if (isPendingFull(data_ref))
            writePending(data_ref);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos < 0)
        {
            addRows(data(place), *columns[0], row_begin, row_end);
            return;
        }

        const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        IColumn::Filter filter;
        filter.assign(flags.begin() + row_begin, flags.begin() + row_end);
        auto rows = columns[0]->cut(row_begin, row_end - row_begin)->filter(filter, -1);
        addRows(data(place), *rows, 0, rows->size());
    }

    /// The default adds the values first and the default rows after them, and the order of the rows matters to compression.
    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *) const override
    {
        auto rows = columns[0]->cut(row_begin, row_end - row_begin)->convertToFullColumnIfSparse();
        addRows(data(place), *rows, 0, rows->size());
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto [source_bytes, compressed_bytes] = read(rhs);
        data(place).source_bytes += source_bytes;
        data(place).compressed_bytes += compressed_bytes;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        auto [source_bytes, compressed_bytes] = read(place);
        writeVarUInt(source_bytes, buf);
        writeVarUInt(compressed_bytes, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        readVarUInt(data(place).source_bytes, buf);
        readVarUInt(data(place).compressed_bytes, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto [source_bytes, compressed_bytes] = read(place);
        Float64 ratio = 0;
        if (compressed_bytes > 0)
            ratio = static_cast<Float64>(source_bytes) / static_cast<Float64>(compressed_bytes);
        assert_cast<ColumnFloat64 &>(to).getData().push_back(ratio);
    }

private:
    struct StreamLayout
    {
        CompressionCodecPtr codec;
        size_t block_size;
    };

    /// As `MergeTreeDataPartWriterOnDisk::getSubstreamCodec`: the codec on the substreams of values, only its generic codecs on
    /// the structural ones (sizes, null maps, indexes). Blocks hold whole values, as in `MergeTreeDataPartWriterWide::addStreams`.
    StreamLayout resolveStream(const ISerialization::SubstreamPath & path) const
    {
        const auto & substream_type = path.back().data.type;
        const bool is_data_substream = ISerialization::isSpecialCompressionAllowed(path);
        auto codec = CompressionCodecFactory::instance().get(
            codec_description, is_data_substream ? substream_type.get() : nullptr, nullptr, /*only_generic=*/!is_data_substream);
        if (codec->needsVectorDimensionUpfront())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} with a target type does not support codec {}", getName(),
                codec->getFullCodecDescription()->formatForErrorMessage());

        const size_t block_size
            = substream_type ? roundCompressBlockSizeToWholeValues(max_compress_block_size, *substream_type) : max_compress_block_size;
        return {std::move(codec), block_size};
    }

    IColumn & pendingOf(Data & data_ref) const
    {
        if (!data_ref.pending)
        {
            data_ref.pending = source_type->createColumn();
            /// Otherwise the first row allocates 4 KiB, which a group of a few rows does not need.
            data_ref.pending->reserve(1);
            data_ref.conversion = conversion;
        }
        return *data_ref.pending;
    }

    /// The pending rows complete the granule by rows, or take `PENDING_BYTES`. Checked for every row added one at a time. Where a
    /// granule ends by bytes depends on the converted rows, and `writePending` finds it.
    static bool isPendingFull(const Data & data_ref)
    {
        return data_ref.granule_rows + data_ref.pending->size() >= GRANULE_ROWS || data_ref.pending->byteSize() >= PENDING_BYTES;
    }

    void addRows(Data & data_ref, const IColumn & column, size_t row_begin, size_t row_end) const
    {
        if (row_begin == row_end)
            return;

        const size_t bytes_per_row = std::max<size_t>(1, column.byteSize() / column.size());
        while (row_begin < row_end)
        {
            auto & pending = pendingOf(data_ref);
            const size_t pending_bytes = pending.byteSize();
            const size_t room_rows = GRANULE_ROWS - std::min(GRANULE_ROWS, data_ref.granule_rows + pending.size());
            const size_t room_bytes = PENDING_BYTES - std::min(PENDING_BYTES, pending_bytes);
            const size_t rows = std::max<size_t>(1, std::min({row_end - row_begin, room_rows, room_bytes / bytes_per_row}));
            pending.insertRangeFrom(column, row_begin, rows);
            /// The pending rows are written after the first row that fills them, as in `add`, whatever the blocks. The bytes
            /// only grow with the rows, so only a range that filled them by bytes is added again row by row to find that row.
            if (rows > 1 && pending.byteSize() >= PENDING_BYTES)
            {
                /// As `add`: a row goes in, then the rows are checked. At least one row goes in, also when a column keeps some
                /// bytes of the rows it gives back.
                pending.popBack(rows);
                do
                    pending.insertFrom(column, row_begin++);
                while (row_begin < row_end && !isPendingFull(data_ref));
            }
            else
            {
                row_begin += rows;
            }
            if (isPendingFull(data_ref))
                writePending(data_ref);
        }
    }

    /// Writes the pending rows to the part, up to the end of the granule: a granule, or a piece of one.
    void writePending(Data & data_ref) const
    {
        throwIfBroken(data_ref);

        /// As `CAST`, which rejects suspicious and experimental types the settings do not allow. Here and not when the function is
        /// created, so that a state type that names such a type can still be read and merged (`ATTACH`, `-Merge`).
        const Conversion & rows_conversion = *data_ref.conversion;
        if (!data_ref.part && rows_conversion.validation_settings)
            validateDataType(target_type, *rows_conversion.validation_settings);

        ColumnPtr rows = data_ref.pending->getPtr();
        /// A value the target type cannot hold is an error here, before the state changes.
        ColumnPtr converted = rows;
        if (converts)
        {
            if (!rows_conversion.cast)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Aggregate function {} has rows to convert but was created without settings", getName());
            /// With the types the cast was built for, not this function's: a `DateTime` without a time zone takes the session's
            /// when the type is created, and a function created by another query that reads the state may have another.
            ColumnsWithTypeAndName arguments{
                {rows, rows_conversion.cast->getArgumentTypes()[0], ""},
                {DataTypeString().createColumnConst(rows->size(), target_type->getName()), std::make_shared<DataTypeString>(), ""}};
            converted = rows_conversion.cast->execute(arguments, rows_conversion.cast->getResultType(), rows->size(), /*dry_run=*/false);
        }

        /// The writer sizes granules on the rows it writes, which are in the target type: a granule ends after `GRANULE_ROWS`
        /// rows, or at the row where its rows reach `GRANULE_BYTES`. The rows after that end stay pending, in their own type.
        size_t write_rows = converted->size();
        size_t write_bytes = converted->byteSize();
        if (data_ref.granule_bytes + write_bytes >= GRANULE_BYTES)
        {
            write_rows = 0;
            write_bytes = 0;
            while (write_rows < converted->size() && data_ref.granule_bytes + write_bytes < GRANULE_BYTES)
                write_bytes += converted->byteSizeAt(write_rows++);
        }
        const bool ends_granule = data_ref.granule_rows + write_rows >= GRANULE_ROWS || data_ref.granule_bytes + write_bytes >= GRANULE_BYTES;

        MutableColumnPtr rest;
        if (write_rows < rows->size())
        {
            rest = source_type->createColumn();
            rest->insertRangeFrom(*rows, write_rows, rows->size() - write_rows);
        }
        const UInt64 rows_bytes = sourceBytes(*rows, write_rows, /*first_in_part=*/!data_ref.part);

        data_ref.broken = true;
        if (!data_ref.part)
            data_ref.part = openPart(*converted);

        /// As `MergeTreeDataPartWriterWide::getCurrentMarksForColumn` before a granule: a stream that holds
        /// `min_compress_block_size` ends its compressed block.
        auto & part = *data_ref.part;
        if (data_ref.granule_rows == 0)
            for (auto & [_, stream] : part.streams)
                if (stream->offset() >= min_compress_block_size)
                    stream->endBlock();
        part.serialization->serializeBinaryBulkWithMultipleStreams(*converted, 0, write_rows, part.settings, part.state);

        data_ref.granule_rows = ends_granule ? 0 : data_ref.granule_rows + write_rows;
        data_ref.granule_bytes = ends_granule ? 0 : data_ref.granule_bytes + write_bytes;
        data_ref.source_bytes += rows_bytes;
        converted.reset();
        rows.reset();
        data_ref.pending = std::move(rest);
        data_ref.broken = false;
    }

    /// The first `limit` rows serialized in their own type, as the function measures them without a target type. The prefix and
    /// the suffix (the mode of `Variant` discriminators) are counted once per part, as one stream has them, so that the numerator
    /// does not depend on where the granules of the target end. The rows themselves write nothing per call: the types that do
    /// (`LowCardinality`, sparse, `Dynamic`) are removed or rejected before.
    UInt64 sourceBytes(const IColumn & rows, size_t limit, bool first_in_part) const
    {
        NullWriteBufferWithMemory out(16384);
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&out](const ISerialization::SubstreamPath &) -> WriteBuffer * { return &out; };
        ISerialization::SerializeBinaryBulkStatePtr state;
        source_serialization->serializeBinaryBulkStatePrefix(rows, settings, state);
        const size_t prefix_end = out.count();
        source_serialization->serializeBinaryBulkWithMultipleStreams(rows, 0, limit, settings, state);
        const size_t rows_end = out.count();
        source_serialization->serializeBinaryBulkStateSuffix(settings, state);
        out.finalize();
        return first_in_part ? out.count() : rows_end - prefix_end;
    }

    std::unique_ptr<TargetPart> openPart(const IColumn & first_granule) const
    {
        auto part = std::make_unique<TargetPart>();

        /// Sparse or not by the default values of the first rows, as a part by those of the rows inserted.
        auto info = target_type->createSerializationInfo(mergeTreeInfoSettings());
        info->add(first_granule);
        part->serialization = target_type->getSerialization(*info);

        ISerialization::EnumerateStreamsSettings enumerate_settings;
        part->serialization->enumerateStreams(
            enumerate_settings,
            [&](const ISerialization::SubstreamPath & path)
            {
                if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                    return;
                String name = streamName(path);
                if (part->streams.contains(name))
                    return;
                auto it = stream_layouts.find(name);
                auto layout = it != stream_layouts.end() ? it->second : resolveStream(path);
                part->streams.emplace(
                    std::move(name),
                    std::make_unique<CompressedSizeCalculator>(layout.codec, layout.block_size, DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE));
            },
            ISerialization::SubstreamData(part->serialization).withType(target_type).withColumn(first_granule.getPtr()));

        part->settings = mergeTreeBulkSettings();
        part->settings.getter = [streams = &part->streams](const ISerialization::SubstreamPath & path) -> WriteBuffer *
        {
            if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                return nullptr;
            auto it = streams->find(streamName(path));
            if (it == streams->end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No stream {} in the estimate of a part", streamName(path));
            return it->second.get();
        };
        part->serialization->serializeBinaryBulkStatePrefix(first_granule, part->settings, part->state);
        return part;
    }

    /// Writes the last rows and the suffix, and adds the compressed bytes of the part. A finished state has no part and no
    /// pending rows, so reading it again gives the same totals.
    void finish(Data & data_ref) const
    {
        throwIfBroken(data_ref);
        /// Every call writes at least one row.
        while (data_ref.pending && !data_ref.pending->empty())
            writePending(data_ref);
        data_ref.granule_rows = 0;
        data_ref.granule_bytes = 0;
        if (!data_ref.part)
            return;

        data_ref.broken = true;
        auto & part = *data_ref.part;
        part.serialization->serializeBinaryBulkStateSuffix(part.settings, part.state);
        UInt64 bytes = 0;
        for (auto & [_, stream] : part.streams)
        {
            stream->finalize();
            bytes += stream->getCompressedBytes();
        }
        data_ref.part.reset();
        data_ref.compressed_bytes += bytes;
        data_ref.broken = false;
    }

    std::pair<UInt64, UInt64> read(ConstAggregateDataPtr __restrict place) const
    {
        auto & data_ref = data(const_cast<AggregateDataPtr>(place));
        std::lock_guard lock(data_ref.mutex);
        finish(data_ref);
        return {data_ref.source_bytes, data_ref.compressed_bytes};
    }

    void throwIfBroken(const Data & data_ref) const
    {
        if (data_ref.broken)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "The state of aggregate function {} is incomplete after an earlier error", getName());
    }

    const DataTypePtr source_type;
    const SerializationPtr source_serialization;
    const DataTypePtr target_type;
    const bool converts;
    const size_t max_compress_block_size;
    const size_t min_compress_block_size;
    ASTPtr codec_description;
    /// The conversion of this function, given to the states it adds rows to.
    std::shared_ptr<const Conversion> conversion;
    UnorderedMapWithMemoryTracking<String, StreamLayout> stream_layouts;
};

}

static AggregateFunctionPtr createAggregateFunctionEstimateCompressionRatio(
    const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings * settings)
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);
    if (parameters.size() > 3)
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Aggregate function {} accepts at most three parameters", name);

    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;
    DataTypePtr target_type;
    for (const auto & param : parameters)
    {
        if (param.getType() == Field::Types::String)
        {
            const auto & value = param.safeGet<String>();
            /// No codec is named like a type, so a string that names a type is the target type. A string that starts with the
            /// name of a type but is not a valid type gets the error of that type, not of a codec.
            auto type = DataTypeFactory::instance().tryGet(value);
            String family = value.substr(0, value.find('('));
            boost::algorithm::trim(family);
            if (!type && DataTypeFactory::instance().hasNameOrAlias(family))
                type = DataTypeFactory::instance().get(value);
            if (type)
            {
                if (target_type)
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple target types specified for {}. Expected at most one string parameter that names a type (target_type)",
                        name);
                target_type = std::move(type);
                continue;
            }

            if (codec.has_value())
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "Multiple codecs specified for {}: '{}' and '{}'. A string parameter is the codec unless it names a data type "
                    "(the target type)",
                    name, *codec, value);
            codec = value;
        }
        else if (param.getType() == Field::Types::UInt64)
        {
            if (block_size_bytes.has_value())
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "Multiple numeric type parameters specified for {}. Expected at most one numeric type (block_size_bytes) parameter",
                    name);

            UInt64 new_block_size_bytes = param.safeGet<UInt64>();
            if (new_block_size_bytes == 0)
                throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "block_size_bytes should be greater than 0");

            /// Limit to 256 MiB to prevent absurd memory allocations from fuzzed queries
            static constexpr UInt64 max_block_size_bytes = 256 * 1024 * 1024;
            if (new_block_size_bytes > max_block_size_bytes)
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "block_size_bytes ({}) is too large, maximum is {}",
                    new_block_size_bytes, max_block_size_bytes);

            block_size_bytes = new_block_size_bytes;
        }
        else
        {
            throw Exception(
                ErrorCodes::UNKNOWN_QUERY_PARAMETER,
                "Invalid parameter type for {}. Expected String (codec or target_type) and/or UInt64 (block_size_bytes)",
                name);
        }
    }

    if (target_type)
    {
        std::optional<DataTypeValidationSettings> validation_settings;
        if (settings)
            validation_settings.emplace(*settings);
        return std::make_shared<AggregateFunctionEstimateCompressionRatioWithTarget>(
            arguments, parameters, std::move(target_type), codec, block_size_bytes, std::move(validation_settings));
    }
    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, codec, block_size_bytes);
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the compression ratio of a given column without compressing it.

With `target_type`, it estimates how the column would compress if it were stored as `target_type`, and returns the size of
the column in its own type divided by the estimated compressed size in `target_type`. The numerator is the same for every
target type and codec, so the results for several candidates of the same column can be compared: the larger the ratio, the
smaller the column on disk. Include the type the column has now as one of the candidates: a call without `target_type` uses
a simpler model, and its result is not comparable with these.

With `target_type`, the estimate follows how a `MergeTree` table with the default settings writes a column of a wide part: a
compressed stream per substream (for example, the sizes and the characters of a `String`, the dictionary and the indexes of
a `LowCardinality` column), granules of 8192 rows or 10 MiB in `target_type`, sparse serialization for columns of mostly default values (decided from
the first rows of a state, 8192 or the rows of 1 MiB, where a part decides from all its rows). It does not read the settings of any table. The values are
converted as `accurateCast` converts them, so a value that `target_type` cannot hold is an error. The numerator of a
`LowCardinality` column is the size of its values, not of its dictionary. Every thread of a query estimates its rows as a
separate part, so use `max_threads = 1` for a stable result. `Dynamic` and `JSON` are not supported as target types, nor as
arguments with `target_type`, and neither is an argument with `LowCardinality` inside a `Variant` (convert them in the query
first).

<Note>
For the examples below, the result will differ based on the default compression codec of the server.
See [Column Compression Codecs](/reference/statements/create/table#column_compression_codec).
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "estimateCompressionRatio([codec][, block_size_bytes][, target_type])(column)";
    FunctionDocumentation::Arguments arguments = {
        {"column", "Column of any type.", {"Any"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"codec", "String containing a compression codec or multiple comma-separated codecs in a single string.", {"String"}},
        {"block_size_bytes", "Block size of compressed data. This is similar to setting both [`max_compress_block_size`](/reference/settings/merge-tree-settings/max#max_compress_block_size) and [`min_compress_block_size`](/reference/settings/merge-tree-settings/min#min_compress_block_size). The default value is 1 MiB (1048576 bytes); with `target_type`, the defaults of `MergeTree` (64 KiB and 1 MiB). Maximum allowed value is 256 MiB (268435456 bytes).", {"UInt64"}},
        {"target_type", "The name of the data type to estimate the column in, for example `'LowCardinality(String)'`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an estimate compression ratio for the given column.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage with default codec",
        R"(
CREATE TABLE compression_estimate_example
(
    `number` UInt64
)
ENGINE = MergeTree()
ORDER BY number
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_example
SELECT number FROM system.numbers LIMIT 100_000;

SELECT estimateCompressionRatio(number) AS estimate FROM compression_estimate_example
        )",
        R"(
┌──────────estimate─┐
│ 5.758875867430677 │
└───────────────────┘
        )"
    },
    {
        "Using a specific codec",
        R"(
SELECT estimateCompressionRatio('T64')(number) AS estimate FROM compression_estimate_example
        )",
        R"(
┌──────────estimate─┐
│ 3.762758101688538 │
└───────────────────┘
        )"
    },
    {
        "Using multiple codecs",
        R"(
SELECT estimateCompressionRatio('T64, ZSTD')(number) AS estimate FROM compression_estimate_example
        )",
        R"(
┌───────────estimate─┐
│ 143.60078980434392 │
└────────────────────┘
        )"
    },
    {
        "Comparing types with `target_type`",
        R"(
SELECT
    estimateCompressionRatio('ZSTD(3)', 'String')(s) AS as_string,
    estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(s) AS as_low_cardinality
FROM (SELECT concat('region_', toString(number % 16)) AS s FROM numbers(1000000))
SETTINGS max_threads = 1
        )",
        R"(
┌─────────as_string─┬─as_low_cardinality─┐
│ 83.43419600583817 │  7352.941176470588 │
└───────────────────┴────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {25, 4};
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(
        "estimateCompressionRatio",
        {createAggregateFunctionEstimateCompressionRatio, documentation, {.is_order_dependent = true, .is_window_function = true}});
}
}
