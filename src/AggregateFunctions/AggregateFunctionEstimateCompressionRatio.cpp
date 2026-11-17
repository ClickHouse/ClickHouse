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
#include <Functions/FunctionFactory.h>
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
#include <Storages/MergeTree/MergeTreeSettings.h>
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
extern const SettingsUInt64 low_cardinality_max_dictionary_size;
extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
extern const SettingsUInt64 max_compress_block_size;
extern const SettingsUInt64 min_compress_block_size;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool compute_exact_num_defaults_for_sparse_columns;
extern const MergeTreeSettingsUInt64 index_granularity;
extern const MergeTreeSettingsUInt64 index_granularity_bytes;
extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version_for_zero_level_parts;
extern const MergeTreeSettingsUInt64 max_compress_block_size;
extern const MergeTreeSettingsUInt64 min_compress_block_size;
extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
extern const MergeTreeSettingsBool use_compact_variant_discriminators_serialization;
}

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_CODEC;
extern const int UNKNOWN_QUERY_PARAMETER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// The codec of the function, given as the body of `CODEC(...)`. The codec is applied to the serialized values directly, so a
/// declarative codec would measure nothing.
ASTPtr parseCodecDescription(const String & codec)
{
    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(ast);
    return ast;
}

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
            return CompressionCodecFactory::instance().get(parseCodecDescription(*codec), argument_types[0]);
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


/// Not a `MergeTree` setting: the most bytes of rows a state keeps before it writes them, which bounds the memory per group. A
/// granule wider than this is written in pieces. What a serialization writes once per call is then written once per piece,
/// where the writer writes it once per granule: the header of a `LowCardinality` column (16 bytes), the end of a granule of a
/// sparse column, the mode of compact `Variant` discriminators.
constexpr size_t PENDING_BYTES = DBMS_DEFAULT_BUFFER_SIZE;

/// How a new `MergeTree` table without `SETTINGS` writes a column, as far as the estimate follows it: the defaults of
/// `MergeTreeSettings`, and of `Settings` where the writer takes a setting from the query.
struct MergeTreeDefaults
{
    /// `index_granularity` and `index_granularity_bytes`.
    size_t granule_rows = 0;
    size_t granule_bytes = 0;
    size_t min_compress_block_size = 0;
    size_t max_compress_block_size = 0;
    /// The serialization the writer chooses for a new part, as `MergeTreeDataWriter` builds it: for example `String` with a
    /// separate size stream, sparse columns.
    SerializationInfoSettings info_settings;
    /// What the wide writer serializes with, as `MergeTreeWriterSettings` has it, as far as it matters for the types estimated here.
    ISerialization::SerializeBinaryBulkSettings bulk_settings;
};

const MergeTreeDefaults & mergeTreeDefaults()
{
    static const MergeTreeDefaults defaults = []
    {
        const MergeTreeSettings table;
        const Settings query;
        MergeTreeDefaults result;
        result.granule_rows = table[MergeTreeSetting::index_granularity];
        result.granule_bytes = table[MergeTreeSetting::index_granularity_bytes];
        /// The setting of the table if it is not 0, otherwise the one of the query.
        result.min_compress_block_size = table[MergeTreeSetting::min_compress_block_size]
            ? table[MergeTreeSetting::min_compress_block_size] : query[Setting::min_compress_block_size];
        result.max_compress_block_size = table[MergeTreeSetting::max_compress_block_size]
            ? table[MergeTreeSetting::max_compress_block_size] : query[Setting::max_compress_block_size];
        result.info_settings = SerializationInfoSettings(
            static_cast<double>(table[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization]),
            /*choose_kind_=*/true,
            table[MergeTreeSetting::compute_exact_num_defaults_for_sparse_columns],
            table[MergeTreeSetting::serialization_info_version],
            table[MergeTreeSetting::string_serialization_version],
            table[MergeTreeSetting::nullable_serialization_version],
            table[MergeTreeSetting::map_serialization_version_for_zero_level_parts],
            table[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types]);
        result.bulk_settings.low_cardinality_max_dictionary_size = query[Setting::low_cardinality_max_dictionary_size];
        result.bulk_settings.low_cardinality_use_single_dictionary_for_part = query[Setting::low_cardinality_use_single_dictionary_for_part];
        result.bulk_settings.use_compact_variant_discriminators_serialization
            = table[MergeTreeSetting::use_compact_variant_discriminators_serialization];
        return result;
    }();
    return defaults;
}

/// The name of the file a part would write the stream to. Substreams that share a file share a stream. `c` stands for the
/// column, whose name does not matter here.
String streamName(const ISerialization::SubstreamPath & path)
{
    return ISerialization::getFileNameForStream("c", path, ISerialization::StreamFileNameSettings{});
}

/// Calls `callback(name, path)` for each substream that `serialization` writes, as the writer enumerates them. Ephemeral
/// substreams are not written. Several substreams can have the same name, and then share one stream.
template <typename Callback>
void forEachStream(const ISerialization & serialization, const ISerialization::SubstreamData & data, Callback && callback)
{
    ISerialization::EnumerateStreamsSettings settings;
    serialization.enumerateStreams(
        settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            if (!ISerialization::isEphemeralSubcolumn(path, path.size()))
                callback(streamName(path), path);
        },
        data);
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

struct EstimateCompressionRatioWithTargetData
{
    /// The bytes of the rows in their own type, of the rows written and of the merged states.
    UInt64 source_bytes = 0;
    /// The compressed bytes in the target type, of the finished parts and of the merged states.
    UInt64 compressed_bytes = 0;
    /// The rows not written yet, in their own type.
    MutableColumnPtr pending;
    /// The rows and bytes (in the target type) of the open granule written so far, when it is written in pieces.
    size_t granule_rows = 0;
    size_t granule_bytes = 0;
    /// The part the rows are written to, until the state is read.
    std::unique_ptr<TargetPart> part;
    /// A state kept in a `Memory` table outlives its query and is read by later queries: maybe several at once (`mutex`: a
    /// read changes the state, as it finishes the part), maybe after a read that failed half-way (`broken`: set while the
    /// part changes, it stays set if that fails, and the state cannot be read then).
    bool broken = false;
    std::mutex mutex;
};

/// `estimateCompressionRatio` with a `target_type`: the bytes of the rows in their own type, divided by the bytes they would
/// take compressed as a column of `target_type` in a wide part of a new `MergeTree` table. The numerator depends only on the
/// rows, so the ratios of several targets for the same rows rank the targets.
///
/// A state keeps the rows it gets as pending rows, in their own type. When they fill a granule (`index_granularity` rows) or
/// `PENDING_BYTES`, they are converted to the target type and written to the part of the state: a `CompressedSizeCalculator`
/// per stream and one serialization state, as the writer has them. A granule wider than `PENDING_BYTES` is written in
/// pieces. Reading the state (its result, its serialization, a merge of a state that has opened a part) finishes the part,
/// and the next rows start a new one.
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
        , max_compress_block_size(block_size_bytes.value_or(merge_tree.max_compress_block_size))
        , min_compress_block_size(block_size_bytes.value_or(merge_tree.min_compress_block_size))
        , validation_settings(std::move(validation_settings_))
    {
        /// `Dynamic` and `JSON`, also nested, are not supported. As a target, their streams depend on the data, and the writer
        /// has more rules for them. As a source, pending rows keep the dynamic structure of the blocks they came in, and the
        /// numerator would depend on the blocks.
        const auto has_dynamic_structure = [](const IDataType & type) { return isDynamic(type) || isObject(type); };
        if (anyInTypeTree(*target_type, has_dynamic_structure))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} does not support target type {}", getName(), target_type->getName());
        /// Nor is a source with `LowCardinality`, which reaches the function only inside a `Variant` (the factory removes it
        /// elsewhere): it writes a dictionary with each serialization, so the numerator would depend on where granules end.
        if (anyInTypeTree(*source_type, [&](const IDataType & type) { return has_dynamic_structure(type) || type.lowCardinality(); }))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Aggregate function {} with a target type does not support argument type {}; convert the argument in the query first",
                getName(), source_type->getName());

        codec_description = codec ? parseCodecDescription(*codec) : CompressionCodecFactory::instance().getDefaultCodec()->getFullCodecDescription();

        /// The values are converted as `ALTER TABLE ... MODIFY COLUMN` converts them: with `_CAST`, which returns exactly the
        /// target type. It copies the conversion settings of the query, or of the server when there is no query (a table being
        /// loaded, a TTL `GROUP BY` in a background merge). Prepared here, so that a conversion the types do not allow is an
        /// error before any row comes.
        if (!source_type->equals(*target_type))
        {
            ContextPtr context = CurrentThread::tryGetQueryContext();
            if (!context)
                context = Context::getGlobalContextInstance();
            ColumnsWithTypeAndName cast_arguments{
                {source_type->createColumn(), source_type, ""},
                {DataTypeString().createColumnConst(1, target_type->getName()), std::make_shared<DataTypeString>(), ""}};
            cast = FunctionFactory::instance().get("_CAST", context)->build(cast_arguments);
            cast->prepare({});
        }

        /// The codec and block size of each stream, resolved once for the parts of all states, and here, so that a codec the
        /// target type cannot take is an error before any row comes.
        auto serialization = target_type->getSerialization(merge_tree.info_settings);
        forEachStream(
            *serialization,
            ISerialization::SubstreamData(serialization).withType(target_type),
            [&](String name, const ISerialization::SubstreamPath & path)
            {
                if (!stream_layouts.contains(name))
                    stream_layouts.emplace(std::move(name), resolveStream(path));
            });
    }

    String getName() const override { return "estimateCompressionRatio"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// The function is registered as a window function, so the factory never wraps it in the `Null` combinator, and its rows
    /// keep their NULLs (measured as a null map, or an error for a type that cannot hold NULL). Returning itself as its own
    /// `Null` adapter tells `RewriteAggregateFunctionWithIfPass` not to turn `f(if(c, NULL, x))` into `fIf(x, not c)`, which
    /// would drop them. The `chassert` checks the registration, not the properties argument: the rewrite pass passes default
    /// ones.
    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes &, const Array &, const AggregateFunctionProperties &) const override
    {
        chassert(AggregateFunctionFactory::instance().tryGetProperties(getName(), NullsAction::EMPTY).value().is_window_function);
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

    /// The other state is often another thread's share of the same group. If it has not opened a part, its rows are added to
    /// this state, so that they are written to the same part: the block headers and dictionaries of a part of its own would
    /// outweigh a few hundred rows. A state that has opened a part is finished as a part. The other state is not changed
    /// otherwise, so a state kept in a `Memory` table can be read again.
    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & rhs_ref = data(const_cast<AggregateDataPtr>(rhs));
        std::lock_guard lock(rhs_ref.mutex);
        throwIfBroken(rhs_ref);
        if (rhs_ref.part)
            finish(rhs_ref);

        auto & data_ref = data(place);
        data_ref.source_bytes += rhs_ref.source_bytes;
        data_ref.compressed_bytes += rhs_ref.compressed_bytes;
        if (rhs_ref.pending)
            addRows(data_ref, *rhs_ref.pending, 0, rhs_ref.pending->size());
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
        }
        return *data_ref.pending;
    }

    /// Whether the pending rows complete the open granule by rows, or reach `PENDING_BYTES`. Where a granule ends by bytes
    /// depends on the converted rows, and `writePending` finds it.
    bool isPendingFull(const Data & data_ref) const
    {
        return data_ref.granule_rows + data_ref.pending->size() >= merge_tree.granule_rows || data_ref.pending->byteSize() >= PENDING_BYTES;
    }

    /// Adds the rows as `add` adds them one by one: the pending rows are written right after the row that fills them, so the
    /// result does not depend on the blocks the rows come in. The rows go in as ranges that fit the room left, guessed from
    /// their average size; a range that overfills the pending rows by bytes is taken back and added row by row.
    void addRows(Data & data_ref, const IColumn & column, size_t row_begin, size_t row_end) const
    {
        if (row_begin == row_end)
            return;

        const size_t bytes_per_row = std::max<size_t>(1, column.byteSize() / column.size());
        while (row_begin < row_end)
        {
            auto & pending = pendingOf(data_ref);
            const size_t pending_bytes = pending.byteSize();
            const size_t room_rows = merge_tree.granule_rows - std::min(merge_tree.granule_rows, data_ref.granule_rows + pending.size());
            const size_t room_bytes = PENDING_BYTES - std::min(PENDING_BYTES, pending_bytes);
            const size_t rows = std::max<size_t>(1, std::min({row_end - row_begin, room_rows, room_bytes / bytes_per_row}));
            pending.insertRangeFrom(column, row_begin, rows);
            if (rows > 1 && pending.byteSize() >= PENDING_BYTES)
            {
                /// At least one row goes in, so the loop progresses even if `popBack` does not give all bytes back.
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

    /// Converts the pending rows and writes them to the part. They complete the open granule or are a piece of it; when their
    /// converted bytes end the granule earlier, the rows after its end go on into the next granules.
    void writePending(Data & data_ref) const
    {
        throwIfBroken(data_ref);
        chassert(data_ref.granule_rows + data_ref.pending->size() <= merge_tree.granule_rows);

        /// `CAST` validates the target type with the settings of the query (suspicious and experimental types). Here and not in
        /// the constructor, so that a stored state type that names such a type can still be attached, read and merged.
        if (!data_ref.part && validation_settings)
            validateDataType(target_type, *validation_settings);

        ColumnPtr rows = data_ref.pending->getPtr();
        ColumnPtr converted = convert(rows);
        const UInt64 rows_bytes = sourceBytes(*rows, /*first_in_part=*/!data_ref.part);

        data_ref.broken = true;
        if (!data_ref.part)
            data_ref.part = openPart(*converted);

        auto & part = *data_ref.part;
        for (size_t offset = 0; offset < converted->size();)
        {
            const auto [write_rows, write_bytes] = rowsToGranuleEnd(data_ref, *converted, offset);
            chassert(write_rows > 0);

            /// Before a granule, a stream that holds `min_compress_block_size` bytes ends its compressed block, as in
            /// `MergeTreeDataPartWriterWide::getCurrentMarksForColumn`.
            if (data_ref.granule_rows == 0)
                for (auto & [_, stream] : part.streams)
                    if (stream->offset() >= min_compress_block_size)
                        stream->endBlock();
            part.serialization->serializeBinaryBulkWithMultipleStreams(*converted, offset, write_rows, part.settings, part.state);

            const bool ends_granule = data_ref.granule_rows + write_rows >= merge_tree.granule_rows
                || data_ref.granule_bytes + write_bytes >= merge_tree.granule_bytes;
            data_ref.granule_rows = ends_granule ? 0 : data_ref.granule_rows + write_rows;
            data_ref.granule_bytes = ends_granule ? 0 : data_ref.granule_bytes + write_bytes;
            offset += write_rows;
        }
        data_ref.source_bytes += rows_bytes;
        data_ref.pending.reset();
        data_ref.broken = false;
    }

    /// The rows in the target type. A value that `CAST` cannot convert is an error here, before the state changes.
    ColumnPtr convert(const ColumnPtr & rows) const
    {
        if (!cast)
            return rows;
        /// The cast does not read its second argument, but it must be a constant.
        ColumnsWithTypeAndName arguments{
            {rows, source_type, ""},
            {DataTypeString().createColumnConst(rows->size(), target_type->getName()), std::make_shared<DataTypeString>(), ""}};
        return cast->execute(arguments, cast->getResultType(), rows->size(), /*dry_run=*/false);
    }

    /// How many of the converted rows from `offset` the open granule takes, and their bytes. A granule ends after
    /// `index_granularity` rows, or at the row where its converted rows reach `index_granularity_bytes`. That approximates the
    /// writer, which picks one granule size per insert block, from the bytes of all the columns of the block
    /// (`computeIndexGranularity`); neither is known here.
    std::pair<size_t, size_t> rowsToGranuleEnd(const Data & data_ref, const IColumn & converted, size_t offset) const
    {
        if (offset == 0)
        {
            const size_t bytes = converted.byteSize();
            if (data_ref.granule_bytes + bytes < merge_tree.granule_bytes)
                return {converted.size(), bytes};
        }

        size_t end = offset;
        size_t bytes = 0;
        while (end < converted.size() && data_ref.granule_bytes + bytes < merge_tree.granule_bytes)
            bytes += converted.byteSizeAt(end++);
        return {end - offset, bytes};
    }

    /// The numerator of the rows: their bytes serialized in their own type, as the function without a target type counts them.
    /// The prefix and suffix of the serialization (for example, the mode of `Variant` discriminators) are counted once per part,
    /// with its first rows, so that the numerator does not depend on where the granules of the target end. The rows themselves
    /// write nothing per call: the source types that would (`LowCardinality`, sparse, `Dynamic`) are removed by the factory,
    /// made dense in `addBatchSparseSinglePlace`, or rejected in the constructor.
    UInt64 sourceBytes(const IColumn & rows, bool first_in_part) const
    {
        NullWriteBufferWithMemory out(16384);
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&out](const ISerialization::SubstreamPath &) -> WriteBuffer * { return &out; };
        ISerialization::SerializeBinaryBulkStatePtr state;
        source_serialization->serializeBinaryBulkStatePrefix(rows, settings, state);
        const size_t prefix_end = out.count();
        source_serialization->serializeBinaryBulkWithMultipleStreams(rows, 0, rows.size(), settings, state);
        const size_t rows_end = out.count();
        source_serialization->serializeBinaryBulkStateSuffix(settings, state);
        out.finalize();
        return first_in_part ? out.count() : rows_end - prefix_end;
    }

    /// Opens a part with the first rows it gets, in the target type. They decide whether the column is sparse, as the rows of
    /// an insert decide it for a new part.
    std::unique_ptr<TargetPart> openPart(const IColumn & first_rows) const
    {
        auto part = std::make_unique<TargetPart>();

        auto info = target_type->createSerializationInfo(merge_tree.info_settings);
        info->add(first_rows);
        part->serialization = target_type->getSerialization(*info);

        /// A sparse serialization has a stream the dense one has not (the offsets of the values), resolved here.
        forEachStream(
            *part->serialization,
            ISerialization::SubstreamData(part->serialization).withType(target_type).withColumn(first_rows.getPtr()),
            [&](String name, const ISerialization::SubstreamPath & path)
            {
                if (part->streams.contains(name))
                    return;
                auto it = stream_layouts.find(name);
                auto layout = it != stream_layouts.end() ? it->second : resolveStream(path);
                part->streams.emplace(
                    std::move(name),
                    std::make_unique<CompressedSizeCalculator>(layout.codec, layout.block_size, DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE));
            });

        part->settings = merge_tree.bulk_settings;
        part->settings.getter = [streams = &part->streams](const ISerialization::SubstreamPath & path) -> WriteBuffer *
        {
            if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                return nullptr;
            auto it = streams->find(streamName(path));
            if (it == streams->end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No stream {} in the estimate of a part", streamName(path));
            return it->second.get();
        };
        part->serialization->serializeBinaryBulkStatePrefix(first_rows, part->settings, part->state);
        return part;
    }

    /// Writes the pending rows and the suffix of the part, and adds the compressed bytes of the part. The state then has no part
    /// and no pending rows, so reading it again gives the same totals.
    void finish(Data & data_ref) const
    {
        throwIfBroken(data_ref);
        if (data_ref.pending && !data_ref.pending->empty())
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

    /// What the estimate follows of how a new table writes a column.
    const MergeTreeDefaults & merge_tree = mergeTreeDefaults();
    const DataTypePtr source_type;
    const SerializationPtr source_serialization;
    const DataTypePtr target_type;
    const size_t max_compress_block_size;
    const size_t min_compress_block_size;
    /// The settings of the query that created the function, which `CAST` would validate the target type with.
    const std::optional<DataTypeValidationSettings> validation_settings;
    /// Null when the target type is the source type. It converts every row this function writes, also the pending rows of a
    /// state that another function added (a live state kept in a `Memory` table and merged by a later query).
    FunctionBasePtr cast;
    ASTPtr codec_description;
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

    /// The function without a target type gets its codec at the first row. A query checks it here as well, so that a
    /// misspelled codec, or a type name taken for a codec, is an error also without rows. Loading a table does not, so that
    /// the server starts with a stored state type that names such a codec (its states are empty: any row fails).
    if (codec && settings)
    {
        try
        {
            CompressionCodecFactory::instance().get(parseCodecDescription(*codec), arguments[0]);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_CODEC)
                e.addMessage("A string parameter is the codec unless it names a data type (the target type)");
            throw;
        }
    }
    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, codec, block_size_bytes);
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the compression ratio of a given column without compressing it.

With `target_type`, it estimates how the column would compress if it were stored as `target_type`.
It returns the size of the column in its own type divided by its estimated compressed size in `target_type`.
The numerator is the same for every target type and codec, so the results for several candidates of the same column can be compared: the larger the ratio, the smaller the column on disk.
A call without `target_type` uses a simpler model, so pass the type the column has now as one of the candidates too.

With `target_type`, the estimate follows how a new `MergeTree` table with the default settings writes a column of a wide part:
- one compressed stream per substream, for example the sizes and the characters of a `String`, or the dictionary and the indexes of a `LowCardinality` column;
- granules of 8192 rows, or of fewer rows when they take 10 MiB in `target_type`;
- sparse serialization for a column of mostly default values, decided from the first 8192 rows (or 1 MiB of rows), where a table decides from all the rows of a part;
- values converted as `CAST` converts them, as `ALTER TABLE ... MODIFY COLUMN` does.

The estimate is approximate, and it does not read the settings of any table.
A table sizes its granules from its insert blocks and from all its columns together, while the function sees one column and no insert blocks, so the granules of wide rows can differ.
The numerator of a `LowCardinality` column is the size of its values, not of its dictionary.
When a query runs in several threads, the rows of a group that one thread gathers join the rows of the other threads unless they fill a granule or 1 MiB, and then they are estimated as a separate part.
States that are serialized (in a distributed query, an external aggregation, or an `AggregateFunction` column) are separate parts too, so use `max_threads = 1` for a stable result of large groups.

`Dynamic` and `JSON` are not supported as target types, nor as arguments with `target_type`, and neither is an argument with `LowCardinality` inside a `Variant`: convert them in the query first.

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
