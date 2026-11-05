#include <algorithm>
#include <memory>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Poco/String.h>

namespace DB
{
struct Settings;

namespace Setting
{
    extern const SettingsUInt64 low_cardinality_max_dictionary_size;
    extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
}

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
extern const int UNKNOWN_QUERY_PARAMETER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
}

namespace
{

/// The codec and the compressed block size of a substream, as `MergeTreeDataPartWriterWide` sets up the streams of a column.
struct StreamLayout
{
    CompressionCodecPtr codec;
    size_t block_size = 0;
};

using StreamLayouts = UnorderedMapWithMemoryTracking<String, StreamLayout>;

/// The serialization of a column over one run: prefix, bulk writes, suffix.
struct ColumnWriter
{
    DataTypePtr type;
    SerializationPtr serialization;
    ISerialization::SerializeBinaryBulkSettings settings;
    ISerialization::SerializeBinaryBulkStatePtr state;
    /// For a column whose streams depend on its content (`Dynamic`, `JSON`): the structure of the first block, which every block
    /// is written with.
    ColumnPtr sample;

    void writePrefix(const IColumn & column)
    {
        if (column.hasDynamicStructure())
        {
            auto first = column.cloneEmpty();
            first->takeExactDynamicStructureFrom(column);
            sample = std::move(first);
        }
        serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    }

    /// As `MergeTreeDataPartWriterOnDisk::prepareBlockForWriting`: rows of another structure than the first block's are rebuilt
    /// with it.
    void write(const IColumn & rows, size_t offset, size_t limit)
    {
        if (sample && !rows.dynamicStructureEquals(*sample))
        {
            auto rebuilt = type->createColumn();
            rebuilt->takeExactDynamicStructureFrom(*sample);
            rebuilt->insertRangeFrom(rows, offset, limit);
            serialization->serializeBinaryBulkWithMultipleStreams(*rebuilt, 0, limit, settings, state);
        }
        else
            serialization->serializeBinaryBulkWithMultipleStreams(rows, offset, limit, settings, state);
    }

    void writeSuffix()
    {
        serialization->serializeBinaryBulkStateSuffix(settings, state);
        state.reset();
    }
};

/// One run of a state, as MergeTree writes a column of one part, with a size calculator per substream. Heap-allocated: the
/// stream getter holds its address.
struct Run
{
    /// A sparse column after a dense one, or vice versa, starts a new run.
    ISerialization::KindStack kind_stack;
    ColumnWriter writer;

    struct Stream
    {
        String key;
        std::unique_ptr<CompressedSizeCalculator> calculator;
    };
    /// A handful of substreams: a linear search beats hashing.
    VectorWithMemoryTracking<Stream> streams;

    /// With a target type: the rows in the type of the column are serialized into a buffer that only counts bytes, the numerator.
    std::optional<ColumnWriter> source_writer;
    std::unique_ptr<NullWriteBuffer> source_counter;

    bool finished = false;
    /// The sizes of this run and of the finished runs before it since the state was last read.
    UInt64 uncompressed_bytes = 0;
    UInt64 compressed_bytes = 0;

    ~Run()
    {
        /// Left over by an exception; the finalized ones are gone already.
        for (auto & stream : streams)
            stream.calculator->cancel();
        if (source_counter)
            source_counter->cancel();
    }

    WriteBuffer * streamFor(const ISerialization::SubstreamPath & path)
    {
        const String key = ISerialization::getSubstreamsCacheKeyForStream(path);
        for (auto & stream : streams)
            if (stream.key == key)
                return stream.calculator.get();

        /// The writer has no stream for an ephemeral subcolumn either.
        if (ISerialization::isEphemeralSubcolumn(path, path.size()))
            return nullptr;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "No stream for substream {}", ISerialization::getSubcolumnNameForStream(path));
    }
};

struct AggregateFunctionEstimateCompressionRatioData
{
    UInt64 merged_compressed_size = 0;
    UInt64 merged_uncompressed_size = 0;

    /// Pending work: rows that do not fill a granule yet and the current run. Completed whenever the state is read, also through
    /// a const pointer (`serialize`, `merge`), which does not change what the state stands for.
    mutable MutableColumnPtr buffer;
    mutable std::unique_ptr<Run> run;
    /// The type of the column the rows of `buffer` come from. A state of a `LowCardinality(String)` column has the same type as
    /// a state of a `String` column, so the function that completes it may not be the one that produced it.
    DataTypePtr represented_type;
};

/// A conversion of the rows, prepared once. The prepared function refers to the function it is prepared from without owning
/// it, so both are kept.
struct Conversion
{
    FunctionBasePtr function;
    ExecutableFunctionPtr prepared;
};

/// The parameters of the function, and the settings the MergeTree writer takes from the query.
struct EstimateCompressionRatioConfig
{
    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;
    DataTypePtr target_type;
    size_t low_cardinality_max_dictionary_size = 8192;
    bool low_cardinality_use_single_dictionary_for_part = false;
};

class AggregateFunctionEstimateCompressionRatio final
    : public IAggregateFunctionDataHelper<AggregateFunctionEstimateCompressionRatioData, AggregateFunctionEstimateCompressionRatio>
{
private:
    /// The defaults of `index_granularity` and `index_granularity_bytes`.
    static constexpr size_t rows_per_granule = 8192;
    static constexpr size_t bytes_per_granule = 10 * 1024 * 1024;

    const EstimateCompressionRatioConfig config;
    /// The type of the column, `LowCardinality` included, which the function is created from (see
    /// `AggregateFunctionProperties::created_with_low_cardinality_types`), and the type its rows arrive as.
    const DataTypePtr represented_type;
    const DataTypePtr plain_type;
    /// The `target_type` parameter when it differs from the type of the column.
    const DataTypePtr target_type;
    /// The ratio is the serialized size of the rows in `represented_type` over their compressed size in `estimated_type`.
    const DataTypePtr estimated_type;
    /// The conversions of the rows from `plain_type`, where it differs.
    const std::optional<Conversion> to_represented;
    const std::optional<Conversion> to_target;
    /// `LowCardinality` writes a header with every write, so a granule is written at once even when the blocks of the query
    /// split it. For the other types the bytes do not depend on how the rows are split into writes.
    const bool write_whole_granules;
    const CompressionCodecPtr default_codec;
    /// The `codec` parameter, or the default codec, as the `CODEC` clause of a column.
    const ASTPtr codec_description;
    const size_t block_size_bytes;
    /// The layout of every substream of the default serialization of the estimated type; building it validates the codec against
    /// the type. A substream only a column reveals (sparse offsets, the variants of `Dynamic`) is resolved when a run starts.
    const StreamLayouts stream_layouts;

    static ASTPtr parseCodecDescription(const String & codec)
    {
        ParserCodec codec_parser;
        auto ast = parseQuery(codec_parser, "(" + codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        /// The serialized values are compressed directly, so a declarative codec would measure nothing.
        CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(ast);
        return ast;
    }

    /// The cast to `to`; `prepare` builds the converters and so rejects a conversion that does not exist.
    static Conversion createCast(const DataTypePtr & from, const DataTypePtr & to)
    {
        ColumnsWithTypeAndName arguments{{from->createColumn(), from, ""}, {nullptr, std::make_shared<DataTypeString>(), ""}};
        Conversion conversion;
        conversion.function = createInternalCast(arguments[0], to, CastType::accurate, {}, nullptr);
        conversion.prepared = conversion.function->prepare(arguments);
        return conversion;
    }

    /// As `MergeTreeDataPartWriterOnDisk::getSubstreamCodec` and `MergeTreeDataPartWriterWide::addStreams`: the codec applies to the
    /// data substreams, a structural substream (null map, array sizes, dictionary indexes) keeps only its generic codecs, and a
    /// compressed block holds whole values.
    StreamLayout resolveStreamLayout(const ISerialization::SubstreamPath & path) const
    {
        const DataTypePtr & substream_type = path.back().data.type;
        const bool is_data_substream = ISerialization::isSpecialCompressionAllowed(path);

        StreamLayout layout;
        layout.codec = CompressionCodecFactory::instance().get(
            codec_description, is_data_substream ? substream_type.get() : nullptr, default_codec, /*only_generic=*/ !is_data_substream);
        layout.block_size = substream_type ? roundCompressBlockSizeToWholeValues(block_size_bytes, *substream_type) : block_size_bytes;
        return layout;
    }

    StreamLayouts buildStreamLayouts(const DataTypePtr & type) const
    {
        StreamLayouts layouts;
        type->getDefaultSerialization()->enumerateStreams(
            [&](const ISerialization::SubstreamPath & path)
            {
                if (!ISerialization::isEphemeralSubcolumn(path, path.size()))
                    layouts.emplace(ISerialization::getSubstreamsCacheKeyForStream(path), resolveStreamLayout(path));
            },
            type);
        return layouts;
    }

    /// As `MergeTreeDataPartWriterWide::getSerializationSettings`.
    void setWriterSettings(ISerialization::SerializeBinaryBulkSettings & settings) const
    {
        settings.data_part_type = MergeTreeDataPartType::Wide;
        settings.low_cardinality_max_dictionary_size = config.low_cardinality_max_dictionary_size;
        settings.low_cardinality_use_single_dictionary_for_part = config.low_cardinality_use_single_dictionary_for_part;
        settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::SUFFIX;
        settings.stream_mark_getter = [](const ISerialization::SubstreamPath &) { return MarkInCompressedFile(); };
    }

    /// As `computeIndexGranularity` with the default settings: a granule holds `rows_per_granule` rows, fewer when they would
    /// exceed `bytes_per_granule`.
    static size_t granuleRows(const IColumn & column)
    {
        const size_t bytes_per_row = std::max<size_t>(column.byteSize() / std::max<size_t>(column.size(), 1), 1);
        return std::clamp<size_t>(bytes_per_granule / bytes_per_row, 1, rows_per_granule);
    }

    /// Starts a run for `column` (and for `source`, the same rows in the type of the column, with a target type): its
    /// serialization, its streams and the prefix.
    std::unique_ptr<Run> startRun(const IColumn & column, const IColumn & source, ISerialization::KindStack kind_stack) const
    {
        auto run = std::make_unique<Run>();
        run->kind_stack = std::move(kind_stack);
        run->writer.type = estimated_type;
        run->writer.serialization = estimated_type->getSerialization(*estimated_type->getSerializationInfo(column));
        setWriterSettings(run->writer.settings);

        Run * raw = run.get();
        run->writer.serialization->enumerateStreams(
            [&](const ISerialization::SubstreamPath & path)
            {
                if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                    return;

                const String key = ISerialization::getSubstreamsCacheKeyForStream(path);
                auto it = stream_layouts.find(key);
                const StreamLayout layout = it != stream_layouts.end() ? it->second : resolveStreamLayout(path);
                raw->streams.push_back({key, std::make_unique<CompressedSizeCalculator>(layout.codec, layout.block_size)});
            },
            estimated_type,
            column.getPtr());
        run->writer.settings.getter = [raw](const ISerialization::SubstreamPath & path) { return raw->streamFor(path); };
        run->writer.writePrefix(column);

        if (target_type)
        {
            run->source_counter = std::make_unique<NullWriteBuffer>();
            auto & source_writer = run->source_writer.emplace();
            source_writer.type = represented_type;
            source_writer.serialization = represented_type->getDefaultSerialization();
            setWriterSettings(source_writer.settings);
            source_writer.settings.getter
                = [counter = run->source_counter.get()](const ISerialization::SubstreamPath &) -> WriteBuffer * { return counter; };
            source_writer.writePrefix(source);
        }

        return run;
    }

    /// Ends the run as the writer ends a part: the suffix (the dictionary of a `LowCardinality` column, statistics) and the last
    /// compressed block of every stream. Idempotent.
    void finishRun(Run & run) const
    {
        if (run.finished)
            return;

        run.writer.writeSuffix();
        for (auto & stream : run.streams)
        {
            stream.calculator->finalize();
            run.compressed_bytes += stream.calculator->getCompressedBytes();
            if (!run.source_writer)
                run.uncompressed_bytes += stream.calculator->getUncompressedBytes();
        }

        if (run.source_writer)
        {
            run.source_writer->writeSuffix();
            run.source_counter->finalize();
            run.uncompressed_bytes += run.source_counter->count();
            run.source_counter.reset();
            run.source_writer.reset();
        }

        run.streams.clear();
        run.finished = true;
    }

    /// The current run, or a new one when there is none, when the state was read, or when the representation changed.
    Run & runFor(const Data & data_ref, const IColumn & rows, const IColumn & source) const
    {
        auto kind_stack = ISerialization::getKindStack(rows);
        if (data_ref.run && !data_ref.run->finished && data_ref.run->kind_stack == kind_stack)
            return *data_ref.run;

        auto run = startRun(rows, source, std::move(kind_stack));
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            run->uncompressed_bytes = data_ref.run->uncompressed_bytes;
            run->compressed_bytes = data_ref.run->compressed_bytes;
        }
        data_ref.run = std::move(run);
        return *data_ref.run;
    }

    /// `rows` of `plain_type` converted to `type`.
    ColumnPtr convertRows(const ColumnPtr & rows, const std::optional<Conversion> & conversion, const DataTypePtr & type) const
    {
        if (!conversion)
            return rows;

        ColumnsWithTypeAndName arguments{
            {rows, plain_type, ""},
            {DataTypeString().createColumnConst(rows->size(), type->getName()), std::make_shared<DataTypeString>(), ""}};
        return conversion->prepared->execute(arguments, type, rows->size(), /*dry_run=*/ false);
    }

    /// Serializes the rows of [offset, offset + limit) one granule at a time, as the writer serializes a block.
    void serializeRows(const Data & data_ref, const IColumn & rows, size_t offset, size_t limit) const
    {
        const size_t granule_rows = granuleRows(rows);
        if (!to_represented && !target_type)
        {
            for (size_t end = offset + limit; offset < end; offset += granule_rows)
                runFor(data_ref, rows, rows).writer.write(rows, offset, std::min(granule_rows, end - offset));
            return;
        }

        /// Converted rows are dense, as the conversion produces them.
        const ColumnPtr dense = rows.convertToFullColumnIfSparse();
        for (size_t end = offset + limit; offset < end; offset += granule_rows)
        {
            const size_t rows_in_granule = std::min(granule_rows, end - offset);
            const ColumnPtr granule = (offset == 0 && rows_in_granule == dense->size()) ? dense : dense->cut(offset, rows_in_granule);
            const ColumnPtr represented = convertRows(granule, to_represented, represented_type);
            if (!target_type)
            {
                runFor(data_ref, *represented, *represented).writer.write(*represented, 0, rows_in_granule);
                continue;
            }

            const ColumnPtr target = convertRows(granule, to_target, target_type);
            Run & run = runFor(data_ref, *target, *represented);
            run.source_writer->write(*represented, 0, rows_in_granule);
            run.writer.write(*target, 0, rows_in_granule);
        }
    }

    IColumn & bufferFor(Data & data_ref) const
    {
        if (!data_ref.buffer)
        {
            data_ref.buffer = plain_type->createColumn();
            data_ref.represented_type = represented_type;
        }
        return *data_ref.buffer;
    }

    /// Serializes rows after the rows added one at a time. With `write_whole_granules`, the rows that do not fill a granule wait
    /// in the buffer for the next rows, as the writer completes a granule with the next block. A sparse column is serialized as
    /// it is, since the buffer is not sparse.
    void addRows(Data & data_ref, const IColumn & rows, size_t offset, size_t limit) const
    {
        if (!write_whole_granules || rows.isSparse())
        {
            flushBuffer(data_ref);
            serializeRows(data_ref, rows, offset, limit);
            return;
        }

        const size_t granule_rows = granuleRows(rows);
        if (data_ref.buffer && !data_ref.buffer->empty())
        {
            const size_t taken = std::min(limit, granule_rows - std::min(granule_rows, data_ref.buffer->size()));
            data_ref.buffer->insertRangeFrom(rows, offset, taken);
            offset += taken;
            limit -= taken;
            if (data_ref.buffer->size() < granule_rows)
                return;
            flushBuffer(data_ref);
        }

        const size_t whole_granules_rows = limit - limit % granule_rows;
        serializeRows(data_ref, rows, offset, whole_granules_rows);
        if (whole_granules_rows < limit)
            bufferFor(data_ref).insertRangeFrom(rows, offset + whole_granules_rows, limit - whole_granules_rows);
    }

    void flushBuffer(const Data & data_ref) const
    {
        if (!data_ref.buffer || data_ref.buffer->empty())
            return;

        /// The rows come from a column of another type: they are measured as the function for that type measures them.
        if (data_ref.represented_type != represented_type && !data_ref.represented_type->equals(*represented_type))
        {
            const AggregateFunctionEstimateCompressionRatio producer({data_ref.represented_type}, parameters, config);
            producer.flushBuffer(data_ref);
            return;
        }

        serializeRows(data_ref, *data_ref.buffer, 0, data_ref.buffer->size());
        data_ref.buffer = data_ref.buffer->cloneEmpty();
    }

    /// Completes the pending work and returns the sizes of everything the state has seen.
    std::pair<UInt64, UInt64> totalSizes(const Data & data_ref) const
    {
        flushBuffer(data_ref);

        UInt64 uncompressed_size = data_ref.merged_uncompressed_size;
        UInt64 compressed_size = data_ref.merged_compressed_size;
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            uncompressed_size += data_ref.run->uncompressed_bytes;
            compressed_size += data_ref.run->compressed_bytes;
        }

        return {uncompressed_size, compressed_size};
    }

    /// Serializes the rows of [row_begin, row_end) that pass the condition of `-If` (`flags`) and are not NULL (`null_map`).
    void addRange(
        AggregateDataPtr __restrict place, const IColumn & column, size_t row_begin, size_t row_end, const UInt8 * flags, const UInt8 * null_map) const
    {
        if (row_begin >= row_end)
            return;

        const IColumn * rows = &column;
        size_t offset = row_begin;
        size_t limit = row_end - row_begin;
        ColumnPtr filtered;

        if (flags || null_map)
        {
            IColumn::Filter filter(limit);
            for (size_t i = row_begin; i < row_end; ++i)
                filter[i - row_begin] = (!flags || flags[i]) && (!null_map || !null_map[i]);

            const size_t passed = countBytesInFilter(filter);
            if (passed == 0)
                return;

            if (passed != limit)
            {
                filtered = (offset == 0 && limit == column.size())
                    ? column.filter(filter, passed)
                    : column.cut(offset, limit)->filter(filter, passed);
                rows = filtered.get();
                offset = 0;
                limit = passed;
            }
        }

        addRows(data(place), *rows, offset, limit);
    }

public:
    AggregateFunctionEstimateCompressionRatio(const DataTypes & arguments, const Array & params, EstimateCompressionRatioConfig config_)
        : IAggregateFunctionDataHelper({recursiveRemoveLowCardinality(arguments[0])}, params, std::make_shared<DataTypeFloat64>())
        , config(std::move(config_))
        , represented_type(arguments[0])
        , plain_type(argument_types[0])
        , target_type(config.target_type && !config.target_type->equals(*represented_type) ? config.target_type : nullptr)
        , estimated_type(target_type ? target_type : represented_type)
        , to_represented(represented_type != plain_type ? std::optional(createCast(plain_type, represented_type)) : std::nullopt)
        , to_target(target_type && !target_type->equals(*plain_type) ? std::optional(createCast(plain_type, target_type)) : std::nullopt)
        , write_whole_granules(represented_type != plain_type || recursiveRemoveLowCardinality(estimated_type) != estimated_type)
        , default_codec(CompressionCodecFactory::instance().getDefaultCodec())
        , codec_description(config.codec ? parseCodecDescription(*config.codec) : default_codec->getFullCodecDescription())
        , block_size_bytes(config.block_size_bytes.value_or(DBMS_DEFAULT_BUFFER_SIZE))
        , stream_layouts(buildStreamLayouts(estimated_type))
    {
    }

    String getName() const override { return "estimateCompressionRatio"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_ref = data(place);
        IColumn & buffer = bufferFor(data_ref);
        buffer.insertFrom(*columns[0], row_num);

        if (buffer.size() >= granuleRows(buffer))
            flushBuffer(data_ref);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        addRange(place, *columns[0], row_begin, row_end, flags, nullptr);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        addRange(place, *columns[0], row_begin, row_end, flags, null_map);
    }

    /// A sparse column is serialized as sparse, as a part stores it.
    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *) const override
    {
        addRange(place, *columns[0], row_begin, row_end, nullptr, nullptr);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto [uncompressed_size, compressed_size] = totalSizes(data(rhs));

        data(place).merged_uncompressed_size += uncompressed_size;
        data(place).merged_compressed_size += compressed_size;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        auto [uncompressed_size, compressed_size] = totalSizes(data(place));

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
        Data & data_ref = data(place);
        auto [uncompressed_size, compressed_size] = totalSizes(data_ref);

        /// The sizes are kept and the run is dropped: the next row of a growing window frame adds to them, while a repeated
        /// result for an unchanged frame does not count the run again.
        data_ref.merged_uncompressed_size = uncompressed_size;
        data_ref.merged_compressed_size = compressed_size;
        data_ref.run.reset();
        data_ref.buffer.reset();

        Float64 ratio = 0;
        if (compressed_size > 0)
            ratio = static_cast<Float64>(uncompressed_size) / static_cast<Float64>(compressed_size);

        assert_cast<ColumnFloat64 &>(to).getData().push_back(ratio);
    }
};

}

static AggregateFunctionPtr createAggregateFunctionEstimateCompressionRatio(
    const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings * settings)
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);
    if (parameters.size() > 3)
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Aggregate function {} accepts at most three parameters", name);

    const auto & type_factory = DataTypeFactory::instance();
    EstimateCompressionRatioConfig config;
    auto & codec = config.codec;
    auto & block_size_bytes = config.block_size_bytes;
    auto & target_type = config.target_type;
    for (const auto & param : parameters)
    {
        if (param.getType() == Field::Types::String)
        {
            const String & value = param.safeGet<String>();

            /// No codec is named like a data type: a string that names a type is the target type, any other string the codec.
            if (DataTypePtr type = type_factory.tryGet(value))
            {
                if (target_type)
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple data type parameters specified for {}. Expected at most one data type (target_type) parameter",
                        name);
                target_type = std::move(type);
            }
            else
            {
                /// A string starting with the name of a type family is a mistyped type: report its error, not an unknown codec.
                const String family(value.begin(), std::find_if_not(value.begin(), value.end(), isWordCharASCII));
                if (type_factory.hasNameOrAlias(family) || type_factory.hasNameOrAlias(Poco::toLower(family)))
                    type_factory.get(value);

                if (codec.has_value())
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple codec parameters specified for {}. Expected at most one string that is not a data type (codec)",
                        name);
                codec = value;
            }
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

    /// Without a query (a type being parsed) the writer's defaults apply.
    if (settings)
    {
        config.low_cardinality_max_dictionary_size = (*settings)[Setting::low_cardinality_max_dictionary_size];
        config.low_cardinality_use_single_dictionary_for_part = (*settings)[Setting::low_cardinality_use_single_dictionary_for_part];
    }

    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, std::move(config));
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the compression ratio of a given column without compressing it.

The estimate follows the layout of a MergeTree wide part: every substream of the column (the dictionary and the indexes of a `LowCardinality` column, the null map of a `Nullable` column, the sizes of an `Array` column, ...) is compressed on its own, the codec applies to the data substreams while the structural substreams keep only its generic codecs, as `CODEC(...)` on a table column does, and the rows are serialized one granule at a time, whatever the size of the blocks the query reads. A `LowCardinality` column is estimated with its dictionary encoding. The rows of each `GROUP BY` group and of each window frame are serialized as if they were a part of their own, so the result also depends on how the rows are split between groups, threads and frames.

To estimate another representation of the data, either convert the argument, for example `toLowCardinality(s)` or `CAST(ip AS IPv6)`, which gives the ratio of that representation, or pass the type as the `target_type` parameter, which keeps the serialized size of the column as it is as the baseline, so that the ratios for different target types are directly comparable. The on-disk size of a representation is its serialized size divided by its ratio (for an existing column, `column_data_uncompressed_bytes` in `system.parts_columns`).

The intermediate state records only sizes, not the type they were measured from: merging states computed from columns of different types, such as `LowCardinality(String)` and `String`, gives the ratio of both columns together.

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
        {"block_size_bytes", "Block size of compressed data. This is similar to setting both [`max_compress_block_size`](/reference/settings/merge-tree-settings/max#max_compress_block_size) and [`min_compress_block_size`](/reference/settings/merge-tree-settings/min#min_compress_block_size). The default value is 1 MiB (1048576 bytes). Maximum allowed value is 256 MiB (268435456 bytes).", {"UInt64"}},
        {"target_type", "Name of a data type. The column is estimated as if it were stored as this type: its rows are converted to it, as `accurateCast` does, and compressed with the codec, while the ratio keeps the serialized size of the column in its own type as the baseline. The ratios of one column for different target types are therefore directly comparable: the higher the ratio, the smaller the column would be on disk. The parameters can be given in any order: a string that names a data type is the target type, any other string is the codec.", {"String"}}
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
        "Comparing a column with another representation of its data",
        R"(
CREATE TABLE compression_estimate_strings
(
    `s` String
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_strings
SELECT concat('category_', toString(number % 50)) FROM system.numbers LIMIT 100_000;

-- `as_low_cardinality` is the ratio of the `LowCardinality(String)` representation, whose serialized size is already
-- small, so it is lower than `as_string` although the column takes fewer bytes on disk. `as_low_cardinality_vs_string`
-- keeps the size of the `String` column as the baseline: `LowCardinality(String)` stores it in 1361 / 231 ≈ 6 times
-- fewer bytes than `String`.
SELECT
    estimateCompressionRatio('LZ4')(s) AS as_string,
    estimateCompressionRatio('LZ4')(toLowCardinality(s)) AS as_low_cardinality,
    estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) AS as_low_cardinality_vs_string
FROM compression_estimate_strings
        )",
        R"(
┌──────────as_string─┬─as_low_cardinality─┬─as_low_cardinality_vs_string─┐
│ 230.69403714565004 │   116.280276816609 │           1361.0149942329874 │
└────────────────────┴────────────────────┴──────────────────────────────┘
        )"
    },
    {
        "Comparing a wider type for the same values under different codecs",
        R"(
-- `IPv6` stores the same addresses in 16 bytes instead of 4. The `IPv6` ratios keep the size of the `IPv4` column as the
-- baseline, so each pair compares the on-disk sizes of the two representations: with `LZ4` the 12 constant bytes of every
-- address cost space, with `ZSTD` they compress away.
SELECT
    estimateCompressionRatio('LZ4')(ip) AS lz4_as_ipv4,
    estimateCompressionRatio('LZ4', 'IPv6')(ip) AS lz4_as_ipv6,
    estimateCompressionRatio('ZSTD')(ip) AS zstd_as_ipv4,
    estimateCompressionRatio('ZSTD', 'IPv6')(ip) AS zstd_as_ipv6
FROM (SELECT toIPv4(number * 7919) AS ip FROM numbers(100000))
        )",
        R"(
┌────────lz4_as_ipv4─┬───────lz4_as_ipv6─┬───────zstd_as_ipv4─┬───────zstd_as_ipv6─┐
│ 0.9960283370061879 │ 0.753694516307123 │ 1.0557822554676324 │ 1.2303679415329154 │
└────────────────────┴───────────────────┴────────────────────┴────────────────────┘
        )"
    },
    {
        "A specialized codec on a Nullable column",
        R"(
-- As for a table column, `T64` applies to the values and the null map keeps only the generic codecs of the chain (none here).
SELECT estimateCompressionRatio('T64')(n) AS estimate
FROM (SELECT if(number % 10 = 0, NULL, toUInt32(number)) AS n FROM numbers(100000))
        )",
        R"(
┌───────────estimate─┐
│ 1.5993090984694611 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {25, 4};
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(
        "estimateCompressionRatio",
        {createAggregateFunctionEstimateCompressionRatio,
         documentation,
         {.is_order_dependent = true, .is_window_function = true, .created_with_low_cardinality_types = true}});
}
}
