#include <algorithm>
#include <memory>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
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
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

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

using StreamLayouts = UnorderedMapWithMemoryTracking<UInt128, StreamLayout, ISerialization::SubstreamsCacheKeyHash>;

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
        UInt128 key;
        std::unique_ptr<CompressedSizeCalculator> calculator;
    };
    /// A handful of substreams: a linear search beats hashing.
    VectorWithMemoryTracking<Stream> streams;

    bool finished = false;
    /// The sizes of this run and of the finished runs before it since the state was last read.
    UInt64 uncompressed_bytes = 0;
    UInt64 compressed_bytes = 0;

    ~Run()
    {
        /// Left over by an exception; the finalized ones are gone already.
        for (auto & stream : streams)
            stream.calculator->cancel();
    }

    WriteBuffer * streamFor(const ISerialization::SubstreamPath & path)
    {
        const UInt128 key = ISerialization::getSubstreamsCacheKeyForStream(path);
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

    /// Pending work: rows added one at a time and the current run. Completed whenever the state is read, also through a const
    /// pointer (`serialize`, `merge`), which does not change what the state stands for.
    mutable MutableColumnPtr buffer;
    mutable std::unique_ptr<Run> run;
};

class AggregateFunctionEstimateCompressionRatio final
    : public IAggregateFunctionDataHelper<AggregateFunctionEstimateCompressionRatioData, AggregateFunctionEstimateCompressionRatio>
{
private:
    /// The defaults of `index_granularity` and `index_granularity_bytes`.
    static constexpr size_t rows_per_granule = 8192;
    static constexpr size_t bytes_per_granule = 10 * 1024 * 1024;

    const DataTypePtr argument_type;
    const CompressionCodecPtr default_codec;
    /// The `codec` parameter, or the default codec, as the `CODEC` clause of a column.
    const ASTPtr codec_description;
    const size_t block_size_bytes;
    const size_t low_cardinality_max_dictionary_size;
    const bool low_cardinality_use_single_dictionary_for_part;
    /// The layout of every substream of the default serialization of the argument type; building it validates the codec against
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
        settings.low_cardinality_max_dictionary_size = low_cardinality_max_dictionary_size;
        settings.low_cardinality_use_single_dictionary_for_part = low_cardinality_use_single_dictionary_for_part;
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

    /// Starts a run for `column`: its serialization, its streams and the prefix.
    std::unique_ptr<Run> startRun(const IColumn & column, ISerialization::KindStack kind_stack) const
    {
        auto run = std::make_unique<Run>();
        run->kind_stack = std::move(kind_stack);
        run->writer.type = argument_type;
        run->writer.serialization = argument_type->getSerialization(*argument_type->getSerializationInfo(column));
        setWriterSettings(run->writer.settings);

        Run * raw = run.get();
        run->writer.serialization->enumerateStreams(
            [&](const ISerialization::SubstreamPath & path)
            {
                if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                    return;

                const UInt128 key = ISerialization::getSubstreamsCacheKeyForStream(path);
                auto it = stream_layouts.find(key);
                const StreamLayout layout = it != stream_layouts.end() ? it->second : resolveStreamLayout(path);
                raw->streams.push_back({key, std::make_unique<CompressedSizeCalculator>(layout.codec, layout.block_size)});
            },
            argument_type,
            column.getPtr());
        run->writer.settings.getter = [raw](const ISerialization::SubstreamPath & path) { return raw->streamFor(path); };
        run->writer.writePrefix(column);
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
            run.uncompressed_bytes += stream.calculator->getUncompressedBytes();
        }

        run.streams.clear();
        run.finished = true;
    }

    /// The current run, or a new one when there is none, when the state was read, or when the representation changed.
    Run & runFor(const Data & data_ref, const IColumn & rows) const
    {
        auto kind_stack = ISerialization::getKindStack(rows);
        if (data_ref.run && !data_ref.run->finished && data_ref.run->kind_stack == kind_stack)
            return *data_ref.run;

        auto run = startRun(rows, std::move(kind_stack));
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            run->uncompressed_bytes = data_ref.run->uncompressed_bytes;
            run->compressed_bytes = data_ref.run->compressed_bytes;
        }
        data_ref.run = std::move(run);
        return *data_ref.run;
    }

    /// Serializes the rows of [offset, offset + limit) one granule at a time, as the writer serializes a block.
    void serializeRows(const Data & data_ref, const IColumn & rows, size_t offset, size_t limit) const
    {
        const size_t granule_rows = granuleRows(rows);
        for (size_t end = offset + limit; offset < end; offset += granule_rows)
            runFor(data_ref, rows).writer.write(rows, offset, std::min(granule_rows, end - offset));
    }

    void flushBuffer(const Data & data_ref) const
    {
        if (!data_ref.buffer || data_ref.buffer->empty())
            return;

        serializeRows(data_ref, *data_ref.buffer, 0, data_ref.buffer->size());
        /// A fresh column rather than `popBack`: a `LowCardinality` buffer keeps every key ever inserted.
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

        const Data & data_ref = data(place);
        /// Rows added one at a time go first, in their order.
        flushBuffer(data_ref);
        serializeRows(data_ref, *rows, offset, limit);
    }

public:
    AggregateFunctionEstimateCompressionRatio(
        const DataTypes & arguments,
        const Array & params,
        std::optional<String> codec,
        std::optional<UInt64> block_size_bytes_,
        const Settings * settings)
        : IAggregateFunctionDataHelper(arguments, params, std::make_shared<DataTypeFloat64>())
        , argument_type(arguments[0])
        , default_codec(CompressionCodecFactory::instance().getDefaultCodec())
        , codec_description(codec ? parseCodecDescription(*codec) : default_codec->getFullCodecDescription())
        , block_size_bytes(block_size_bytes_.value_or(DBMS_DEFAULT_BUFFER_SIZE))
        /// The settings the MergeTree writer takes from the query; their defaults when there is no query (a type being parsed).
        , low_cardinality_max_dictionary_size(settings ? (*settings)[Setting::low_cardinality_max_dictionary_size] : 8192)
        , low_cardinality_use_single_dictionary_for_part(settings && (*settings)[Setting::low_cardinality_use_single_dictionary_for_part])
        , stream_layouts(buildStreamLayouts(argument_type))
    {
    }

    String getName() const override { return "estimateCompressionRatio"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// The state is two sizes whatever the argument type: a state produced from `LowCardinality(T)` is interchangeable with one
    /// produced from `T`.
    DataTypePtr getNormalizedStateType() const override
    {
        DataTypes normalized_argument_types;
        normalized_argument_types.reserve(argument_types.size());
        for (const auto & type : argument_types)
            normalized_argument_types.push_back(recursiveRemoveLowCardinality(type)->getNormalizedType());
        return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), normalized_argument_types, parameters);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_ref = data(place);
        if (!data_ref.buffer)
            data_ref.buffer = argument_type->createColumn();
        data_ref.buffer->insertFrom(*columns[0], row_num);

        if (data_ref.buffer->size() >= granuleRows(*data_ref.buffer))
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
    if (parameters.size() > 2)
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Aggregate function {} accepts at most two parameters", name);

    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;
    for (const auto & param : parameters)
    {
        if (param.getType() == Field::Types::String)
        {
            if (codec.has_value())
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "Multiple string type parameters specified for {}. Expected at most one string type (codec) parameter",
                    name);
            codec = param.safeGet<String>();
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
                "Invalid parameter type for {}. Expected String (codec) and/or UInt64 (block_size_bytes)",
                name);
        }
    }

    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, codec, block_size_bytes, settings);
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the compression ratio of a given column without compressing it.

The estimate follows the layout of a MergeTree wide part: every substream of the column (the dictionary and the indexes of a `LowCardinality` column, the null map of a `Nullable` column, the sizes of an `Array` column, ...) is compressed on its own, the codec applies to the data substreams while the structural substreams keep only its generic codecs, as `CODEC(...)` on a table column does, and the rows are serialized one granule at a time. A `LowCardinality` column is estimated with its dictionary encoding. The rows of each `GROUP BY` group and of each window frame are serialized as if they were a part of their own, so the result also depends on how the rows are split between groups, threads and frames.

To estimate another representation of the data, convert the argument, for example `toLowCardinality(s)` or `CAST(ip AS IPv6)`. The ratio relates the serialized size of a representation to its compressed size, so to compare the on-disk sizes of two representations, divide their serialized sizes by their ratios (for an existing column, `column_data_uncompressed_bytes` in `system.parts_columns`).

<Note>
For the examples below, the result will differ based on the default compression codec of the server.
See [Column Compression Codecs](/reference/statements/create/table#column_compression_codec).
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "estimateCompressionRatio([codec, block_size_bytes])(column)";
    FunctionDocumentation::Arguments arguments = {
        {"column", "Column of any type.", {"Any"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"codec", "String containing a compression codec or multiple comma-separated codecs in a single string.", {"String"}},
        {"block_size_bytes", "Block size of compressed data. This is similar to setting both [`max_compress_block_size`](/reference/settings/merge-tree-settings/max#max_compress_block_size) and [`min_compress_block_size`](/reference/settings/merge-tree-settings/min#min_compress_block_size). The default value is 1 MiB (1048576 bytes). Maximum allowed value is 256 MiB (268435456 bytes).", {"UInt64"}}
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
         {.is_order_dependent = true, .is_window_function = true, .accepts_low_cardinality_arguments = true}});
}
}
