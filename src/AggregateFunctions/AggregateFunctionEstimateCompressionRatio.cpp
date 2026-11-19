#include <cstdio>
#include <Formats/FormatSettings.h>
#include <cstdlib>
#include <memory>
#include <optional>
#include <AggregateFunctions/AggregateFunctionEstimateCompressionRatioWithTarget.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
extern const int UNKNOWN_CODEC;
extern const int UNKNOWN_QUERY_PARAMETER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

ASTPtr parseEstimateCompressionRatioCodec(const String & codec)
{
    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(ast);
    return ast;
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
            return CompressionCodecFactory::instance().get(parseEstimateCompressionRatioCodec(*codec), argument_types[0]);
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
        return createAggregateFunctionEstimateCompressionRatioWithTarget(
            arguments, parameters, std::move(target_type), codec, block_size_bytes, settings);

    /// The function without a target type gets its codec at the first row. A query checks it here as well, so that a
    /// misspelled codec, or a type name taken for a codec, is an error also without rows. Loading a table does not, so that
    /// the server starts with a stored state type that names such a codec (its states are empty: any row fails).
    if (codec && settings)
    {
        try
        {
            (void)CompressionCodecFactory::instance().get(parseEstimateCompressionRatioCodec(*codec), arguments[0]);
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
- values converted as `CAST` converts them, as `ALTER TABLE ... MODIFY COLUMN` does, with the settings of the query that adds them (a `SETTINGS` clause in a subquery doesn't apply).

The estimate is approximate, and it does not read the settings of any table.
A state kept in a `Memory` table keeps the conversion settings of the query that added its rows, whichever query reads it.
A table sizes its granules from its insert blocks and from all its columns together, while the function sees one column and no insert blocks, so the granules of wide rows can differ.
The numerator of a `LowCardinality` column is the size of its values, not of its dictionary.
As `LowCardinality`, the estimated size of wide values with few distinct values is a few percent too large (up to about 11% for 1.2 KB values with 1000 distinct ones), because the function writes the rows of a granule in pieces of 1 MiB, where a table writes them at once.
The result can depend on how a query splits the rows between threads.
The rows of a group that one thread gathers join the rows of the other threads while they are fewer than a granule and less than 1 MiB; beyond that, they are estimated as a separate part.
States that are serialized (in a distributed query, an external aggregation, or an `AggregateFunction` column) are separate parts too.
Every part adds its own block headers and, for `LowCardinality`, its own dictionary, which matters when a group compresses to a few kilobytes: the ratio of such a group as `LowCardinality` can drop by half with several threads.
`max_threads = 1` gives a more stable result, though still an estimate.

`Dynamic` and `JSON` are not supported as target types, nor as arguments with `target_type`, and neither is an argument with `LowCardinality` inside a `Variant`: convert them in the query first.

With or without `target_type`, in a window with a growing frame (for example `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`), the new rows of every step are compressed on their own, as a separate part, so the result is far lower than for the same rows without the window (0.14 instead of 8.8 for 1000 short strings).

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
