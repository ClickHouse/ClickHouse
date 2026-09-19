#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
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
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
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
    /// When set, simulate compression as if the column had this type instead of argument_types[0]
    /// (e.g. to compare `String` vs `LowCardinality(String)`.
    DataTypePtr cast_to_type;
    mutable InternalCastFunctionCache cast_function_cache;

    /// Returns argument_types[0], or cast_to_type if a `cast_to` parameter was given.
    const DataTypePtr & effectiveType() const { return cast_to_type ? cast_to_type : argument_types[0]; }

    /// Slices [row_begin, row_begin + row_count) out of `column` and casts it to cast_to_type.
    /// Only called when cast_to_type is set.
    ColumnPtr castSlice(const IColumn & column, size_t row_begin, size_t row_count) const
    {
        ColumnWithTypeAndName arg{column.cut(row_begin, row_count), argument_types[0], ""};
        return castColumn(arg, cast_to_type, &cast_function_cache);
    }

    /// Serializes [row_begin, row_begin + row_count) of `column` using argument_types[0]'s own
    /// serialization (i.e. the column's actual, uncast representation) into a buffer that only counts
    /// bytes, and returns that count. Only called when cast_to_type is set.
    UInt64 countOriginalUncompressedBytes(const IColumn & column, size_t row_begin, size_t row_count) const
    {
        SerializationInfoPtr info = argument_types[0]->getSerializationInfo(column);
        SerializationPtr original_serialization = argument_types[0]->getSerialization(*info);

        NullWriteBuffer counter;
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&counter](ISerialization::SubstreamPath) -> WriteBuffer * { return &counter; };

        ISerialization::SerializeBinaryBulkStatePtr state;
        original_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
        original_serialization->serializeBinaryBulkWithMultipleStreams(column, row_begin, row_count, settings, state);
        original_serialization->serializeBinaryBulkStateSuffix(settings, state);

        counter.finalize();
        return counter.count();
    }

    void resetCalculatorIfNeeded(AggregateDataPtr __restrict place) const
    {
        Data & data_ref = data(place);

        /// When aggregating on windows transformed columns, the function WindowTransform::appendChunk
        /// calls updateAggregationState + writeOutCurrentRow in a loop.
        /// writeOutCurrentRow finalizes the buffer to flush and compute sizes, but doesn't deletes it.
        /// Ideally on finalized buffers we could "reinitialize" without reconstructing the whole object buffer.
        if (!data_ref.calculator || data_ref.calculator->isFinalized())
            data_ref.calculator = std::make_unique<CompressedSizeCalculator>(
                getCodecOrDefault(), block_size_bytes.value_or(DBMS_DEFAULT_BUFFER_SIZE));
    }

    std::pair<UInt64, UInt64> finalizeAndGetSizes(ConstAggregateDataPtr __restrict place) const
    {
        const Data & data_ref = data(place);

        UInt64 uncompressed_size = data_ref.merged_uncompressed_size;
        UInt64 compressed_size = data_ref.merged_compressed_size;

        if (data_ref.calculator)
        {
            data_ref.calculator->finalize();

            /// The ratio's uncompressed side must always reflect the column's actual, current size, so
            /// that ratios stay comparable across different `cast_to` choices for the same data: with a
            /// `cast_to`, `merged_uncompressed_size` is already the original (uncast) size, kept up to
            /// date by `add`/`addBatchSinglePlace` on every call; the calculator's own uncompressed count
            /// (bytes of the *casted* representation) would give a different, incomparable baseline.
            if (!cast_to_type)
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
            return CompressionCodecFactory::instance().get(ast, effectiveType());
        }
        return CompressionCodecFactory::instance().getDefaultCodec();
    }

public:
    [[maybe_unused]] explicit AggregateFunctionEstimateCompressionRatio(
        const DataTypes & arguments,
        const Array & params,
        std::optional<String> codec_,
        std::optional<UInt64> block_size_bytes_,
        DataTypePtr cast_to_type_)
        : IAggregateFunctionDataHelper(arguments, params, createResultType())
        , serialization(this->result_type->getDefaultSerialization())
        , codec(codec_)
        , block_size_bytes(block_size_bytes_)
        , cast_to_type(std::move(cast_to_type_))
    {
    }


    String getName() const override { return "estimateCompressionRatio"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        resetCalculatorIfNeeded(place);

        const IColumn * original_column = columns[0];
        if (cast_to_type)
            data(place).merged_uncompressed_size += countOriginalUncompressedBytes(*original_column, row_num, 1);

        ColumnPtr casted_holder;
        const IColumn * column = original_column;
        size_t effective_row = row_num;
        if (cast_to_type)
        {
            casted_holder = castSlice(*column, row_num, 1);
            column = casted_holder.get();
            effective_row = 0;
        }

        const DataTypePtr & type_ptr = effectiveType();
        SerializationInfoPtr info = type_ptr->getSerializationInfo(*column);
        SerializationPtr type_serialization_ptr = type_ptr->getSerialization(*info);

        type_serialization_ptr->serializeBinary(*column, effective_row, *data(place).calculator, {});
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
        resetCalculatorIfNeeded(place);

        const IColumn * original_column = columns[0];
        size_t row_count = row_end - row_begin;
        if (cast_to_type)
            data(place).merged_uncompressed_size += countOriginalUncompressedBytes(*original_column, row_begin, row_count);

        ColumnPtr casted_holder;
        const IColumn * column = original_column;
        size_t effective_begin = row_begin;
        size_t effective_count = row_count;
        if (cast_to_type)
        {
            casted_holder = castSlice(*column, row_begin, effective_count);
            column = casted_holder.get();
            effective_begin = 0;
        }

        const DataTypePtr & type_ptr = effectiveType();
        SerializationInfoPtr info = type_ptr->getSerializationInfo(*column);
        SerializationPtr type_serialization_ptr = type_ptr->getSerialization(*info);

        ISerialization::SerializeBinaryBulkSettings settings;

        settings.getter = [place](ISerialization::SubstreamPath) -> WriteBuffer * { return data(place).calculator.get(); };

        ISerialization::SerializeBinaryBulkStatePtr state;
        type_serialization_ptr->serializeBinaryBulkStatePrefix(*column, settings, state);
        type_serialization_ptr->serializeBinaryBulkWithMultipleStreams(*column, effective_begin, effective_count, settings, state);
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
    const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);
    if (parameters.size() > 3)
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Aggregate function {} accepts at most three parameters", name);

    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;
    DataTypePtr cast_to_type;
    for (const auto & param : parameters)
    {
        if (param.getType() == Field::Types::String)
        {
            const String & value = param.safeGet<String>();

            /// Two kinds of String parameters are accepted: a codec spec (e.g. `'ZSTD(1)'`) and a
            /// type name to cast the column to before compressing (e.g. `'LowCardinality(String)'`).
            /// Neither a codec name nor a codec chain is ever a valid data type name, so trying to
            /// resolve it as a type first is an unambiguous way to tell the two apart.
            if (DataTypePtr parsed_type = DataTypeFactory::instance().tryGet(value))
            {
                if (cast_to_type)
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple cast_to parameters specified for {}. Expected at most one parameter naming the type to cast to",
                        name);
                cast_to_type = std::move(parsed_type);
            }
            else
            {
                if (codec.has_value())
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple codec parameters specified for {}. Expected at most one parameter naming a codec",
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
                "Invalid parameter type for {}. Expected String (a codec, or a type name to cast to) and/or UInt64 (block_size_bytes)",
                name);
        }
    }

    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, codec, block_size_bytes, cast_to_type);
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the compression ratio of a given column without compressing it.

<Note>
For the examples below, the result will differ based on the default compression codec of the server.
See [Column Compression Codecs](/reference/statements/create/table#column_compression_codec).
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "estimateCompressionRatio([codec, block_size_bytes, cast_to])(column)";
    FunctionDocumentation::Arguments arguments = {
        {"column", "Column of any type.", {"Any"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"codec", "String containing a compression codec or multiple comma-separated codecs in a single string.", {"String"}},
        {"block_size_bytes", "Block size of compressed data. This is similar to setting both [`max_compress_block_size`](/reference/settings/merge-tree-settings/max#max_compress_block_size) and [`min_compress_block_size`](/reference/settings/merge-tree-settings/min#min_compress_block_size). The default value is 1 MiB (1048576 bytes). Maximum allowed value is 256 MiB (268435456 bytes).", {"UInt64"}},
        {"cast_to", "Name of a data type. When given, `column` is cast to this type before estimating compression, so a type change can be evaluated together with a codec change (for example, comparing `String` against `LowCardinality(String)`). The returned ratio always divides by the size of `column` as it actually is today, not by the size of the cast-to representation, so ratios stay comparable across different `cast_to` choices for the same column. For a type whose serialization depends on more than one row at a time, such as `LowCardinality`, the cast is applied independently per execution block rather than once for the whole input, so the result can vary with `max_block_size`.", {"String"}}
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
        "Comparing against a different type via `cast_to`",
        R"(
CREATE TABLE compression_estimate_strings
(
    `str` String
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_strings
SELECT toString(number % 50) FROM system.numbers LIMIT 100_000;

SELECT
    estimateCompressionRatio('ZSTD')(str) AS plain,
    estimateCompressionRatio('ZSTD', 'LowCardinality(String)')(str) AS as_low_cardinality
FROM compression_estimate_strings
        )",
        R"(
┌─plain─┬─as_low_cardinality─┐
│  2000 │ 1147.5409836065573 │
└───────┴────────────────────┘
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
