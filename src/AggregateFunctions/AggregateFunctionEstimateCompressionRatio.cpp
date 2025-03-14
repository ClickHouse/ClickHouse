#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
#include <fcntl.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <Poco/Exception.h>
#include <Poco/Logger.h>
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

    std::unique_ptr<NullWriteBuffer> null_buf;
    std::unique_ptr<CompressedWriteBuffer> compressed_buf;

    [[maybe_unused]] ~AggregationFunctionEstimateCompressionRatioData()
    {
        if (compressed_buf)
            compressed_buf->finalize();
        if (null_buf)
            null_buf->finalize();
    }
};

class AggregateFunctionEstimateCompressionRatio final
    : public IAggregateFunctionDataHelper<AggregationFunctionEstimateCompressionRatioData, AggregateFunctionEstimateCompressionRatio>
{
private:
    SerializationPtr serialization;
    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;


    void createBuffersIfNeeded(AggregateDataPtr __restrict place) const
    {
        if (!data(place).null_buf)
            data(place).null_buf = std::make_unique<NullWriteBuffer>();
        if (!data(place).compressed_buf)
            data(place).compressed_buf = std::make_unique<CompressedWriteBuffer>(
                *data(place).null_buf, getCodecOrDefault(), block_size_bytes.value_or(DBMS_DEFAULT_BUFFER_SIZE));
    }

    std::pair<UInt64, UInt64> finalizeAndGetSizes(ConstAggregateDataPtr __restrict place) const
    {
        UInt64 uncompressed_size = data(place).merged_uncompressed_size;
        UInt64 compressed_size = data(place).merged_compressed_size;
        if (data(place).compressed_buf)
        {
            data(place).compressed_buf->finalize();
            data(place).null_buf->finalize();

            uncompressed_size += data(place).compressed_buf->getUncompressedBytes();
            compressed_size += data(place).compressed_buf->getCompressedBytes();
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

        createBuffersIfNeeded(place);

        DataTypePtr type_ptr = argument_types[0];
        SerializationInfoPtr info = type_ptr->getSerializationInfo(*column);
        SerializationPtr type_serialization_ptr = type_ptr->getSerialization(*info);

        type_serialization_ptr->serializeBinary(*column, row_num, *data(place).compressed_buf, {});
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


        createBuffersIfNeeded(place);

        DataTypePtr type_ptr = argument_types[0];
        SerializationInfoPtr info = type_ptr->getSerializationInfo(*column);
        SerializationPtr type_serialization_ptr = type_ptr->getSerialization(*info);

        ISerialization::SerializeBinaryBulkSettings settings;

        settings.getter = [place](ISerialization::SubstreamPath) -> WriteBuffer * { return data(place).compressed_buf.get(); };

        ISerialization::SerializeBinaryBulkStatePtr state;
        type_serialization_ptr->serializeBinaryBulkStatePrefix(*column, settings, state);
        type_serialization_ptr->serializeBinaryBulkWithMultipleStreams(*column, row_begin, row_end - row_begin, settings, state);
        type_serialization_ptr->serializeBinaryBulkStateSuffix(settings, state);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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

        Float64 ratio = 0;
        if (compressed_size > 0)
        {
            ratio = static_cast<Float64>(uncompressed_size) / compressed_size;
        }

        assert_cast<ColumnFloat64 &>(to).getData().push_back(ratio);
    }
};
}

AggregateFunctionPtr createAggregateFunctionEstimateCompressionRatio(
    const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
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
            block_size_bytes = param.safeGet<UInt64>();
        }
        else
        {
            throw Exception(
                ErrorCodes::UNKNOWN_QUERY_PARAMETER,
                "Invalid parameter type for {}. Expected String (codec) and/or UInt64 (block_size_bytes)",
                name);
        }
    }

    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, codec, block_size_bytes);
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        "estimateCompressionRatio",
        {createAggregateFunctionEstimateCompressionRatio, {.is_order_dependent = true, .is_window_function = true}});
}
}
