#include <DataTypes/Serializations/SerializationQuantizedVector.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/VectorQuantization.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

/// Read the float vector at `row` from an Array(Float32) / Array(Float64) / Array(BFloat16) column.
void readVectorRow(const ColumnArray & col_arr, size_t row, std::vector<float> & out)
{
    const IColumn & nested = col_arr.getData();
    const auto & offsets = col_arr.getOffsets();
    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t size = offsets[row] - begin;
    out.resize(size);

    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(&nested))
    {
        const auto & data = f32->getData();
        for (size_t i = 0; i < size; ++i)
            out[i] = data[begin + i];
    }
    else if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(&nested))
    {
        const auto & data = f64->getData();
        for (size_t i = 0; i < size; ++i)
            out[i] = static_cast<float>(data[begin + i]);
    }
    else if (const auto * bf16 = typeid_cast<const ColumnBFloat16 *>(&nested))
    {
        const auto & data = bf16->getData();
        for (size_t i = 0; i < size; ++i)
            out[i] = static_cast<float>(data[begin + i]);
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Column with a Quantize codec must be Array(Float32), Array(Float64) or Array(BFloat16)");
}

}

SerializationQuantizedVector::SerializationQuantizedVector(const SerializationPtr & nested_, const QuantizeCodecParams & params_)
    : SerializationWrapper(nested_)
    , params(params_)
    , bytes_per_vector(VectorQuantization::bytesPerVector(params_.method, params_.dimensions, params_.bits))
    , codes_type(std::make_shared<DataTypeFixedString>(bytes_per_vector))
    , codes_serialization(SerializationNamed::create(
          codes_type->getDefaultSerialization(), subcolumn_name, ISerialization::Substream::QuantizedCodes))
{
}

void SerializationQuantizedVector::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    /// The derived code stream, exposed as the `<column>.quantized` subcolumn.
    settings.path.push_back(Substream::QuantizedCodes);
    settings.path.back().name_of_substream = subcolumn_name;

    auto codes_data = SubstreamData(codes_serialization)
                          .withType(data.type ? codes_type : nullptr)
                          .withColumn(nullptr)
                          .withSerializationInfo(data.serialization_info);

    if (data.column && typeid_cast<const ColumnArray *>(data.column.get()))
        codes_data.withLazyColumnCreator([this, col = data.column]() -> ColumnPtr { return encodeCodes(*col, 0, col->size()); });

    settings.path.back().data = codes_data;
    callback(settings.path);
    settings.path.pop_back();

    /// The full-precision array streams, at the top level: their layout is identical to a plain Array column, so a
    /// reader that only needs the vectors reads them exactly as it would without the codec.
    nested_serialization->enumerateStreams(settings, callback, data);
}

void SerializationQuantizedVector::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// Full-precision data, written exactly like a plain Array column (uses the array state from the prefix).
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);

    /// The codes are written only into on-disk parts, never into transport (Native) streams. In Native the column is
    /// reconstructed from its type name alone (a plain `Array(Float32)`), so a reader would not consume the codes and
    /// the stream would desync. On disk the codes go to their own stream/region (written last, after the float data),
    /// which a full-precision reader simply skips.
    if (settings.native_format)
        return;

    /// Derived quantized codes into the separate `.quantized` stream. The codes are a stateless per-row function of the
    /// vector, so no prefix/suffix state is needed.
    size_t count = limit;
    if (count == 0 || offset + count > column.size())
        count = column.size() - offset;

    auto codes_column = encodeCodes(column, offset, count);
    SerializeBinaryBulkStatePtr codes_state;
    codes_serialization->serializeBinaryBulkWithMultipleStreams(*codes_column, 0, codes_column->size(), settings, codes_state);
}

ColumnPtr SerializationQuantizedVector::encodeCodes(const IColumn & column, size_t offset, size_t count) const
{
    const auto * col_arr = typeid_cast<const ColumnArray *>(&column);
    if (!col_arr)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with a Quantize codec must be an Array");

    auto res = ColumnFixedString::create(bytes_per_vector);
    auto & chars = res->getChars();
    chars.resize_fill(count * bytes_per_vector, 0);

    std::vector<float> buf;
    for (size_t i = 0; i < count; ++i)
    {
        readVectorRow(*col_arr, offset + i, buf);
        if (buf.size() != params.dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Vector at row {} has {} elements but the Quantize codec was declared with {} dimensions",
                offset + i, buf.size(), params.dimensions);

        VectorQuantization::encode(
            params.method, buf.data(), params.dimensions, params.bits, reinterpret_cast<char *>(&chars[i * bytes_per_vector]));
    }

    return res;
}

}
