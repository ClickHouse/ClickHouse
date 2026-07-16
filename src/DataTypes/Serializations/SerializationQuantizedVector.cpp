#include <DataTypes/Serializations/SerializationQuantizedVector.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProductQuantizer.h>
#include <Common/VectorQuantizer.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <cstring>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

/// Read the float vector at `row` of an Array(Float32|Float64|BFloat16) column into `result`.
void readVectorRow(const ColumnArray & col_arr, size_t row, std::vector<float> & result)
{
    const IColumn & arr_data = col_arr.getData();
    const auto & arr_offsets = col_arr.getOffsets();
    const size_t begin = row == 0 ? 0 : arr_offsets[row - 1];
    const size_t size = arr_offsets[row] - begin;
    result.resize(size);

    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(&arr_data))
    {
        for (size_t i = 0; i < size; ++i)
            result[i] = f32->getData()[begin + i];
    }
    else if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(&arr_data))
    {
        for (size_t i = 0; i < size; ++i)
            result[i] = static_cast<float>(f64->getData()[begin + i]);
    }
    else if (const auto * bf16 = typeid_cast<const ColumnBFloat16 *>(&arr_data))
    {
        for (size_t i = 0; i < size; ++i)
            result[i] = static_cast<float>(bf16->getData()[begin + i]);
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Column with a Quantize codec must be Array(Float32|Float64|BFloat16)");
}

/// Maximum number of vectors used for training the per-part codebook (bounds the costs for k-means; assumes that the part's
/// first block is a representative sample)
constexpr size_t PRODUCT_QUANTIZATION_MAX_TRAINING_VECTORS = 100'000;

/// Maximum number of training sample by a byte budget
constexpr size_t PRODUCT_QUANTIZATION_MAX_TRAINING_BYTES = 256 * 1024 * 1024;

/// Write state for the `product` method: the codebook is trained from the first block and reused for the whole part, then
/// written once at the suffix.
struct SerializedStateProductQuantization : public ISerialization::SerializeBinaryBulkState
{
    ISerialization::SerializeBinaryBulkStatePtr nested; /// the full-precision array's own write state
    std::vector<float> codebook;
    bool trained = false;
};

/// Holds the part's single codebook value once read. Broadcasted to every granule without re-reading the stream.
struct DeserializedStateProductQuantization : public ISerialization::DeserializeBinaryBulkState
{
    ColumnPtr codebook; /// a one-row column, or null until the first granule reads it
};

/// Read serialization for the codebook subcolumn. The codebook is stored as a SINGLE value per part (written
/// once at the suffix; every granule's mark points at it).
class SerializationProductQuantizationCodebook final : public SerializationWrapper
{
public:
    SerializationProductQuantizationCodebook(const SerializationPtr & nested_, const DataTypePtr & value_type_)
        : SerializationWrapper(nested_)
        , value_type(value_type_)
    {}

    /// Created via the serialization pool so it carries a stable hash (required when attached to a column), mirroring
    /// SerializationNamed::create.
    static SerializationPtr create(const SerializationPtr & nested_, const DataTypePtr & value_type_)
    {
        if (!nested_->supportsPooling())
            return std::shared_ptr<ISerialization>(new SerializationProductQuantizationCodebook(nested_, value_type_));
        SipHash hash;
        hash.update("ProductQuantizationCodebook");
        hash.update(nested_->getHash());
        return ISerialization::pooled(hash.get128(), [&] { return new SerializationProductQuantizationCodebook(nested_, value_type_); });
    }

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & /*settings*/,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * /*cache*/) const override
    {
        state = std::make_shared<DeserializedStateProductQuantization>();
    }

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t /*rows_offset*/,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * /*cache*/) const override
    {
        /// The reader keeps `state` across granules (it is `deserialize_binary_bulk_state_map[name]`). Ensure it is our
        /// type and reuse it; if a prior pass populated the map with a different state, replace it on the first call.
        auto * state_pq = typeid_cast<DeserializedStateProductQuantization *>(state.get());
        if (!state_pq)
        {
            auto new_state = std::make_shared<DeserializedStateProductQuantization>();
            state_pq = new_state.get();
            state = std::move(new_state);
        }
        const size_t prev_size = column ? column->size() : 0;

        /// Read the part's single codebook value exactly once (the stream holds one value for the whole part); every
        /// granule reuses it. The first granule of a read range is positioned at the codebook's start by its mark.
        if (!state_pq->codebook)
        {
            settings.path.push_back(Substream::Regular);
            ReadBuffer * stream = settings.getter(settings.path);
            settings.path.pop_back();
            if (!stream)
                return;

            auto value = value_type->createColumn();
            nested_serialization->deserializeBinaryBulk(*value, *stream, /*rows_offset=*/0, /*limit=*/1, /*avg_value_size_hint=*/0.0);
            if (value->size() != 1)
                throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                    "Expected exactly one per-part PQ codebook value but read {}", value->size());
            state_pq->codebook = std::move(value);
        }

        column = ColumnConst::create(state_pq->codebook, prev_size + limit);
    }

private:
    DataTypePtr value_type;
};

}

SerializationQuantizedVector::SerializationQuantizedVector(const SerializationPtr & nested_, const QuantizedCodecParams & params_)
    : SerializationWrapper(nested_)
    , params(params_)
    , is_product_quantization(params_.method == "product")
    , bytes_per_vector(is_product_quantization
          ? ProductQuantizer::bytesPerVector(params_.dimensions, params_.m, params_.bits)
          : VectorQuantizer::bytesPerVector(params_.method, params_.dimensions, params_.bits))
    , codes_type(std::make_shared<DataTypeFixedString>(bytes_per_vector))
    , codes_serialization(SerializationNamed::create(
          codes_type->getDefaultSerialization(), subcolumn_name, ISerialization::Substream::QuantizedCodes))
{
    if (is_product_quantization)
    {
        codebook_bytes = ProductQuantizer::codebookFloats(params_.dimensions, params_.m, params_.bits) * sizeof(float);
        codebook_type = std::make_shared<DataTypeFixedString>(codebook_bytes);
        codebook_serialization = SerializationNamed::create(
            SerializationProductQuantizationCodebook::create(codebook_type->getDefaultSerialization(), codebook_type),
            product_quantization_subcolumn_name, ISerialization::Substream::ProductQuantizationCodebook);
    }
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

    /// The codes need the trained codebook, which is not available at enumerate time, so no lazy creator for `product`.
    if (!is_product_quantization && data.column && typeid_cast<const ColumnArray *>(data.column.get()))
        codes_data.withLazyColumnCreator([this, col = data.column]() -> ColumnPtr { return encodeCodes(*col, 0, col->size(), nullptr); });

    settings.path.back().data = codes_data;
    callback(settings.path);
    settings.path.pop_back();

    /// The per-part trained codebook, exposed as the `<column>.product_quantization_codebook` subcolumn (`product` only).
    if (is_product_quantization)
    {
        settings.path.push_back(Substream::ProductQuantizationCodebook);
        settings.path.back().name_of_substream = product_quantization_subcolumn_name;
        settings.path.back().data = SubstreamData(codebook_serialization)
                                        .withType(data.type ? codebook_type : nullptr)
                                        .withColumn(nullptr)
                                        .withSerializationInfo(data.serialization_info);
        callback(settings.path);
        settings.path.pop_back();
    }

    /// The full-precision array streams, at the top level: their layout is identical to a plain Array column, so a
    /// reader that only needs the vectors reads them exactly as it would without the codec.
    nested_serialization->enumerateStreams(settings, callback, data);
}

void SerializationQuantizedVector::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!is_product_quantization)
    {
        nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
        return;
    }

    auto state_pq = std::make_shared<SerializedStateProductQuantization>();
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state_pq->nested);
    state = std::move(state_pq);
}

void SerializationQuantizedVector::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    SerializedStateProductQuantization * state_pq = is_product_quantization ? assert_cast<SerializedStateProductQuantization *>(state.get()) : nullptr;

    /// Full-precision data, written exactly like a plain Array column (uses the array state from the prefix).
    SerializeBinaryBulkStatePtr & nested_state = is_product_quantization ? state_pq->nested : state;
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, nested_state);

    /// The codes (and codebook) are written only into on-disk parts, never into transport (Native) streams: in Native
    /// the column is reconstructed from its type name alone, so a reader would not consume them and the stream desyncs.
    if (settings.native_format)
        return;

    size_t count = limit;
    if (count == 0 || offset + count > column.size())
        count = column.size() - offset;

    const float * codebook = nullptr;
    if (is_product_quantization)
    {
        /// Train the codebook once, from the part's first block, then reuse it for every subsequent block.
        if (!state_pq->trained)
        {
            state_pq->codebook = trainCodebook(column, offset, count);
            state_pq->trained = true;
        }
        codebook = state_pq->codebook.data();
    }

    auto codes_column = encodeCodes(column, offset, count, codebook);
    SerializeBinaryBulkStatePtr codes_state;
    codes_serialization->serializeBinaryBulkWithMultipleStreams(*codes_column, 0, codes_column->size(), settings, codes_state);
}

void SerializationQuantizedVector::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!is_product_quantization)
    {
        nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
        return;
    }

    auto * state_pq = assert_cast<SerializedStateProductQuantization *>(state.get());
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state_pq->nested);

    if (settings.native_format)
        return;

    /// Write the trained codebook once for the whole part (a single FixedString value in the `product_quantization_codebook` substream).
    auto codebook_column = ColumnFixedString::create(codebook_bytes);
    auto & chars = codebook_column->getChars();
    chars.resize_fill(codebook_bytes, 0);
    if (state_pq->trained)
        std::memcpy(chars.data(), state_pq->codebook.data(), codebook_bytes);

    SerializeBinaryBulkStatePtr codebook_state;
    codebook_serialization->serializeBinaryBulkWithMultipleStreams(*codebook_column, 0, 1, settings, codebook_state);
}

std::vector<float> SerializationQuantizedVector::trainCodebook(const IColumn & column, size_t offset, size_t count) const
{
    /// A constant vector expression (e.g. `INSERT INTO t SELECT [1., ...] FROM numbers(N)`) could reach the serializer as
    /// a `ColumnConst`; materialize it so the `ColumnArray` cast succeeds (a no-op for an already-full column).
    const auto full = column.convertToFullColumnIfConst();
    const auto * col_arr = typeid_cast<const ColumnArray *>(full.get());
    if (!col_arr)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with a Quantized codec must be an Array");

    const size_t budget_vectors = std::max<size_t>(1, PRODUCT_QUANTIZATION_MAX_TRAINING_BYTES / (params.dimensions * sizeof(float)));
    const size_t n = std::min({count, PRODUCT_QUANTIZATION_MAX_TRAINING_VECTORS, budget_vectors});
    std::vector<float> flat(n * params.dimensions);
    std::vector<float> buf;
    for (size_t i = 0; i < n; ++i)
    {
        readVectorRow(*col_arr, offset + i, buf);
        if (buf.size() != params.dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Vector at row {} has {} elements but the Quantize codec was declared with {} dimensions",
                offset + i, buf.size(), params.dimensions);
        std::memcpy(flat.data() + i * params.dimensions, buf.data(), params.dimensions * sizeof(float));
    }
    return ProductQuantizer::trainCodebook(flat.data(), n, params.dimensions, params.m, params.bits);
}

ColumnPtr SerializationQuantizedVector::encodeCodes(const IColumn & column, size_t offset, size_t count, const float * codebook) const
{
    /// Materialize a possible `ColumnConst` (e.g. from a constant vector expression in an INSERT) before the cast; a
    /// no-op for an already-full column.
    const auto full = column.convertToFullColumnIfConst();
    const auto * col_arr = typeid_cast<const ColumnArray *>(full.get());
    if (!col_arr)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with a Quantized codec must be an Array");

    auto result = ColumnFixedString::create(bytes_per_vector);
    auto & chars = result->getChars();
    chars.resize_fill(count * bytes_per_vector, 0);

    /// Build the encoder once and reuse it for every row, so the per-codebook setup (pq) or the deterministic projection
    /// (the data-independent methods) is not recomputed per row.
    ProductQuantizer::EncoderPtr encoder_pq;
    VectorQuantizer::EncoderPtr encoder_flat;
    if (is_product_quantization)
        encoder_pq = ProductQuantizer::createEncoder(codebook, params.dimensions, params.m, params.bits);
    else
        encoder_flat = VectorQuantizer::createEncoder(params.method, params.dimensions, params.bits);

    std::vector<float> buf;
    for (size_t i = 0; i < count; ++i)
    {
        readVectorRow(*col_arr, offset + i, buf);
        if (buf.size() != params.dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Vector at row {} has {} elements but the Quantize codec was declared with {} dimensions",
                offset + i, buf.size(), params.dimensions);

        char * dst = reinterpret_cast<char *>(&chars[i * bytes_per_vector]);
        if (is_product_quantization)
            ProductQuantizer::encode(*encoder_pq, buf.data(), dst);
        else
            VectorQuantizer::encode(*encoder_flat, buf.data(), dst);
    }

    return result;
}

}
