#include <DataTypes/Serializations/SerializationQuantizedVector.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProductQuantization.h>
#include <Common/VectorQuantization.h>
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

/// Cap the number of vectors used to train the per-part codebook (k-means cost is bounded; the part's first block is
/// already a representative sample once it has well more than `k` rows).
constexpr size_t PQ_MAX_TRAINING_VECTORS = 100000;

/// Write state for the `pq` method: the codebook is trained from the first block and reused for the whole part, then
/// written once at the suffix (mirrors LowCardinality's per-part dictionary lifecycle).
struct SerializeStatePQ : public ISerialization::SerializeBinaryBulkState
{
    ISerialization::SerializeBinaryBulkStatePtr nested; /// the full-precision array's own write state
    std::vector<float> codebook;
    bool trained = false;
};

/// Holds the part's single codebook value once read, so it is broadcast to every granule without re-reading the stream.
struct DeserializeStatePQCodebook : public ISerialization::DeserializeBinaryBulkState
{
    ColumnPtr codebook; /// a one-row column, or null until the first granule reads it
};

/// Read serialization for the per-part codebook subcolumn. The codebook is stored as a SINGLE value per part (written
/// once at the suffix; every granule's mark points at it), but a scan asks for one value per row. Reading it as a plain
/// FixedString would try to read `limit` values from a one-value stream (and materialize `limit` copies of a large
/// blob). Instead we read the one value ONCE into the deserialize state and return a `ColumnConst` broadcast to `limit`
/// rows for every granule - O(1) memory, and the distance function sees the codebook as a per-block constant. Reading
/// the stream on every granule (the previous approach) exhausts the one-value stream and fails on multi-granule parts.
class SerializationPQCodebook final : public SerializationWrapper
{
public:
    SerializationPQCodebook(const SerializationPtr & nested_, const DataTypePtr & value_type_)
        : SerializationWrapper(nested_), value_type(value_type_) {}

    /// Created via the serialization pool so it carries a stable hash (required when attached to a column), mirroring
    /// SerializationNamed::create.
    static SerializationPtr create(const SerializationPtr & nested_, const DataTypePtr & value_type_)
    {
        if (!nested_->supportsPooling())
            return std::shared_ptr<ISerialization>(new SerializationPQCodebook(nested_, value_type_));
        SipHash hash;
        hash.update("PQCodebook");
        hash.update(nested_->getHash());
        return ISerialization::pooled(hash.get128(), [&] { return new SerializationPQCodebook(nested_, value_type_); });
    }

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & /*settings*/,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * /*cache*/) const override
    {
        state = std::make_shared<DeserializeStatePQCodebook>();
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
        auto * pq_state = typeid_cast<DeserializeStatePQCodebook *>(state.get());
        if (!pq_state)
        {
            auto new_state = std::make_shared<DeserializeStatePQCodebook>();
            pq_state = new_state.get();
            state = std::move(new_state);
        }
        const size_t prev_size = column ? column->size() : 0;

        /// Read the part's single codebook value exactly once (the stream holds one value for the whole part); every
        /// granule reuses it. The first granule of a read range is positioned at the codebook's start by its mark.
        if (!pq_state->codebook)
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
            pq_state->codebook = std::move(value);
        }

        column = ColumnConst::create(pq_state->codebook, prev_size + limit);
    }

private:
    DataTypePtr value_type;
};

}

SerializationQuantizedVector::SerializationQuantizedVector(const SerializationPtr & nested_, const QuantizeCodecParams & params_)
    : SerializationWrapper(nested_)
    , params(params_)
    , is_pq(params_.method == "pq")
    , bytes_per_vector(is_pq
          ? ProductQuantization::bytesPerVector(params_.dimensions, params_.m, params_.bits)
          : VectorQuantization::bytesPerVector(params_.method, params_.dimensions, params_.bits))
    , codes_type(std::make_shared<DataTypeFixedString>(bytes_per_vector))
    , codes_serialization(SerializationNamed::create(
          codes_type->getDefaultSerialization(), subcolumn_name, ISerialization::Substream::QuantizedCodes))
{
    if (is_pq)
    {
        codebook_bytes = ProductQuantization::codebookFloats(params_.dimensions, params_.m, params_.bits) * sizeof(float);
        codebook_type = std::make_shared<DataTypeFixedString>(codebook_bytes);
        codebook_serialization = SerializationNamed::create(
            SerializationPQCodebook::create(codebook_type->getDefaultSerialization(), codebook_type),
            pq_codebook_subcolumn_name, ISerialization::Substream::PQCodebook);
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

    /// The codes need the trained codebook, which is not available at enumerate time, so no lazy creator for `pq`.
    if (!is_pq && data.column && typeid_cast<const ColumnArray *>(data.column.get()))
        codes_data.withLazyColumnCreator([this, col = data.column]() -> ColumnPtr { return encodeCodes(*col, 0, col->size(), nullptr); });

    settings.path.back().data = codes_data;
    callback(settings.path);
    settings.path.pop_back();

    /// The per-part trained codebook, exposed as the `<column>.pq_codebook` subcolumn (`pq` only).
    if (is_pq)
    {
        settings.path.push_back(Substream::PQCodebook);
        settings.path.back().name_of_substream = pq_codebook_subcolumn_name;
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
    if (!is_pq)
    {
        nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
        return;
    }

    auto pq_state = std::make_shared<SerializeStatePQ>();
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, pq_state->nested);
    state = std::move(pq_state);
}

void SerializationQuantizedVector::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    SerializeStatePQ * pq_state = is_pq ? assert_cast<SerializeStatePQ *>(state.get()) : nullptr;

    /// Full-precision data, written exactly like a plain Array column (uses the array state from the prefix).
    SerializeBinaryBulkStatePtr & nested_state = is_pq ? pq_state->nested : state;
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, nested_state);

    /// The codes (and codebook) are written only into on-disk parts, never into transport (Native) streams: in Native
    /// the column is reconstructed from its type name alone, so a reader would not consume them and the stream desyncs.
    if (settings.native_format)
        return;

    size_t count = limit;
    if (count == 0 || offset + count > column.size())
        count = column.size() - offset;

    const float * codebook = nullptr;
    if (is_pq)
    {
        /// Train the codebook once, from the part's first block, then reuse it for every subsequent block.
        if (!pq_state->trained)
        {
            pq_state->codebook = trainCodebook(column, offset, count);
            pq_state->trained = true;
        }
        codebook = pq_state->codebook.data();
    }

    auto codes_column = encodeCodes(column, offset, count, codebook);
    SerializeBinaryBulkStatePtr codes_state;
    codes_serialization->serializeBinaryBulkWithMultipleStreams(*codes_column, 0, codes_column->size(), settings, codes_state);
}

void SerializationQuantizedVector::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (!is_pq)
    {
        nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
        return;
    }

    auto * pq_state = assert_cast<SerializeStatePQ *>(state.get());
    nested_serialization->serializeBinaryBulkStateSuffix(settings, pq_state->nested);

    if (settings.native_format)
        return;

    /// Write the trained codebook once for the whole part (a single FixedString value in the `pq_codebook` substream).
    auto codebook_column = ColumnFixedString::create(codebook_bytes);
    auto & chars = codebook_column->getChars();
    chars.resize_fill(codebook_bytes, 0);
    if (pq_state->trained)
        std::memcpy(chars.data(), pq_state->codebook.data(), codebook_bytes);

    SerializeBinaryBulkStatePtr codebook_state;
    codebook_serialization->serializeBinaryBulkWithMultipleStreams(*codebook_column, 0, 1, settings, codebook_state);
}

std::vector<float> SerializationQuantizedVector::trainCodebook(const IColumn & column, size_t offset, size_t count) const
{
    const auto * col_arr = typeid_cast<const ColumnArray *>(&column);
    if (!col_arr)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with a Quantize codec must be an Array");

    const size_t n = std::min(count, PQ_MAX_TRAINING_VECTORS);
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
    return ProductQuantization::trainCodebook(flat.data(), n, params.dimensions, params.m, params.bits);
}

ColumnPtr SerializationQuantizedVector::encodeCodes(const IColumn & column, size_t offset, size_t count, const float * codebook) const
{
    const auto * col_arr = typeid_cast<const ColumnArray *>(&column);
    if (!col_arr)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column with a Quantize codec must be an Array");

    auto res = ColumnFixedString::create(bytes_per_vector);
    auto & chars = res->getChars();
    chars.resize_fill(count * bytes_per_vector, 0);

    /// For pq, build the encoder once for this codebook so the per-codebook setup is shared across all rows.
    std::shared_ptr<ProductQuantization::Encoder> pq_encoder;
    if (is_pq)
        pq_encoder = ProductQuantization::prepareEncoder(codebook, params.dimensions, params.m, params.bits);

    std::vector<float> buf;
    for (size_t i = 0; i < count; ++i)
    {
        readVectorRow(*col_arr, offset + i, buf);
        if (buf.size() != params.dimensions)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Vector at row {} has {} elements but the Quantize codec was declared with {} dimensions",
                offset + i, buf.size(), params.dimensions);

        char * dst = reinterpret_cast<char *>(&chars[i * bytes_per_vector]);
        if (is_pq)
            ProductQuantization::encode(*pq_encoder, buf.data(), dst);
        else
            VectorQuantization::encode(params.method, buf.data(), params.dimensions, params.bits, dst);
    }

    return res;
}

}
