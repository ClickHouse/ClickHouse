#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include "Common/Exception.h"
#include "Common/PODArray.h"
#include "Common/PODArray_fwd.h"
#include "Common/assert_cast.h"
#include "Columns/ColumnFSST.h"
#include "Columns/ColumnString.h"
#include "Columns/IColumn_fwd.h"
#include "Core/Field.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "SerializationStringFsst.h"
#include "base/types.h"

#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <fsst.h>

namespace DB
{

struct fsst_decoder_t
{
    unsigned long long version;
    unsigned char zero_terminated;
    unsigned char len[255];
    unsigned long long symbol[255];
};

struct SerializeFsstState : public ISerialization::SerializeBinaryBulkState
{
    SerializeFsstState() = default;

    PaddedPODArray<UInt8> chars;
    PaddedPODArray<UInt64> offsets;
};
class DeserializeFsstState : public ISerialization::DeserializeBinaryBulkState
{
public:
    DeserializeFsstState() = default;
    DeserializeFsstState(
        PODArray<char8_t> & _chars, PODArray<UInt64> & _offsets, PODArray<UInt64> & _origin_lengths, const ::fsst_decoder_t & _decoder)
        : chars(std::move(_chars))
        , offsets(std::move(_offsets))
        , origin_lengths(std::move(_origin_lengths))
        , current_ind(0)
        , decoder(_decoder)
    {
    }

    std::optional<CompressedField> pop();
    const ::fsst_decoder_t & getDecoder() { return decoder; }
    ~DeserializeFsstState() override = default;

private:
    PODArray<UInt8> chars;
    PODArray<UInt64> offsets;
    PODArray<UInt64> origin_lengths;
    size_t current_ind;
    ::fsst_decoder_t decoder;
};

std::optional<CompressedField> DeserializeFsstState::pop()
{
    if (current_ind >= offsets.size())
    {
        return std::nullopt;
    }

    size_t current_offset = offsets[current_ind];
    size_t compressed_string_size = (current_ind + 1 == offsets.size() ? chars.size() : offsets[current_ind + 1]) - current_offset;
    size_t uncompressed_string_size = origin_lengths[current_ind++];
    auto result
        = CompressedField{.value = Field(&chars[current_offset], compressed_string_size), .uncompressed_size = uncompressed_string_size};
    return result;
}

void SerializationStringFsst::serializeState(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");
    settings.path.pop_back();

    settings.path.push_back(Substream::FsstOffsets);
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");
    settings.path.pop_back();

    settings.path.push_back(Substream::Regular);
    auto * compressed_data_stream = settings.getter(settings.path);
    if (!compressed_data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    auto fsst_state = std::static_pointer_cast<SerializeFsstState>(state);
    /* create and save fsst */
    size_t strings = fsst_state->offsets.size();
    PODArray<UInt64> origin_lengths;
    std::unique_ptr<const unsigned char *[]> string_pointers(new const unsigned char *[strings]);
    bool zero_terminated = false; // not sure

    for (size_t ind = 0; ind < strings; ind++)
    {
        size_t next_offset = ind + 1 == strings ? fsst_state->chars.size() : fsst_state->offsets[ind + 1];
        origin_lengths.push_back(next_offset - fsst_state->offsets[ind]);
        string_pointers[ind] = reinterpret_cast<const unsigned char *>(fsst_state->chars.data() + fsst_state->offsets[ind]);
    }

    auto * fsst_encoder = fsst_create(strings, reinterpret_cast<size_t *>(origin_lengths.data()), string_pointers.get(), zero_terminated);

    ::fsst_decoder_t decoder = fsst_decoder(fsst_encoder);
    unsigned char decoder_raw[sizeof(decoder)];
    fsst_export(fsst_encoder, decoder_raw);
    fsst_stream->write(reinterpret_cast<const char *>(decoder_raw), sizeof(decoder_raw));

    /* compress state itself and save */
    PODArray<char8_t> compressed_data(2 * fsst_state->chars.size());
    PODArray<size_t> compressed_data_lenegths(strings);
    std::unique_ptr<unsigned char *[]> compressed_data_pointers(new unsigned char *[strings]);

    fsst_compress(
        fsst_encoder,
        strings,
        reinterpret_cast<unsigned long *>(origin_lengths.data()),
        string_pointers.get(),
        compressed_data.size(),
        reinterpret_cast<unsigned char *>(compressed_data.data()),
        compressed_data_lenegths.data(),
        compressed_data_pointers.get());

    PODArray<size_t> offsets_after_compression(strings);
    for (size_t ind = 0; ind < strings; ind++)
    {
        offsets_after_compression[ind] = compressed_data_pointers[ind] - compressed_data_pointers[0];
    }
    size_t total_size_after_compression
        = compressed_data_pointers[strings - 1] - compressed_data_pointers[0] + compressed_data_lenegths[strings - 1];

    offsets_stream->write(reinterpret_cast<const char *>(&strings), sizeof(strings));
    offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression.data()), strings * sizeof(size_t));
    offsets_stream->write(reinterpret_cast<const char *>(origin_lengths.data()), strings * sizeof(size_t));

    compressed_data_stream->write(reinterpret_cast<const char *>(&total_size_after_compression), sizeof(total_size_after_compression));
    compressed_data_stream->write(reinterpret_cast<const char *>(compressed_data.data()), total_size_after_compression);

    /* clear state */
    fsst_state->chars.clear();
    fsst_state->offsets.clear();
}

size_t SerializationStringFsst::deserializeState(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");
    settings.path.pop_back();

    settings.path.push_back(Substream::FsstOffsets);
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");
    settings.path.pop_back();

    settings.path.push_back(Substream::Regular);
    auto * compressed_data_stream = settings.getter(settings.path);
    if (!compressed_data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    if (offsets_stream->eof())
    {
        return 0;
    }

    /* read fsst */
    ::fsst_decoder_t decoder;
    unsigned char decoder_raw[sizeof(::fsst_decoder_t)];
    size_t decoder_read_bytes = fsst_stream->readBig(reinterpret_cast<char *>(decoder_raw), sizeof(decoder_raw));
    fsst_import(&decoder, decoder_raw);

    /* read offsets and lengths */
    size_t strings;
    size_t metadata_bytes_read = offsets_stream->readBig(reinterpret_cast<char *>(&strings), sizeof(strings));

    PODArray<UInt64> offsets(strings);
    PODArray<UInt64> origin_lengths(strings);

    metadata_bytes_read += offsets_stream->readBig(reinterpret_cast<char *>(offsets.data()), sizeof(size_t) * strings);
    metadata_bytes_read += offsets_stream->readBig(reinterpret_cast<char *>(origin_lengths.data()), sizeof(size_t) * strings);

    /* read compressed strings */
    size_t total_compressed_bytes;
    size_t compressed_bytes_read
        = compressed_data_stream->readBig(reinterpret_cast<char *>(&total_compressed_bytes), sizeof(total_compressed_bytes));

    PODArray<char8_t> compressed_bytes(total_compressed_bytes);
    compressed_bytes_read += compressed_data_stream->readBig(reinterpret_cast<char *>(compressed_bytes.data()), total_compressed_bytes);

    state = std::make_shared<DeserializeFsstState>(compressed_bytes, offsets, origin_lengths, decoder);
    return decoder_read_bytes + metadata_bytes_read + compressed_bytes_read;
}

void SerializationStringFsst::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & /*data*/) const
{
    // fsst stream
    settings.path.push_back(Substream::Fsst);
    callback(settings.path);
    settings.path.pop_back();

    // compressed strings offsets stream
    settings.path.push_back(Substream::FsstOffsets);
    callback(settings.path);
    settings.path.pop_back();

    // compressed strings data stream
    settings.path.push_back(Substream::Regular);
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationStringFsst::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_string = assert_cast<const ColumnString &>(column);
    if (!state)
    {
        state = std::make_shared<SerializeFsstState>();
    }

    auto serialize_state = std::static_pointer_cast<SerializeFsstState>(state);

    for (size_t ind = 0; ind < column_string.size(); ind++)
    {
        size_t start_offset = ind == 0 ? 0 : column_string.getOffsets()[ind - 1];
        serialize_state->offsets.push_back(serialize_state->chars.size());
        serialize_state->chars.insert(
            column_string.getChars().data() + start_offset, column_string.getChars().data() + column_string.getOffsets()[ind]);
        if (serialize_state->chars.size() >= kCompressSize)
        {
            serializeState(settings, state);
        }
    }

    if (offset + limit >= column.size() && !serialize_state->chars.empty())
    {
        serializeState(settings, state);
    }
}

void SerializationStringFsst::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & column_fsst = assert_cast<ColumnFSST &>(*column->assumeMutable());

    if (!state)
    {
        state = std::make_shared<DeserializeFsstState>();
    }
    auto deserialize_state = std::static_pointer_cast<DeserializeFsstState>(state);
    auto decoder_ptr = std::make_shared<fsst_decoder_t>();

    for (size_t ind = 0; ind < limit + rows_offset; ind++)
    {
        auto current_field = deserialize_state->pop();
        if (ind < rows_offset)
        {
            continue;
        }

        if (current_field.has_value())
        {
            if (!decoder_ptr)
            {
                ::fsst_decoder_t current_decoder = deserialize_state->getDecoder();
                decoder_ptr = std::make_shared<fsst_decoder_t>();
                std::memcpy(decoder_ptr.get(), &current_decoder, sizeof(current_decoder));
                column_fsst.appendNewBatch(current_field.value(), decoder_ptr);
            }
            else
            {
                column_fsst.append(current_field.value());
            }
            continue;
        }

        state.reset();
        decoder_ptr.reset();
        if (deserializeState(settings, state) == 0)
        {
            break;
        }
        --ind;
        deserialize_state = std::static_pointer_cast<DeserializeFsstState>(state);
    }
}

};
