#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string_view>

#include <Columns/ColumnFSST.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/Serializations/SerializationStringFSST.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <fsst.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}


template <>
struct SerializeFSSTState<false> : public ISerialization::SerializeBinaryBulkState
{
    SerializeFSSTState() = default;

    PaddedPODArray<UInt8> chars;
    PaddedPODArray<UInt64> offsets;
};

template <>
struct SerializeFSSTState<true> : public ISerialization::SerializeBinaryBulkState
{
    SerializeFSSTState() = default;

    std::vector<std::string_view> compressed_data;
    std::vector<size_t> origin_lengths;
    fsst_decoder_t decoder;
};

class DeserializeFSSTState : public ISerialization::DeserializeBinaryBulkState
{
public:
    DeserializeFSSTState() = default;
    DeserializeFSSTState(
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
    ~DeserializeFSSTState() override = default;

private:
    PODArray<UInt8> chars;
    PODArray<UInt64> offsets;
    PODArray<UInt64> origin_lengths;
    size_t current_ind;
    fsst_decoder_t decoder;
};

std::optional<CompressedField> DeserializeFSSTState::pop()
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

template <bool compressed>
void saveSerializeState(
    SerializeFSSTState<compressed> & state, WriteBuffer * fsst_stream, WriteBuffer * offsets_stream, WriteBuffer * data_stream)
{
    if constexpr (compressed)
    {
        size_t strings = state.compressed_data.size();
        size_t total_compressed_size = 0;
        std::vector<size_t> offsets_after_compression;

        for (std::string_view string : state.compressed_data)
        {
            offsets_after_compression.emplace_back(total_compressed_size);
            total_compressed_size += string.size();
        }

        fsst_stream->write(reinterpret_cast<const char *>(&state.decoder), sizeof(state.decoder));

        offsets_stream->write(reinterpret_cast<const char *>(&strings), sizeof(strings));
        offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression.data()), strings * sizeof(size_t));
        offsets_stream->write(reinterpret_cast<const char *>(state.origin_lengths.data()), strings * sizeof(size_t));

        data_stream->write(reinterpret_cast<const char *>(&total_compressed_size), sizeof(total_compressed_size));
        for (std::string_view string : state.compressed_data)
            data_stream->write(reinterpret_cast<const char *>(string.data()), string.size());

        state.compressed_data.clear();
        state.origin_lengths.clear();
    }
    else
    {
        size_t strings = state.offsets.size();
        PODArray<UInt64> origin_lengths;
        std::unique_ptr<const unsigned char *[]> string_pointers(new const unsigned char *[strings]);
        bool zero_terminated = false; // not sure

        for (size_t ind = 0; ind < strings; ind++)
        {
            size_t next_offset = ind + 1 == strings ? state.chars.size() : state.offsets[ind + 1];
            origin_lengths.push_back(next_offset - state.offsets[ind]);
            string_pointers[ind] = reinterpret_cast<const unsigned char *>(state.chars.data() + state.offsets[ind]);
        }

        auto * fsst_encoder
            = fsst_create(strings, reinterpret_cast<size_t *>(origin_lengths.data()), string_pointers.get(), zero_terminated);

        auto decoder = fsst_decoder(fsst_encoder);
        fsst_stream->write(reinterpret_cast<const char *>(&decoder), sizeof(decoder));

        PODArray<char8_t> compressed_data(2 * state.chars.size());
        PODArray<size_t> compressed_data_lenegths(strings);
        std::unique_ptr<unsigned char *[]> compressed_data_pointers(new unsigned char *[strings]);

        fsst_compress(
            fsst_encoder,
            strings,
            reinterpret_cast<unsigned long *>(origin_lengths.data()), // NOLINT
            string_pointers.get(),
            compressed_data.size(),
            reinterpret_cast<unsigned char *>(compressed_data.data()),
            compressed_data_lenegths.data(),
            compressed_data_pointers.get());

        PODArray<size_t> offsets_after_compression(strings);
        for (size_t ind = 0; ind < strings; ind++)
            offsets_after_compression[ind] = compressed_data_pointers[ind] - compressed_data_pointers[0];

        size_t total_size_after_compression
            = compressed_data_pointers[strings - 1] - compressed_data_pointers[0] + compressed_data_lenegths[strings - 1];

        offsets_stream->write(reinterpret_cast<const char *>(&strings), sizeof(strings));
        offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression.data()), strings * sizeof(size_t));
        offsets_stream->write(reinterpret_cast<const char *>(origin_lengths.data()), strings * sizeof(size_t));

        data_stream->write(reinterpret_cast<const char *>(&total_size_after_compression), sizeof(total_size_after_compression));
        data_stream->write(reinterpret_cast<const char *>(compressed_data.data()), total_size_after_compression);

        /* clear state */
        state.chars.clear();
        state.offsets.clear();
    }
}

template <bool compressed>
void SerializationStringFSST::serializeState(SerializeBinaryBulkSettings & settings, SerializeFSSTState<compressed> & state) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");

    settings.path.back() = Substream::FsstOffsets;
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");

    settings.path.back() = Substream::FsstCompressed;
    auto * data_stream = settings.getter(settings.path);
    if (!data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    saveSerializeState(state, fsst_stream, offsets_stream, data_stream);
}

size_t SerializationStringFSST::deserializeState(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");

    settings.path.back() = Substream::FsstOffsets;
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");

    settings.path.back() = Substream::FsstCompressed;
    auto * compressed_data_stream = settings.getter(settings.path);
    if (!compressed_data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    if (offsets_stream->eof())
        return 0;

    /* read fsst */
    fsst_decoder_t decoder;
    size_t decoder_read_bytes = fsst_stream->readBig(reinterpret_cast<char *>(&decoder), sizeof(decoder));

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

    state = std::make_shared<DeserializeFSSTState>(compressed_bytes, offsets, origin_lengths, decoder);
    return decoder_read_bytes + metadata_bytes_read + compressed_bytes_read;
}

ISerialization::KindStack SerializationStringFSST::getKindStack() const
{
    auto kind_stack = nested->getKindStack();
    kind_stack.push_back(Kind::FSST);
    return kind_stack;
}

SerializationPtr SerializationStringFSST::SubcolumnCreator::create(const SerializationPtr & string_nested, const DataTypePtr &) const
{
    return std::make_shared<SerializationStringFSST>(string_nested);
}

ColumnPtr SerializationStringFSST::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnFSST::create(prev);
}

void SerializationStringFSST::enumerateStreams(
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
    settings.path.push_back(Substream::FsstCompressed);
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(ColumnString::create());
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const ColumnString * column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (!state)
    {
        state = std::make_shared<SerializeFSSTState<false>>();
    }
    auto serialize_state = std::static_pointer_cast<SerializeFSSTState<false>>(state);

    for (size_t ind = offset; ind < std::min(column->size(), offset + limit); ind++)
    {
        size_t start_offset = ind == 0 ? 0 : column->getOffsets()[ind - 1];
        serialize_state->offsets.push_back(serialize_state->chars.size());
        serialize_state->chars.insert(column->getChars().data() + start_offset, column->getChars().data() + column->getOffsets()[ind]);
        if (serialize_state->chars.size() >= kCompressSize)
        {
            serializeState(settings, *serialize_state);
        }
    }

    if (offset + limit >= column->size() && !serialize_state->chars.empty())
    {
        serializeState(settings, *serialize_state);
    }
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const ColumnFSST * column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    state = state ? state : std::make_shared<SerializeFSSTState<true>>();

    auto serialize_state = std::static_pointer_cast<SerializeFSSTState<true>>(state);
    size_t state_size = 0;
    auto string_column = column->getStringColumn();
    const auto & origin_lengths = column->getLengths();
    const auto & decoders = column->getDecoders();

    auto decoder_it = std::lower_bound(
        decoders.begin(), decoders.end(), offset, [](const auto & decoder, size_t val) { return decoder.batch_start_index < val; });
    for (size_t ind = offset; ind < std::min(string_column->size(), offset + limit); ind++)
    {
        while (decoder_it != decoders.end() && decoder_it->batch_start_index < ind)
            ++decoder_it;

        serialize_state->compressed_data.emplace_back(string_column->getDataAt(ind));
        serialize_state->origin_lengths.emplace_back(origin_lengths[ind]);
        state_size += origin_lengths.back();

        if (decoder_it != decoders.end() && decoder_it->batch_start_index == ind)
            serialize_state->decoder = *decoder_it->decoder;

        if (state_size >= kCompressSize)
        {
            state_size = 0;
            serializeState(settings, *serialize_state);
        }
    }

    if (offset + limit >= column->size() && !serialize_state->compressed_data.empty())
        serializeState(settings, *serialize_state);
}

void SerializationStringFSST::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    limit = limit == 0 ? UINT64_MAX : limit;
    if (const auto * column_string = typeid_cast<const ColumnString *>(&column))
    {
        serializeBinaryBulkWithMultipleStreams(column_string, offset, limit, settings, state);
    }
    else if (const auto * column_fsst = typeid_cast<const ColumnFSST *>(&column))
    {
        serializeBinaryBulkWithMultipleStreams(column_fsst, offset, limit, settings, state);
    }
}

void SerializationStringFSST::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * /* cache */) const
{
    auto & column_fsst = assert_cast<ColumnFSST &>(*column->assumeMutable());

    state = state ? state : std::make_shared<DeserializeFSSTState>();
    auto deserialize_state = std::static_pointer_cast<DeserializeFSSTState>(state);
    auto decoder_ptr = std::make_shared<fsst_decoder_t>();

    for (size_t ind = 0; ind < limit + rows_offset; ind++)
    {
        auto current_field = deserialize_state->pop();
        if (ind < rows_offset)
            continue;

        if (current_field.has_value())
        {
            if (!decoder_ptr)
            {
                fsst_decoder_t current_decoder = deserialize_state->getDecoder();
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
            break;

        --ind;
        deserialize_state = std::static_pointer_cast<DeserializeFSSTState>(state);
    }
}

};
