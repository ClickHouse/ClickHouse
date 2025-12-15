#include <cstddef>
#include <memory>
#include <string>
#include "Common/Exception.h"
#include "Common/PODArray.h"
#include "Common/assert_cast.h"
#include "Columns/ColumnFSST.h"
#include "Columns/ColumnString.h"
#include "Columns/IColumn_fwd.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "SerializationStringFsst.h"

#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <fsst.h>

namespace DB
{

void SerializationStringFsst::serializeState(SerializeBinaryBulkSettings & settings, std::shared_ptr<SerializeFsstState> state) const
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

    settings.path.push_back(Substream::FsstOffsets);
    auto * compressed_data_stream = settings.getter(settings.path);
    if (!compressed_data_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed data stream");
    settings.path.pop_back();

    /* create and save fsst */
    size_t strings = state->offsets.size();
    PODArray<size_t> origin_lengths;
    std::unique_ptr<const unsigned char *[]> string_pointers(new const unsigned char *[strings]);
    bool zero_terminated = false; // not sure

    for (size_t ind = 0; ind < strings; ind++)
    {
        size_t next_offset = ind + 1 == strings ? state->chars.size() : state->offsets[ind + 1];
        origin_lengths.push_back(next_offset - state->offsets[ind]);
        string_pointers[ind] = reinterpret_cast<const unsigned char *>(state->chars.data() + state->offsets[ind]);
    }

    fsst_decoder_t fsst_decoder;
    auto * fsst_encoder = fsst_create(strings, reinterpret_cast<size_t *>(origin_lengths.data()), string_pointers.get(), zero_terminated);

    fsst_export(fsst_encoder, reinterpret_cast<unsigned char *>(&fsst_decoder));
    fsst_stream->write(reinterpret_cast<const char *>(&fsst_decoder), sizeof(fsst_decoder));

    /* compress state itself and save */
    PODArray<char8_t> compressed_data(2 * state->chars.size());
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

    ColumnString::Offsets offsets_after_compression(strings);
    for (size_t ind = 0; ind < strings; ind++)
    {
        offsets_after_compression[ind] = compressed_data_pointers[ind] - compressed_data_pointers[0];
    }
    size_t total_size_after_compression
        = compressed_data_pointers[strings - 1] - compressed_data_pointers[0] + compressed_data_lenegths[strings - 1];
    offsets_stream->write(reinterpret_cast<const char *>(offsets_after_compression.data()), offsets_after_compression.size());
    compressed_data_stream->write(reinterpret_cast<const char *>(compressed_data.data()), total_size_after_compression);

    /* clear state */
    state->chars.clear();
    state->offsets.clear();
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
    auto serialize_state = std::static_pointer_cast<SerializeFsstState>(state);

    for (size_t ind = 0; ind < column_string.size(); ind++)
    {
        size_t end_offset = ind + 1 == column_string.size() ? column_string.getChars().size() : column_string.getOffsets()[ind + 1];
        serialize_state->offsets.push_back(serialize_state->chars.size());
        serialize_state->chars.insert(
            column_string.getChars().data() + column_string.getOffsets()[ind], column_string.getChars().data() + end_offset);
        if (serialize_state->chars.size() >= kCompressSize)
        {
            serializeState(settings, serialize_state);
        }
    }

    if (offset + limit >= column.size() && !serialize_state->chars.empty())
    {
        serializeState(settings, serialize_state);
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
    auto & column_string = assert_cast<ColumnString &>(*column_fsst.getStringColumn());

    /* read string column */
    nested->deserializeBinaryBulkWithMultipleStreams(column_fsst.getStringColumn(), rows_offset, limit, settings, state, cache);

    /* read FSST*/
    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty FSST stream");
    assert(fsst_stream->readBig(reinterpret_cast<char *>(&column_fsst.getFsst()), sizeof(fsst_decoder_t)) == sizeof(fsst_decoder_t));
    settings.path.pop_back();

    /* read string offsets */
    settings.path.push_back(Substream::FsstOffsets);
    auto * offsets_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty FSST stream");
    column_fsst.getOffsets().resize_fill(column_string.size());
    size_t offsets_array_size = column_fsst.getOffsets().size();
    assert(
        offsets_stream->readBig(reinterpret_cast<char *>(column_fsst.getOffsets().data()), offsets_array_size)
        == sizeof(offsets_array_size));
    settings.path.pop_back();
}

};
