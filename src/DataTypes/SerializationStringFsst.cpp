#include <memory>
#include "Common/Exception.h"
#include "Common/assert_cast.h"
#include "Common/typeid_cast.h"
#include "Columns/ColumnFSST.h"
#include "Columns/ColumnString.h"
#include "SerializationStringFsst.h"

#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <fsst.h>

namespace DB
{

void SerializationStringFsst::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}


void SerializationStringFsst::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}


void SerializationStringFsst::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(column, row_num, ostr, settings);
}

void SerializationStringFsst::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const {
    nested->deserializeBinary(column, istr, settings);
}

void SerializationStringFsst::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    // fsst stream
    settings.path.push_back(Substream::Fsst);
    callback(settings.path);
    settings.path.pop_back();

    // compressed strings offsets stream
    settings.path.push_back(Substream::FsstOffsets);
    callback(settings.path);
    settings.path.pop_back();

    /* StringSerialization streams */
    nested->enumerateStreams(settings, callback, data);
}

void SerializationStringFsst::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_fsst = assert_cast<const ColumnFSST &>(column);
    const auto & column_string = assert_cast<const ColumnString &>(*column_fsst.getStringColumn());

    settings.path.push_back(Substream::Fsst);
    auto * fsst_stream = settings.getter(settings.path);
    if (!fsst_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "no FSST stream");
    settings.path.pop_back();

    settings.path.push_back(Substream::Fsst);
    auto * offsets_stream = settings.getter(settings.path);
    if (!offsets_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "No compressed strings offsets stream");
    settings.path.pop_back();

    /* write fsst */
    fsst_stream->write(reinterpret_cast<const char *>(&column_fsst.getFsst()), sizeof(fsst_decoder_t));

    /* write compressed strings offsets */
    offsets_stream->write(reinterpret_cast<const char *>(column_fsst.getOffsets().data()), column_fsst.getOffsets().size());

    /* write compressed string */
    nested->serializeBinaryBulkWithMultipleStreams(column_string, offset, limit, settings, state);
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
