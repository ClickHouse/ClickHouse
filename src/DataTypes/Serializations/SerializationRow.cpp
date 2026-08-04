#include <DataTypes/Serializations/SerializationRow.h>
#include <base/scope_guard.h>
#include <Columns/ColumnTuple.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

SerializationRow::SerializationRow(Serializations field_serializations_, Strings field_names_)
    : field_serializations(std::move(field_serializations_)), field_names(std::move(field_names_))
{
    if (field_serializations.size() != field_names.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Row serialization: field count mismatch");
}

UInt128 SerializationRow::getHash(const Serializations & field_serializations_, const Strings & field_names_)
{
    SipHash hash;
    hash.update("Row");
    for (size_t i = 0; i < field_serializations_.size(); ++i)
    {
        hash.update(field_names_[i].size());
        hash.update(field_names_[i]);
        hash.update(field_serializations_[i]->getHash());
    }
    return hash.get128();
}

SerializationPtr SerializationRow::create(Serializations field_serializations_, Strings field_names_)
{
    for (const auto & field_serialization : field_serializations_)
    {
        if (!field_serialization->supportsPooling())
            return std::shared_ptr<ISerialization>(new SerializationRow(std::move(field_serializations_), std::move(field_names_)));
    }

    auto hash = getHash(field_serializations_, field_names_);
    return ISerialization::pooled(
        hash,
        [s = std::move(field_serializations_), n = std::move(field_names_)]() mutable
        { return new SerializationRow(std::move(s), std::move(n)); });
}

size_t SerializationRow::allocatedBytes() const
{
    size_t bytes = sizeof(*this);
    bytes += field_serializations.capacity() * sizeof(SerializationPtr);
    bytes += field_names.capacity() * sizeof(String);
    for (const auto & field_name : field_names)
        bytes += field_name.capacity();
    return bytes;
}

bool SerializationRow::supportsPooling() const
{
    for (const auto & field_serialization : field_serializations)
        if (!field_serialization->supportsPooling())
            return false;
    return true;
}

void SerializationRow::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    /// Single physical stream; the Regular marker yields the column's file name.
    settings.path.push_back(Substream::Regular);
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

namespace
{
    void writeRowFields(
        const ColumnTuple & tuple, size_t row_num,
        const Serializations & fields, const FormatSettings & settings, WriteBuffer & out)
    {
        for (size_t i = 0; i < fields.size(); ++i)
            fields[i]->serializeBinary(tuple.getColumn(i), row_num, out, settings);
    }

    void readRowFields(
        ColumnTuple & tuple, const Serializations & fields, const FormatSettings & settings, ReadBuffer & in)
    {
        for (size_t i = 0; i < fields.size(); ++i)
            fields[i]->deserializeBinary(tuple.getColumn(i), in, settings);
    }
}

void SerializationRow::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & tuple = field.safeGet<Tuple>();
    if (tuple.size() != field_serializations.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Row field count mismatch in serializeBinary");

    WriteBufferFromOwnString buf;
    for (size_t i = 0; i < field_serializations.size(); ++i)
        field_serializations[i]->serializeBinary(tuple[i], buf, settings);

    auto sv = buf.stringView();
    writeVarUInt(sv.size(), ostr);
    ostr.write(sv.data(), sv.size());
}

void SerializationRow::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    UInt64 size = 0;
    readVarUInt(size, istr);
    String blob(size, '\0');
    istr.readStrict(blob.data(), size);
    ReadBufferFromString rb(blob);

    Tuple t;
    t.reserve(field_serializations.size());
    for (const auto & fs : field_serializations)
    {
        Field v;
        fs->deserializeBinary(v, rb, settings);
        t.push_back(std::move(v));
    }
    field = std::move(t);
}

void SerializationRow::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    writeRowFields(assert_cast<const ColumnTuple &>(column), row_num, field_serializations, settings, buf);
    auto sv = buf.stringView();
    writeVarUInt(sv.size(), ostr);
    ostr.write(sv.data(), sv.size());
}

void SerializationRow::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    UInt64 size = 0;
    readVarUInt(size, istr);
    String blob(size, '\0');
    istr.readStrict(blob.data(), size);
    ReadBufferFromString rb(blob);
    readRowFields(assert_cast<ColumnTuple &>(column), field_serializations, settings, rb);
}

void SerializationRow::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// Tuple literal form, e.g. ('alpha', 10, 'x').
    const auto & tuple = assert_cast<const ColumnTuple &>(column);
    writeChar('(', ostr);
    for (size_t i = 0; i < field_serializations.size(); ++i)
    {
        if (i != 0)
            writeChar(',', ostr);
        field_serializations[i]->serializeTextQuoted(tuple.getColumn(i), row_num, ostr, settings);
    }
    writeChar(')', ostr);
}

void SerializationRow::deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Text deserialization for Row data type is not supported; insert via the source columns instead");
}

bool SerializationRow::tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const
{
    return false;
}

void SerializationRow::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
}

void SerializationRow::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
}

void SerializationRow::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsDeserializeStatesCache *) const
{
}

void SerializationRow::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr &) const
{
    settings.path.push_back(Substream::Regular);
    SCOPE_EXIT(settings.path.pop_back());

    auto * stream = settings.getter(settings.path);
    if (!stream)
        return;

    const auto & tuple = assert_cast<const ColumnTuple &>(column);
    const size_t end = limit ? std::min(offset + limit, tuple.size()) : tuple.size();
    const size_t num_fields = field_serializations.size();
    const FormatSettings format_settings;

    /// Layout per row: [VarUInt row_size][fields...]. The size prefix lets the
    /// reader skip rows_offset rows without deserializing them.
    WriteBufferFromOwnString row_buf;
    for (size_t row = offset; row < end; ++row)
    {
        row_buf.restart();
        for (size_t i = 0; i < num_fields; ++i)
            field_serializations[i]->serializeBinary(tuple.getColumn(i), row, row_buf, format_settings);

        auto sv = row_buf.stringView();
        writeVarUInt(sv.size(), *stream);
        stream->write(sv.data(), sv.size());
    }
}

void SerializationRow::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr &,
    SubstreamsCache *) const
{
    settings.path.push_back(Substream::Regular);
    SCOPE_EXIT(settings.path.pop_back());

    auto * stream = settings.getter(settings.path);
    if (!stream)
        return;

    auto mutable_column = column->assumeMutable();
    auto & tuple = assert_cast<ColumnTuple &>(*mutable_column);
    const size_t num_fields = field_serializations.size();

    /// Must not use settings.format_settings: it is uninitialised on the MergeTree
    /// read path. A local default is correct for binary storage.
    const FormatSettings format_settings;

    if (limit)
        for (size_t i = 0; i < num_fields; ++i)
            tuple.getColumn(i).reserve(tuple.getColumn(i).size() + limit);

    for (size_t skipped = 0; skipped < rows_offset && !stream->eof(); ++skipped)
    {
        UInt64 size = 0;
        readVarUInt(size, *stream);
        stream->ignore(size);
    }

    /// Deserialize fields straight from the stream (fields are self-delimiting,
    /// so the row_size prefix is read but unused here).
    for (size_t read = 0; read < limit && !stream->eof(); ++read)
    {
        [[maybe_unused]] UInt64 size = 0;
        readVarUInt(size, *stream);
        readRowFields(tuple, field_serializations, format_settings, *stream);
    }

    column = std::move(mutable_column);
}

}
