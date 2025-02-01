#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/** Serialization for sparse representation.
 *  Only '{serialize,deserialize}BinaryBulk' makes sense.
 *  Format:
 *    Values and offsets are written to separate substreams.
 *    There are written only non-default values.
 *
 *    Offsets have position independent format: as i-th offset there
 *    is written number of default values, that precedes the i-th non-default value.
 *    Offsets are written in VarInt encoding.
 *    Additionally at the end of every call of 'serializeBinaryBulkWithMultipleStreams'
 *    there is written number of default values in the suffix of part of column,
 *    that we currently writing. This value also marked with a flag, that means the end of portion of data.
 *    This value is used, e.g. to allow independent reading of granules in MergeTree.
 */
class SerializationDetached final : public ISerialization
{
public:
    explicit SerializationDetached(const SerializationPtr & nested_);

    Kind getKind() const override { return Kind::DETACHED; }

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    /// Allows to write ColumnSparse and other columns in sparse serialization.
    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    /// Allows to read only ColumnSparse.
    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeTextEscaped(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeTextEscaped(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeTextQuoted(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeTextQuoted(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeTextCSV(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeTextCSV(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeWholeText(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }
    void deserializeTextJSON(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwInapplicable(); }

    void serializeTextXML(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwInapplicable(); }

private:
    [[noreturn]] static void throwInapplicable()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBlob should be converted to a regular column before usage");
    }

    template <typename Reader>
    void deserialize(IColumn & column, Reader && reader) const;

    SerializationPtr nested;
};

}
