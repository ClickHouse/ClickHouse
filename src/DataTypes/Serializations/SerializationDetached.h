#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// Implementation detail for parallel blocks marshalling on pipeline threads.
/// Input column for `serialize` method should be `ColumnBLOB`. To serialize it we simply copy the BLOB into the output buffer.
/// `deserialize` method will return a `ColumnBLOB` with the same BLOB as input. Deserialization and decompression of the BLOB will be done later by `UnmarshallBlocksTransform`.
class SerializationDetached final : public ISerialization
{
public:
    explicit SerializationDetached(const SerializationPtr & nested_);

    KindStack getKindStack() const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void
    deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
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
    [[noreturn]] static void throwInapplicable();

    SerializationPtr nested;
};

}
