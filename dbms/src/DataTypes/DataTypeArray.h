#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/EnrichedDataTypePtr.h>


namespace DB
{


class DataTypeArray final : public IDataType
{
private:
    /// Extended type of array elements.
    DataTypeTraits::EnrichedDataTypePtr enriched_nested;
    /// The type of array elements.
    DataTypePtr nested;
    /// Type of offsets.
    DataTypePtr offsets;

public:
    static constexpr bool is_parametric = true;

    DataTypeArray(const DataTypePtr & nested_);
    DataTypeArray(const DataTypeTraits::EnrichedDataTypePtr & enriched_nested_);

    std::string getName() const override
    {
        return "Array(" + nested->getName() + ")";
    }

    const char * getFamilyName() const override
    {
        return "Array";
    }

    bool canBeInsideNullable() const override
    {
        return false;
    }

    DataTypePtr clone() const override
    {
        return std::make_shared<DataTypeArray>(enriched_nested);
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr) const;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    /** Streaming serialization of arrays is arranged in a special way:
      * - elements placed in a row are written/read without array sizes;
      * - the sizes are written/read in a separate stream,
      * This is necessary, because when implementing nested structures, several arrays can have common sizes.
      */

    void enumerateStreams(StreamCallback callback, SubstreamPath path) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        OutputStreamGetter getter,
        size_t offset,
        size_t limit,
        bool position_independent_encoding,
        SubstreamPath path) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        InputStreamGetter getter,
        size_t limit,
        double avg_value_size_hint,
        bool position_independent_encoding,
        SubstreamPath path) const override;

    ColumnPtr createColumn() const override;

    Field getDefault() const override;

    const DataTypePtr & getNestedType() const { return nested; }
    const DataTypeTraits::EnrichedDataTypePtr & getEnrichedNestedType() const { return enriched_nested; }
    const DataTypePtr & getOffsetsType() const { return offsets; }
};

}
