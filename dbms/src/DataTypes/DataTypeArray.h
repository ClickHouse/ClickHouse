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
    DataTypeArray(DataTypePtr nested_);
    DataTypeArray(DataTypeTraits::EnrichedDataTypePtr enriched_nested_);

    std::string getName() const override
    {
        return "Array(" + nested->getName() + ")";
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
      * - the sizes are written/read in a separate column,
      *   and the caller must take care of writing/reading the sizes.
      * This is necessary, because when implementing nested structures, several arrays can have common sizes.
      */

    /** Write only values, without dimensions. The caller also needs to record the offsets somewhere. */
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    /** Read only values, without dimensions.
      * In this case, all the sizes must already be read in the column beforehand.
      */
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    /** Write the dimensions. */
    void serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const;

    /** Read the dimensions. Call this method before reading the values. */
    void deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const;

    ColumnPtr createColumn() const override;
    ColumnPtr createConstColumn(size_t size, const Field & field) const override;

    Field getDefault() const override
    {
        return Array();
    }

    const DataTypePtr & getNestedType() const { return nested; }
    const DataTypeTraits::EnrichedDataTypePtr & getEnrichedNestedType() const { return enriched_nested; }
    const DataTypePtr & getOffsetsType() const { return offsets; }

    /// Returns the data type found at the most nested level.
    const DataTypePtr & getMostNestedType() const;
};

}
