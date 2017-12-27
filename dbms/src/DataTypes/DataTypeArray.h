#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{


class DataTypeArray final : public IDataType
{
private:
    /// The type of array elements.
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    DataTypeArray(const DataTypePtr & nested_);

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

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested->cannotBeStoredInTables(); }
    bool textCanContainOnlyValidUTF8() const override { return nested->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested->isComparable(); };
    bool canBeComparedWithCollation() const override { return nested->canBeComparedWithCollation(); }

    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override
    {
        return nested->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
    }

    const DataTypePtr & getNestedType() const { return nested; }
};

}
