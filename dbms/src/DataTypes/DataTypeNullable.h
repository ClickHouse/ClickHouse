#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType
{
public:
    static constexpr bool is_parametric = true;

    DataTypeNullable(const DataTypePtr & nested_data_type_);
    std::string getName() const override { return "Nullable(" + nested_data_type->getName() + ")"; }
    const char * getFamilyName() const override { return "Nullable"; }

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

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override { nested_data_type->serializeBinary(field, ostr); }
    void deserializeBinary(Field & field, ReadBuffer & istr) const override { nested_data_type->deserializeBinary(field, istr); }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    /** It is questionable, how NULL values could be represented in CSV. There are three variants:
      * 1. \N
      * 2. empty string (without quotes)
      * 3. NULL
      * Now we support only first.
      * In CSV, non-NULL string value, starting with \N characters, must be placed in quotes, to avoid ambiguity.
      */
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return Null(); }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested_data_type->cannotBeStoredInTables(); }
    bool shouldAlignRightInPrettyFormats() const override { return nested_data_type->shouldAlignRightInPrettyFormats(); }
    bool textCanContainOnlyValidUTF8() const override { return nested_data_type->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested_data_type->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested_data_type->canBeComparedWithCollation(); }
    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return nested_data_type->isSummable(); }
    bool canBeUsedInBooleanContext() const override { return nested_data_type->canBeUsedInBooleanContext(); }
    bool haveMaximumSizeOfValue() const override { return nested_data_type->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return 1 + nested_data_type->getMaximumSizeOfValueInMemory(); }
    bool isNullable() const override { return true; }
    size_t getSizeOfValueInMemory() const override;
    bool onlyNull() const override;

    const DataTypePtr & getNestedType() const { return nested_data_type; }

private:
    DataTypePtr nested_data_type;
};


DataTypePtr makeNullable(const DataTypePtr & type);
DataTypePtr removeNullable(const DataTypePtr & type);

}
