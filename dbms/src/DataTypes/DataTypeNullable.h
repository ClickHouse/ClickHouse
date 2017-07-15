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
    DataTypeNullable(DataTypePtr nested_data_type_);
    std::string getName() const override { return "Nullable(" + nested_data_type->getName() + ")"; }
    bool isNullable() const override { return true; }

    bool isNumeric() const override { return nested_data_type->isNumeric(); }    /// TODO Absolutely wrong.
    bool isNumericNotNullable() const override { return false; }
    bool behavesAsNumber() const override { return nested_data_type->behavesAsNumber(); }    /// TODO Absolutely wrong.

    DataTypePtr clone() const override { return std::make_shared<DataTypeNullable>(nested_data_type->clone()); }

    /// Bulk serialization and deserialization is processing only nested columns. You should process null byte map separately.
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override { nested_data_type->serializeBinary(field, ostr); }
    void deserializeBinary(Field & field, ReadBuffer & istr) const override { nested_data_type->deserializeBinary(field, istr); }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    /** It is questionable, how NULL values could be represented in CSV. There are three variants:
      * 1. \N
      * 2. empty string (without quotes)
      * 3. NULL
      * Now we support only first.
      * In CSV, non-NULL string value, starting with \N characters, must be placed in quotes, to avoid ambiguity.
      */
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    ColumnPtr createColumn() const override;
    ColumnPtr createConstColumn(size_t size, const Field & field) const override;

    Field getDefault() const override { return Null(); }

    size_t getSizeOfField() const override { return nested_data_type->getSizeOfField(); }    /// TODO Absolutely wrong.

    DataTypePtr & getNestedType() { return nested_data_type; }
    const DataTypePtr & getNestedType() const { return nested_data_type; }

private:
    DataTypePtr nested_data_type;
};

}
