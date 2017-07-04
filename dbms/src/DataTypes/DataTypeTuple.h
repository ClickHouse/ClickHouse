#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Tuple data type.
  * Used as an intermediate result when evaluating expressions.
  * Also can be used as a column - the result of the query execution.
  * Can not be saved to tables.
  */
class DataTypeTuple final : public IDataType
{
private:
    DataTypes elems;
public:
    DataTypeTuple(DataTypes elems_) : elems(elems_) {}

    std::string getName() const override;
    DataTypePtr clone() const override { return std::make_shared<DataTypeTuple>(elems); }

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

    /// Tuples in CSV format will be serialized as separate columns (that is, losing their nesting in the tuple).
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    /** `limit` must be exactly equal to the number of serialized values.
      * It is because of this (the inability to read a smaller piece of recorded data) that Tuple can not be used to store data in tables.
      * (Although they can be used to transfer data over a network in Native format.)
      */
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    ColumnPtr createColumn() const override;
    ColumnPtr createConstColumn(size_t size, const Field & field) const override;

    Field getDefault() const override;
    const DataTypes & getElements() const { return elems; }
};

}

