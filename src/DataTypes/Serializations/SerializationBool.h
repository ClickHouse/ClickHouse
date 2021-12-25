#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Columns/ColumnsNumber.h>
#include <unordered_set>

namespace DB
{

class SerializationBool final : public SerializationWrapper
{
private:
    static constexpr char str_true[5] = "true";
    static constexpr char str_false[6] = "false";

public:
    SerializationBool(const SerializationPtr & nested_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const  override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const  override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const  override;

    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

protected:
    void serializeCustom(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;
    void serializeSimple(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;
    void deserializeImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, std::function<bool(ReadBuffer & buf)> check_end_of_value) const;
    bool tryDeserializeAllVariants(ColumnUInt8 * column, ReadBuffer & istr) const;
};

}
