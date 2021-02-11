#pragma once

#include <DataTypes/Serializations/SerializationDecimalBase.h>

namespace DB
{

template <typename T>
class SerializationDecimal final : public SerializationDecimalBase<T>
{
public:
    using typename SerializationDecimalBase<T>::ColumnType;
    
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    void readText(T & x, ReadBuffer & istr, bool csv = false) const { readText(x, istr, this->precision, this->scale, csv); }

    static void readText(T & x, ReadBuffer & istr, UInt32 precision_, UInt32 scale_, bool csv = false);
    static bool tryReadText(T & x, ReadBuffer & istr, UInt32 precision_, UInt32 scale_);
};

}
