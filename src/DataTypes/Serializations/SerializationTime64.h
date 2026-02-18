#pragma once

#include <DataTypes/Serializations/SerializationDecimalBase.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <Common/DateLUT.h>

class DateLUTImpl;

namespace DB
{

class DataTypeTime64;

class SerializationTime64 final : public SerializationDecimalBase<Time64>
{
private:
    explicit SerializationTime64(UInt32 scale_);
    explicit SerializationTime64(UInt32 scale_, const DataTypeTime64 & /*time_type*/);

public:
    static SerializationPtr create(UInt32 scale_)
    {
        auto ptr = SerializationPtr(new SerializationTime64(scale_));
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    static SerializationPtr create(UInt32 scale_, const DataTypeTime64 & time_type)
    {
        auto ptr = SerializationPtr(new SerializationTime64(scale_, time_type));
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    ~SerializationTime64() override;

    String getName() const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
};

}
