#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>

#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <Common/IntervalKind.h>

namespace DB
{

class SerializationInterval : public SerializationNumber<typename DataTypeInterval::FieldType>
{
private:
    explicit SerializationInterval(IntervalKind kind_);

public:
    static SerializationPtr create(IntervalKind kind_)
    {
        auto ptr = SerializationPtr(new SerializationInterval(kind_));
        return SerializationObjectPool::instance().getOrCreate(ptr->getHash(), std::move(ptr));
    }

    UInt128 getHash() const override;

    void serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSON(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextCSV(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextQuoted(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
private:
    using Base = SerializationNumber<typename DataTypeInterval::FieldType>;
    IntervalKind interval_kind;
};

}
