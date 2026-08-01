#pragma once

#include <DataTypes/Serializations/ISerialization.h>

#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <Common/IntervalKind.h>

namespace DB
{

class SerializationInterval final : public SerializationNumber<typename DataTypeInterval::FieldType>
{
private:
    explicit SerializationInterval(IntervalKind kind_);

public:
    static UInt128 getHash(IntervalKind kind_);
    static SerializationPtr create(IntervalKind kind_);

private:
    IntervalKind interval_kind;
};

}
