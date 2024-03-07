#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInterval.h>


namespace DB
{

SerializationPtr DataTypeInterval::doGetDefaultSerialization() const { return std::make_shared<SerializationInterval>(kind); }

bool DataTypeInterval::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && kind == static_cast<const DataTypeInterval &>(rhs).kind;
}

void registerDataTypeInterval(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IntervalNanosecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Nanosecond)); });
    factory.registerSimpleDataType("IntervalMicrosecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Microsecond)); });
    factory.registerSimpleDataType("IntervalMillisecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Millisecond)); });
    factory.registerSimpleDataType("IntervalSecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Second)); });
    factory.registerSimpleDataType("IntervalMinute", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Minute)); });
    factory.registerSimpleDataType("IntervalHour", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Hour)); });
    factory.registerSimpleDataType("IntervalDay", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Day)); });
    factory.registerSimpleDataType("IntervalWeek", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Week)); });
    factory.registerSimpleDataType("IntervalMonth", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Month)); });
    factory.registerSimpleDataType("IntervalQuarter", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Quarter)); });
    factory.registerSimpleDataType("IntervalYear", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Year)); });
}

}
