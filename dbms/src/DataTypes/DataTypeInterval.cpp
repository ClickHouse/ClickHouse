#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

bool DataTypeInterval::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && kind == static_cast<const DataTypeInterval &>(rhs).kind;
}


void registerDataTypeInterval(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IntervalSecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Second)); });
    factory.registerSimpleDataType("IntervalMinute", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Minute)); });
    factory.registerSimpleDataType("IntervalHour", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Hour)); });
    factory.registerSimpleDataType("IntervalDay", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Day)); });
    factory.registerSimpleDataType("IntervalWeek", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Week)); });
    factory.registerSimpleDataType("IntervalMonth", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Month)); });
    factory.registerSimpleDataType("IntervalYear", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Year)); });
}

}
