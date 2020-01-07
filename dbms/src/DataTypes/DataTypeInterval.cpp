#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

bool DataTypeInterval::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && kind == static_cast<const DataTypeInterval &>(rhs).kind;
}

template <IntervalKind::Kind kind>
static DataTypePtr create(const String & /*type_name*/)
{
    return DataTypePtr(std::make_shared<DataTypeInterval>(kind));
}

void registerDataTypeInterval(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IntervalSecond", create<IntervalKind::Second>);
    factory.registerSimpleDataType("IntervalMinute", create<IntervalKind::Minute>);
    factory.registerSimpleDataType("IntervalHour", create<IntervalKind::Hour>);
    factory.registerSimpleDataType("IntervalDay", create<IntervalKind::Day>);
    factory.registerSimpleDataType("IntervalWeek", create<IntervalKind::Week>);
    factory.registerSimpleDataType("IntervalMonth", create<IntervalKind::Month>);
    factory.registerSimpleDataType("IntervalQuarter", create<IntervalKind::Quarter>);
    factory.registerSimpleDataType("IntervalYear", create<IntervalKind::Year>);
}

}
