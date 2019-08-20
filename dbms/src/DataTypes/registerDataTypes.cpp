#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/registerDataTypes.h>

namespace DB
{

void registerDataTypeNumbers(DataTypeFactory & factory);
void registerDataTypeDecimal(DataTypeFactory & factory);
void registerDataTypeDate(DataTypeFactory & factory);
void registerDataTypeDateTime(DataTypeFactory & factory);
void registerDataTypeString(DataTypeFactory & factory);
void registerDataTypeFixedString(DataTypeFactory & factory);
void registerDataTypeEnum(DataTypeFactory & factory);
void registerDataTypeArray(DataTypeFactory & factory);
void registerDataTypeTuple(DataTypeFactory & factory);
void registerDataTypeNullable(DataTypeFactory & factory);
void registerDataTypeNothing(DataTypeFactory & factory);
void registerDataTypeUUID(DataTypeFactory & factory);
void registerDataTypeAggregateFunction(DataTypeFactory & factory);
void registerDataTypeNested(DataTypeFactory & factory);
void registerDataTypeInterval(DataTypeFactory & factory);
void registerDataTypeLowCardinality(DataTypeFactory & factory);
void registerDataTypeDomainIPv4AndIPv6(DataTypeFactory & factory);
void registerDataTypeDomainSimpleAggregateFunction(DataTypeFactory & factory);

void registerDataTypes()
{
    auto & factory = DataTypeFactory::instance();

    registerDataTypeNumbers(factory);
    registerDataTypeDecimal(factory);
    registerDataTypeDate(factory);
    registerDataTypeDateTime(factory);
    registerDataTypeString(factory);
    registerDataTypeFixedString(factory);
    registerDataTypeEnum(factory);
    registerDataTypeArray(factory);
    registerDataTypeTuple(factory);
    registerDataTypeNullable(factory);
    registerDataTypeNothing(factory);
    registerDataTypeUUID(factory);
    registerDataTypeAggregateFunction(factory);
    registerDataTypeNested(factory);
    registerDataTypeInterval(factory);
    registerDataTypeLowCardinality(factory);
    registerDataTypeDomainIPv4AndIPv6(factory);
    registerDataTypeDomainSimpleAggregateFunction(factory);
}

}
