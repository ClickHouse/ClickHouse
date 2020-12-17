# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    clickhouse/src/Formats
)


SRCS(
    DataTypeAggregateFunction.cpp
    DataTypeArray.cpp
    DataTypeCustomGeo.cpp
    DataTypeCustomIPv4AndIPv6.cpp
    DataTypeCustomSimpleAggregateFunction.cpp
    DataTypeCustomSimpleTextSerialization.cpp
    DataTypeDate.cpp
    DataTypeDateTime.cpp
    DataTypeDateTime64.cpp
    DataTypeDecimalBase.cpp
    DataTypeEnum.cpp
    DataTypeFactory.cpp
    DataTypeFixedString.cpp
    DataTypeFunction.cpp
    DataTypeInterval.cpp
    DataTypeLowCardinality.cpp
    DataTypeLowCardinalityHelpers.cpp
    DataTypeMap.cpp
    DataTypeNested.cpp
    DataTypeNothing.cpp
    DataTypeNullable.cpp
    DataTypeNumberBase.cpp
    DataTypeOneElementTuple.cpp
    DataTypeString.cpp
    DataTypeTuple.cpp
    DataTypeUUID.cpp
    DataTypesDecimal.cpp
    DataTypesNumber.cpp
    FieldToDataType.cpp
    IDataType.cpp
    NestedUtils.cpp
    convertMySQLDataType.cpp
    getLeastSupertype.cpp
    getMostSubtype.cpp
    registerDataTypeDateTime.cpp

)

END()
