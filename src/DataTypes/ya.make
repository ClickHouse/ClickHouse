# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
    clickhouse/src/Formats
)

SRCS(
    convertMySQLDataType.cpp
    DataTypeAggregateFunction.cpp
    DataTypeArray.cpp
    DataTypeCustomGeo.cpp
    DataTypeCustomIPv4AndIPv6.cpp
    DataTypeCustomSimpleAggregateFunction.cpp
    DataTypeCustomSimpleTextSerialization.cpp
    DataTypeDate.cpp
    DataTypeDateTime64.cpp
    DataTypeDateTime.cpp
    DataTypeDecimalBase.cpp
    DataTypeEnum.cpp
    DataTypeFactory.cpp
    DataTypeFixedString.cpp
    DataTypeFunction.cpp
    DataTypeInterval.cpp
    DataTypeLowCardinality.cpp
    DataTypeLowCardinalityHelpers.cpp
    DataTypeNothing.cpp
    DataTypeNullable.cpp
    DataTypeNumberBase.cpp
    DataTypesDecimal.cpp
    DataTypesNumber.cpp
    DataTypeString.cpp
    DataTypeTuple.cpp
    DataTypeUUID.cpp
    FieldToDataType.cpp
    getLeastSupertype.cpp
    getMostSubtype.cpp
    IDataType.cpp
    NestedUtils.cpp

)

END()
