LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    clickhouse/dbms/src/Formats
)

SRCS(
    DataTypeAggregateFunction.cpp
    DataTypeArray.cpp
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
    IDataType.cpp
)

END()
