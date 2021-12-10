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
    DataTypeString.cpp
    DataTypeTuple.cpp
    DataTypeUUID.cpp
    DataTypesDecimal.cpp
    DataTypesNumber.cpp
    EnumValues.cpp
    FieldToDataType.cpp
    IDataType.cpp
    NestedUtils.cpp
    Serializations/ISerialization.cpp
    Serializations/SerializationAggregateFunction.cpp
    Serializations/SerializationArray.cpp
    Serializations/SerializationCustomSimpleText.cpp
    Serializations/SerializationDate.cpp
    Serializations/SerializationDateTime.cpp
    Serializations/SerializationDateTime64.cpp
    Serializations/SerializationDecimal.cpp
    Serializations/SerializationDecimalBase.cpp
    Serializations/SerializationEnum.cpp
    Serializations/SerializationFixedString.cpp
    Serializations/SerializationIP.cpp
    Serializations/SerializationLowCardinality.cpp
    Serializations/SerializationMap.cpp
    Serializations/SerializationNothing.cpp
    Serializations/SerializationNullable.cpp
    Serializations/SerializationNumber.cpp
    Serializations/SerializationString.cpp
    Serializations/SerializationTuple.cpp
    Serializations/SerializationTupleElement.cpp
    Serializations/SerializationUUID.cpp
    Serializations/SerializationWrapper.cpp
    convertMySQLDataType.cpp
    getLeastSupertype.cpp
    getMostSubtype.cpp
    registerDataTypeDateTime.cpp

)

END()
