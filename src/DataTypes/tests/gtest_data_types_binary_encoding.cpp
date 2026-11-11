#include <gtest/gtest.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}


void check(const DataTypePtr & type)
{
//    std::cerr << "Check " << type->getName() << "\n";
    WriteBufferFromOwnString ostr;
    encodeDataType(type, ostr);
    ReadBufferFromString istr(ostr.str());
    DataTypePtr decoded_type = decodeDataType(istr);
    ASSERT_TRUE(istr.eof());
    ASSERT_EQ(type->getName(), decoded_type->getName());
    ASSERT_TRUE(type->equals(*decoded_type));
}

GTEST_TEST(DataTypesBinaryEncoding, EncodeAndDecode)
{
    tryRegisterAggregateFunctions();
    check(std::make_shared<DataTypeNothing>());
    check(std::make_shared<DataTypeInt8>());
    check(std::make_shared<DataTypeUInt8>());
    check(std::make_shared<DataTypeInt16>());
    check(std::make_shared<DataTypeUInt16>());
    check(std::make_shared<DataTypeInt32>());
    check(std::make_shared<DataTypeUInt32>());
    check(std::make_shared<DataTypeInt64>());
    check(std::make_shared<DataTypeUInt64>());
    check(std::make_shared<DataTypeInt128>());
    check(std::make_shared<DataTypeUInt128>());
    check(std::make_shared<DataTypeInt256>());
    check(std::make_shared<DataTypeUInt256>());
    check(std::make_shared<DataTypeFloat32>());
    check(std::make_shared<DataTypeFloat64>());
    check(std::make_shared<DataTypeDate>());
    check(std::make_shared<DataTypeDate32>());
    check(std::make_shared<DataTypeDateTime>());
    check(std::make_shared<DataTypeDateTime>("EST"));
    check(std::make_shared<DataTypeDateTime>("CET"));
    check(std::make_shared<DataTypeDateTime64>(3));
    check(std::make_shared<DataTypeDateTime64>(3, "EST"));
    check(std::make_shared<DataTypeDateTime64>(3, "CET"));
    check(std::make_shared<DataTypeString>());
    check(std::make_shared<DataTypeFixedString>(10));
    check(DataTypeFactory::instance().get("Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = -128)"));
    check(DataTypeFactory::instance().get("Enum16('a' = 1, 'b' = 2, 'c' = 3, 'd' = -1000)"));
    check(std::make_shared<DataTypeDecimal32>(3, 6));
    check(std::make_shared<DataTypeDecimal64>(3, 6));
    check(std::make_shared<DataTypeDecimal128>(3, 6));
    check(std::make_shared<DataTypeDecimal256>(3, 6));
    check(std::make_shared<DataTypeUUID>());
    check(DataTypeFactory::instance().get("Array(UInt32)"));
    check(DataTypeFactory::instance().get("Array(Array(Array(UInt32)))"));
    check(DataTypeFactory::instance().get("Tuple(UInt32, String, UUID)"));
    check(DataTypeFactory::instance().get("Tuple(UInt32, String, Tuple(UUID, Date, IPv4))"));
    check(DataTypeFactory::instance().get("Tuple(c1 UInt32, c2 String, c3 UUID)"));
    check(DataTypeFactory::instance().get("Tuple(c1 UInt32, c2 String, c3 Tuple(c4 UUID, c5 Date, c6 IPv4))"));
    check(std::make_shared<DataTypeSet>());
    check(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond));
    check(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Microsecond));
    check(DataTypeFactory::instance().get("Nullable(UInt32)"));
    check(DataTypeFactory::instance().get("Nullable(Nothing)"));
    check(DataTypeFactory::instance().get("Nullable(UUID)"));
    check(std::make_shared<DataTypeFunction>(
        DataTypes{
            std::make_shared<DataTypeInt8>(),
            std::make_shared<DataTypeDate>(),
            DataTypeFactory::instance().get("Array(Array(Array(UInt32)))")},
        DataTypeFactory::instance().get("Tuple(c1 UInt32, c2 String, c3 UUID)")));
    DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
    Array parameters = {Field(0.1), Field(0.2)};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function = AggregateFunctionFactory::instance().get("quantiles", NullsAction::EMPTY, argument_types, parameters, properties);
    check(std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters));
    check(std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters, 2));
    check(DataTypeFactory::instance().get("AggregateFunction(sum, UInt64)"));
    check(DataTypeFactory::instance().get("AggregateFunction(quantiles(0.5, 0.9), UInt64)"));
    check(DataTypeFactory::instance().get("AggregateFunction(sequenceMatch('(?1)(?2)'), Date, UInt8, UInt8)"));
    check(DataTypeFactory::instance().get("AggregateFunction(sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))"));
    check(DataTypeFactory::instance().get("LowCardinality(UInt32)"));
    check(DataTypeFactory::instance().get("LowCardinality(Nullable(String))"));
    check(DataTypeFactory::instance().get("Map(String, UInt32)"));
    check(DataTypeFactory::instance().get("Map(String, Map(String, Map(String, UInt32)))"));
    check(std::make_shared<DataTypeIPv4>());
    check(std::make_shared<DataTypeIPv6>());
    check(DataTypeFactory::instance().get("Variant(String, UInt32, Date32)"));
    check(std::make_shared<DataTypeDynamic>());
    check(std::make_shared<DataTypeDynamic>(10));
    check(std::make_shared<DataTypeDynamic>(255));
    check(DataTypeFactory::instance().get("Bool"));
    check(DataTypeFactory::instance().get("SimpleAggregateFunction(sum, UInt64)"));
    check(DataTypeFactory::instance().get("SimpleAggregateFunction(maxMap, Tuple(Array(UInt32), Array(UInt32)))"));
    check(DataTypeFactory::instance().get("SimpleAggregateFunction(groupArrayArray(19), Array(UInt64))"));
    check(DataTypeFactory::instance().get("Nested(a UInt32, b UInt32)"));
    check(DataTypeFactory::instance().get("Nested(a UInt32, b Nested(c String, d Nested(e Date)))"));
    check(DataTypeFactory::instance().get("Ring"));
    check(DataTypeFactory::instance().get("Point"));
    check(DataTypeFactory::instance().get("Polygon"));
    check(DataTypeFactory::instance().get("MultiPolygon"));
    check(DataTypeFactory::instance().get("Tuple(Map(LowCardinality(String), Array(AggregateFunction(2, quantiles(0.1, 0.2), Float32))), Array(Array(Tuple(UInt32, Tuple(a Map(String, String), b Nullable(Date), c Variant(Tuple(g String, d Array(UInt32)), Date, Map(String, String)))))))"));
    check(DataTypeFactory::instance().get("JSON"));
    check(DataTypeFactory::instance().get("JSON(max_dynamic_paths=10)"));
    check(DataTypeFactory::instance().get("JSON(max_dynamic_paths=10, max_dynamic_types=10, a.b.c UInt32, SKIP a.c, b.g String, SKIP l.d.f)"));
}
