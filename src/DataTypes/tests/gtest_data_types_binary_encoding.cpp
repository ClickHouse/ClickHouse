#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>

#include <array>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}


static void check(const DataTypePtr & type)
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
    check(DataTypeFactory::instance().get("MultiPoint"));
    check(DataTypeFactory::instance().get("Tuple(Map(LowCardinality(String), Array(AggregateFunction(2, quantiles(0.1, 0.2), Float32))), Array(Array(Tuple(UInt32, Tuple(a Map(String, String), b Nullable(Date), c Variant(Tuple(g String, d Array(UInt32)), Date, Map(String, String)))))))"));
    check(DataTypeFactory::instance().get("JSON"));
    check(DataTypeFactory::instance().get("JSON(max_dynamic_paths=10)"));
    check(DataTypeFactory::instance().get("JSON(max_dynamic_paths=10, max_dynamic_types=10, a.b.c UInt32, SKIP a.c, b.g String, SKIP l.d.f)"));
}

namespace
{

DataTypePtr makeJSONTypeWithInsertionOrder(bool reverse)
{
    std::unordered_map<String, DataTypePtr> typed_paths;
    typed_paths.reserve(reverse ? 7 : 32);
    const std::array<std::pair<String, DataTypePtr>, 3> typed_path_values{{
        {"z.path", std::make_shared<DataTypeUInt8>()},
        {"a.path", std::make_shared<DataTypeString>()},
        {"middle.path", std::make_shared<DataTypeUInt64>()},
    }};
    if (reverse)
    {
        for (auto it = typed_path_values.rbegin(); it != typed_path_values.rend(); ++it)
            typed_paths.emplace(it->first, it->second);
    }
    else
    {
        for (const auto & [path, type] : typed_path_values)
            typed_paths.emplace(path, type);
    }

    std::unordered_set<String> paths_to_skip;
    paths_to_skip.reserve(reverse ? 7 : 32);
    const std::array<String, 3> paths_to_skip_values{"z.skip", "a.skip", "middle.skip"};
    if (reverse)
    {
        for (auto it = paths_to_skip_values.rbegin(); it != paths_to_skip_values.rend(); ++it)
            paths_to_skip.emplace(*it);
    }
    else
    {
        for (const auto & path : paths_to_skip_values)
            paths_to_skip.emplace(path);
    }

    return std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::move(typed_paths),
        std::move(paths_to_skip),
        std::vector<String>{"skip_z.*", "skip_a.*"},
        17,
        23);
}

void readJSONEncodingHeader(ReadBuffer & input)
{
    UInt8 type_index = 0;
    UInt8 version = 0;
    UInt64 max_dynamic_paths = 0;
    UInt8 max_dynamic_types = 0;
    readBinary(type_index, input);
    readBinary(version, input);
    readVarUInt(max_dynamic_paths, input);
    readBinary(max_dynamic_types, input);
    EXPECT_EQ(type_index, static_cast<UInt8>(BinaryTypeIndex::JSON));
    EXPECT_EQ(version, 0);
    EXPECT_EQ(max_dynamic_paths, 17);
    EXPECT_EQ(max_dynamic_types, 23);
}

}

GTEST_TEST(DataTypesBinaryEncoding, CanonicalJSONEncodingOrdersUnorderedPathCollections)
{
    const auto type = makeJSONTypeWithInsertionOrder(false);

    WriteBufferFromOwnString encoded;
    encodeCanonicalDataType(type, encoded);
    ReadBufferFromString input(encoded.str());

    readJSONEncodingHeader(input);

    UInt64 typed_path_count = 0;
    readVarUInt(typed_path_count, input);
    ASSERT_EQ(typed_path_count, 3);
    const std::array<std::pair<String, String>, 3> expected_typed_paths{{
        {"a.path", "String"},
        {"middle.path", "UInt64"},
        {"z.path", "UInt8"},
    }};
    for (const auto & [expected_path, expected_type] : expected_typed_paths)
    {
        String path;
        readStringBinary(path, input);
        EXPECT_EQ(path, expected_path);
        EXPECT_EQ(decodeDataType(input)->getName(), expected_type);
    }

    UInt64 skip_path_count = 0;
    readVarUInt(skip_path_count, input);
    ASSERT_EQ(skip_path_count, 3);
    for (const String & expected_path : {"a.skip", "middle.skip", "z.skip"})
    {
        String path;
        readStringBinary(path, input);
        EXPECT_EQ(path, expected_path);
    }

    UInt64 regexp_count = 0;
    readVarUInt(regexp_count, input);
    ASSERT_EQ(regexp_count, 2);
    for (const String & expected_regexp : {"skip_z.*", "skip_a.*"})
    {
        String regexp;
        readStringBinary(regexp, input);
        EXPECT_EQ(regexp, expected_regexp);
    }
    EXPECT_TRUE(input.eof());
}

GTEST_TEST(DataTypesBinaryEncoding, CanonicalJSONEncodingPropagatesRecursivelyAndIgnoresInsertionOrder)
{
    const auto first = std::make_shared<DataTypeArray>(makeJSONTypeWithInsertionOrder(false));
    const auto second = std::make_shared<DataTypeArray>(makeJSONTypeWithInsertionOrder(true));

    EXPECT_EQ(encodeCanonicalDataType(first), encodeCanonicalDataType(second));
}

GTEST_TEST(DataTypesBinaryEncoding, OrdinaryJSONEncodingPreservesLegacyContainerIterationOrder)
{
    const auto type = makeJSONTypeWithInsertionOrder(false);
    const auto & object_type = assert_cast<const DataTypeObject &>(*type);
    const String ordinary_encoding = encodeDataType(type);
    ReadBufferFromString input(ordinary_encoding);
    readJSONEncodingHeader(input);

    UInt64 typed_path_count = 0;
    readVarUInt(typed_path_count, input);
    ASSERT_EQ(typed_path_count, object_type.getTypedPaths().size());
    for (const auto & [expected_path, expected_type] : object_type.getTypedPaths())
    {
        String path;
        readStringBinary(path, input);
        EXPECT_EQ(path, expected_path);
        EXPECT_TRUE(decodeDataType(input)->equals(*expected_type));
    }

    UInt64 skip_path_count = 0;
    readVarUInt(skip_path_count, input);
    ASSERT_EQ(skip_path_count, object_type.getPathsToSkip().size());
    for (const auto & expected_path : object_type.getPathsToSkip())
    {
        String path;
        readStringBinary(path, input);
        EXPECT_EQ(path, expected_path);
    }

    UInt64 regexp_count = 0;
    readVarUInt(regexp_count, input);
    ASSERT_EQ(regexp_count, object_type.getPathRegexpsToSkip().size());
    for (const auto & expected_regexp : object_type.getPathRegexpsToSkip())
    {
        String regexp;
        readStringBinary(regexp, input);
        EXPECT_EQ(regexp, expected_regexp);
    }
    EXPECT_TRUE(input.eof());
}
