#include <gtest/gtest.h>
#include <Common/FoundationDB/ProtobufTypeHelpers.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <base/Decimal.h>
#include <base/UUID.h>
#include <base/types.h>

using namespace DB;
using namespace std::string_literals;

TEST(MetadataStoreFoundationDB, SerializeProto)
{
    Array field;
    field.emplace_back(Null{});
    field.emplace_back(true);
    field.emplace_back(UInt64{1});
    field.emplace_back(UInt128{1, 2});
    field.emplace_back(UInt256{100, 200, 300, 400});
    field.emplace_back(Int64{1});
    field.emplace_back(Int128{1, 2});
    field.emplace_back(Int256{100, 200, 300, 400});
    field.emplace_back(UUIDHelpers::generateV4());
    field.emplace_back(1.0);
    field.emplace_back("String");
    field.emplace_back(DecimalField<Decimal32>(1, 2));
    field.emplace_back(DecimalField<Decimal64>(1, 2));
    field.emplace_back(DecimalField<Decimal128>({UInt128{1, 2}}, 2));
    field.emplace_back(DecimalField<Decimal256>({UInt256{100, 200, 300, 400}}, 2));

    Tuple tuple;
    tuple.emplace_back("1");
    field.emplace_back(tuple);

    Object obj;
    obj["aa"] = "b";
    field.emplace_back(obj);

    AggregateFunctionStateData agg_func_stat;
    agg_func_stat.name = "aa";
    agg_func_stat.data = "aba\x0e\xff\x00"s;

    auto proto = FoundationDB::toProto(field);
    auto field_new = FoundationDB::makeField(proto);
    ASSERT_EQ(field, field_new);
}
