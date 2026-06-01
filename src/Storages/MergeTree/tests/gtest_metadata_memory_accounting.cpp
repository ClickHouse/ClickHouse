#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeMemoryAccounting.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

AggregateFunctionPtr getAggregateFunction(const String & name, const DataTypes & argument_types)
{
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, argument_types, {}, properties);
}

}

TEST(MetadataMemoryAccounting, SplitOwnedAndLookupBytes)
{
    ColumnsSubstreams substreams;
    EXPECT_EQ(substreams.getTotalSubstreams(), 0);

    substreams.addColumn("long_column_name_for_metadata_accounting");
    substreams.addSubstreamToLastColumn("long_column_name_for_metadata_accounting.substream_one");
    substreams.addSubstreamToLastColumn("long_column_name_for_metadata_accounting.substream_two");

    EXPECT_EQ(substreams.getTotalSubstreams(), 2);

    const auto owned_bytes = substreams.getOwnedBytesAllocated();
    const auto lookup_bytes = substreams.getLookupBytesAllocated();
    const auto total_bytes = substreams.getBytesAllocated();

    EXPECT_GT(owned_bytes, 0);
    EXPECT_GT(lookup_bytes, 0);
    EXPECT_EQ(total_bytes, owned_bytes + lookup_bytes);

    substreams.addSubstreamToLastColumn("long_column_name_for_metadata_accounting.substream_two");
    EXPECT_EQ(substreams.getTotalSubstreams(), 2);
    EXPECT_EQ(substreams.getOwnedBytesAllocated(), owned_bytes);
    EXPECT_EQ(substreams.getLookupBytesAllocated(), lookup_bytes);
    EXPECT_EQ(substreams.getBytesAllocated(), total_bytes);
}

TEST(MetadataMemoryAccounting, CountsListNodesAndNames)
{
    NamesAndTypesList columns
    {
        {"long_column_name_for_metadata_accounting_a", std::make_shared<DataTypeString>()},
        {"long_column_name_for_metadata_accounting_b", DataTypeFactory::instance().get("Array(Tuple(String, UInt64))")},
    };

    const auto bytes = columns.getBytesAllocated();
    EXPECT_GE(bytes, columns.size() * (sizeof(NameAndTypePair) + 2 * sizeof(void *)));

    columns.emplace_back("much_longer_column_name_for_metadata_accounting_to_force_growth", std::make_shared<DataTypeUInt64>());
    EXPECT_GT(columns.getBytesAllocated(), bytes);
}

TEST(MetadataMemoryAccounting, UsesForEachChildShallowCallback)
{
    tryRegisterAggregateFunctions();

    const auto tuple_type = DataTypeFactory::instance().get(
        "Tuple(arr Array(String), m Map(String, Array(UInt32)), e Enum8('long_enum_value_name_for_accounting' = 1))");
    EXPECT_GT(getDataTypeBytesAllocated(*tuple_type), getDataTypeShallowBytesAllocated(*tuple_type));

    const DataTypes aggregate_arguments{DataTypeFactory::instance().get("Tuple(String, Array(UInt32))")};
    const auto aggregate_type = std::make_shared<DataTypeAggregateFunction>(
        getAggregateFunction("uniq", aggregate_arguments), aggregate_arguments, Array{});
    EXPECT_GT(getDataTypeBytesAllocated(*aggregate_type), getDataTypeShallowBytesAllocated(*aggregate_type));

    const DataTypes nested_aggregate_arguments{std::make_shared<DataTypeString>()};
    const auto nested_aggregate_type = std::make_shared<DataTypeAggregateFunction>(
        getAggregateFunction("uniq", nested_aggregate_arguments), nested_aggregate_arguments, Array{});
    const auto array_of_aggregate = std::make_shared<DataTypeArray>(nested_aggregate_type);
    EXPECT_GT(
        getDataTypeBytesAllocated(*array_of_aggregate),
        getDataTypeShallowBytesAllocated(*array_of_aggregate) + getDataTypeShallowBytesAllocated(*nested_aggregate_type));

    size_t aggregate_child_count = 0;
    nested_aggregate_type->forEachChild([&](const IDataType &) { ++aggregate_child_count; });
    EXPECT_EQ(aggregate_child_count, 0);

    const auto qbit_type = std::make_shared<DataTypeQBit>(std::make_shared<DataTypeFloat32>(), 4);
    EXPECT_GE(
        getDataTypeBytesAllocated(*qbit_type),
        getDataTypeShallowBytesAllocated(*qbit_type) + getDataTypeShallowBytesAllocated(*qbit_type->getElementType()));

    const auto array_of_qbit = std::make_shared<DataTypeArray>(qbit_type);
    EXPECT_GT(
        getDataTypeBytesAllocated(*array_of_qbit),
        getDataTypeShallowBytesAllocated(*array_of_qbit) + getDataTypeShallowBytesAllocated(*qbit_type));

    size_t qbit_child_count = 0;
    qbit_type->forEachChild([&](const IDataType &) { ++qbit_child_count; });
    EXPECT_EQ(qbit_child_count, 0);

    const auto function_type = std::make_shared<DataTypeFunction>(
        DataTypes{std::make_shared<DataTypeString>()},
        std::make_shared<DataTypeUInt64>());
    EXPECT_GT(getDataTypeBytesAllocated(*function_type), getDataTypeShallowBytesAllocated(*function_type));

    size_t function_child_count = 0;
    function_type->forEachChild([&](const IDataType &) { ++function_child_count; });
    EXPECT_EQ(function_child_count, 0);
}

TEST(SerializationInfoAccounting, CountsNestedTupleInfos)
{
    SerializationInfoSettings settings;
    settings.version = MergeTreeSerializationInfoVersion::BASIC;
    settings.ratio_of_defaults_for_sparse = 0.5;
    settings.choose_kind = true;

    auto string_type = std::make_shared<DataTypeString>();
    NamesAndTypesList columns
    {
        {"long_string_column_for_metadata_accounting", string_type},
        {"long_tuple_column_for_metadata_accounting", std::make_shared<DataTypeTuple>(DataTypes{string_type, string_type}, Strings{"first", "second"})},
    };

    SerializationInfoByName infos(columns, settings);

    EXPECT_GT(infos.getTotalSerializationInfos(), infos.size());
    EXPECT_GT(infos.getBytesAllocated(), 0);

    auto tuple_info = infos.tryGet("long_tuple_column_for_metadata_accounting");
    ASSERT_NE(tuple_info, nullptr);
    tuple_info->setKindStack({ISerialization::Kind::DEFAULT, ISerialization::Kind::DETACHED});

    auto cloned_infos = infos.clone();
    auto cloned_tuple_info = cloned_infos.tryGet("long_tuple_column_for_metadata_accounting");
    ASSERT_NE(cloned_tuple_info, nullptr);
    EXPECT_EQ(cloned_tuple_info->getKindStack(), tuple_info->getKindStack());
}
