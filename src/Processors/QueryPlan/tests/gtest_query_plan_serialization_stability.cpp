#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/Serialization.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// Checks that the serialized bytes of the basic building blocks of the query plan format never
/// change unnoticed. The plan format carries these blocks as opaque byte blobs inside step
/// payloads (`ActionsDAG`, the binary data type encoding, sort/aggregate descriptions, the
/// header encoding). The payload framing lets a reader skip *around* such a blob, but an old
/// reader cannot decode a change *inside* one -- so any byte change here must be a deliberate
/// decision: bump or gate the plan version, and only then update the expected constants below.
/// These tests failing is the gate that forces that decision.
/// (Set payload encodings are pinned indirectly by the round-trip tests in
/// gtest_query_plan_serialization.cpp.)

namespace
{

std::string toHex(const std::string & bytes)
{
    std::string hex;
    hex.reserve(bytes.size() * 2);
    for (unsigned char c : bytes)
        hex += fmt::format("{:02x}", c);
    return hex;
}

template <typename F>
std::string capture(F && write)
{
    WriteBufferFromOwnString out;
    write(out);
    out.finalize();
    return toHex(out.str());
}

}

TEST(QueryPlanSerializationStability, DataTypeEncoding)
{
    const std::vector<std::pair<String, String>> cases =
    {
        {"UInt64", "04"},
        {"String", "15"},
        {"Nullable(Int32)", "2309"},
        {"Array(String)", "1e15"},
        {"LowCardinality(String)", "2615"},
        {"DateTime", "11"},
        {"Tuple(UInt8, String)", "1f020115"},
        {"Map(String, UInt64)", "271504"},
    };

    const auto & factory = DataTypeFactory::instance();
    for (const auto & [type_name, expected_hex] : cases)
    {
        auto actual = capture([&](WriteBuffer & out) { encodeDataType(factory.get(type_name), out); });
        EXPECT_EQ(actual, expected_hex) << "type " << type_name;
    }
}

TEST(QueryPlanSerializationStability, HeaderEncoding)
{
    ColumnsWithTypeAndName columns;
    columns.emplace_back(DataTypeUInt64().createColumn(), std::make_shared<DataTypeUInt64>(), "x");
    columns.emplace_back(DataTypeFactory::instance().get("String")->createColumn(), DataTypeFactory::instance().get("String"), "s");
    Block header(columns);

    auto actual = capture([&](WriteBuffer & out) { serializeQueryPlanHeader(header, out); });
    EXPECT_EQ(actual, "02017804017315");
}

TEST(QueryPlanSerializationStability, SortDescriptionEncoding)
{
    SortDescription description;
    description.emplace_back("a", 1, 1);
    description.emplace_back("b", -1, -1);

    auto actual = capture([&](WriteBuffer & out) { serializeSortDescription(description, out); });
    EXPECT_EQ(actual, "02016103016200");
}

TEST(QueryPlanSerializationStability, AggregateDescriptionsEncoding)
{
    tryRegisterAggregateFunctions();

    AggregateDescription aggregate;
    AggregateFunctionProperties properties;
    aggregate.function = AggregateFunctionFactory::instance().get(
        "sum", NullsAction::EMPTY, {std::make_shared<DataTypeUInt64>()}, {}, properties);
    aggregate.argument_names = {"x"};
    aggregate.column_name = "sum(x)";

    AggregateDescriptions aggregates{aggregate};
    auto actual = capture([&](WriteBuffer & out) { serializeAggregateDescriptions(aggregates, out); });
    EXPECT_EQ(actual, "010673756d287829010178040373756d00");
}

TEST(QueryPlanSerializationStability, ActionsDAGEncoding)
{
    tryRegisterFunctions();

    ColumnsWithTypeAndName inputs;
    inputs.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "x");
    inputs.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "y");

    ActionsDAG dag(inputs);
    auto resolver = FunctionFactory::instance().get("plus", getContext().context);
    const auto & plus_node = dag.addFunction(resolver, {dag.getInputs()[0], dag.getInputs()[1]}, "");
    dag.getOutputs() = {&plus_node};

    auto actual = capture([&](WriteBuffer & out)
    {
        SerializedSetsRegistry registry;
        dag.serialize(out, registry);
    });
    EXPECT_EQ(actual, "03000178040000000179040000040a706c757328782c207929040200010004706c75730200010102");
}
