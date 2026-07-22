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

/// Golden-bytes tests for the native leaf codecs that the query plan format carries as opaque
/// payload bytes (`ActionsDAG`, data type encoding, sort/aggregate descriptions, the header
/// encoding). Framing can skip *around* these blobs but cannot protect readers from changes
/// *inside* them, so any wire change here requires an explicit query plan version decision and a
/// `NativeProtocol.md` sync -- and this test failing is the gate that forces that decision.
/// If you changed a codec deliberately: bump/gate the plan version, update the spec, then update
/// the golden constants below.
/// (Set payload encodings are pinned indirectly by the round-trip tests in
/// gtest_query_plan_serialization.cpp.)

namespace
{

std::string hexify(const std::string & bytes)
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
    return hexify(out.str());
}

}

TEST(QueryPlanLeafCodecGoldens, DataTypeEncoding)
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
    for (const auto & [type_name, golden] : cases)
    {
        auto actual = capture([&](WriteBuffer & out) { encodeDataType(factory.get(type_name), out); });
        EXPECT_EQ(actual, golden) << "type " << type_name;
    }
}

TEST(QueryPlanLeafCodecGoldens, HeaderEncoding)
{
    ColumnsWithTypeAndName columns;
    columns.emplace_back(DataTypeUInt64().createColumn(), std::make_shared<DataTypeUInt64>(), "x");
    columns.emplace_back(DataTypeFactory::instance().get("String")->createColumn(), DataTypeFactory::instance().get("String"), "s");
    Block header(columns);

    auto actual = capture([&](WriteBuffer & out) { serializeQueryPlanHeader(header, out); });
    EXPECT_EQ(actual, "02017804017315");
}

TEST(QueryPlanLeafCodecGoldens, SortDescription)
{
    SortDescription description;
    description.emplace_back("a", 1, 1);
    description.emplace_back("b", -1, -1);

    auto actual = capture([&](WriteBuffer & out) { serializeSortDescription(description, out); });
    EXPECT_EQ(actual, "02016103016200");
}

TEST(QueryPlanLeafCodecGoldens, AggregateDescriptions)
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

TEST(QueryPlanLeafCodecGoldens, ActionsDag)
{
    tryRegisterFunctions();

    ColumnsWithTypeAndName inputs;
    inputs.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "x");
    inputs.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "y");

    ActionsDAG dag(inputs);
    auto resolver = FunctionFactory::instance().get("plus", getContext().context);
    const auto & sum_node = dag.addFunction(resolver, {dag.getInputs()[0], dag.getInputs()[1]}, "");
    dag.getOutputs() = {&sum_node};

    auto actual = capture([&](WriteBuffer & out)
    {
        SerializedSetsRegistry registry;
        dag.serialize(out, registry);
    });
    EXPECT_EQ(actual, "03000178040000000179040000040a706c757328782c207929040200010004706c75730200010102");
}
