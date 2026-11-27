#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{

using ActionType = ActionsDAG::ActionType;

const DataTypePtr u8 = std::make_shared<DataTypeUInt8>();
const DataTypePtr u16 = std::make_shared<DataTypeUInt16>();
const DataTypePtr u64 = std::make_shared<DataTypeUInt64>();

/// Writes a node without a constant column the way `ActionsDAG::serialize` does.
void writeNode(WriteBuffer & out, ActionType type, const String & name, const DataTypePtr & result_type,
    const std::vector<size_t> & children, UInt8 column_flags = 0)
{
    writeIntBinary(static_cast<UInt8>(type), out);
    writeStringBinary(name, out);
    encodeDataType(result_type, out);
    writeVarUInt(children.size(), out);
    for (size_t child : children)
        writeVarUInt(child, out);
    writeIntBinary(column_flags, out);
}

void writeIds(WriteBuffer & out, const std::vector<size_t> & ids)
{
    writeVarUInt(ids.size(), out);
    for (size_t id : ids)
        writeVarUInt(id, out);
}

ActionsDAG deserializeDag(const String & data)
{
    tryRegisterFunctions();
    ReadBufferFromString in(data);
    DeserializedSetsRegistry registry;
    return ActionsDAG::deserialize(in, registry, getContext().context);
}

void expectRejected(const String & data)
{
    try
    {
        deserializeDag(data);
        FAIL() << "deserialization succeeded";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
        EXPECT_NE(e.message().find("is out of range"), std::string::npos) << e.message();
    }
}

/// `plus(input, input)` with the nodes in the given order, `forward` puts the function first.
String plusStream(const String & input, bool forward, const std::vector<size_t> & outputs)
{
    WriteBufferFromOwnString out;
    writeVarUInt(2, out);
    const String name = "plus(" + input + ", " + input + ")";
    if (forward)
    {
        writeNode(out, ActionType::FUNCTION, name, u16, {1, 1});
        writeStringBinary("plus", out);
        writeNode(out, ActionType::INPUT, input, u8, {});
        writeIds(out, {1});
    }
    else
    {
        writeNode(out, ActionType::INPUT, input, u8, {});
        writeNode(out, ActionType::FUNCTION, name, u16, {0, 0});
        writeStringBinary("plus", out);
        writeIds(out, {0});
    }
    writeIds(out, outputs);
    return out.str();
}

/// A single lambda node `x -> plus(x, x)` whose capture DAG is `inner`.
String lambdaStream(const String & inner)
{
    WriteBufferFromOwnString out;
    writeVarUInt(1, out);
    writeNode(out, ActionType::FUNCTION, "lambda", std::make_shared<DataTypeFunction>(DataTypes{u8}, u16), {}, 4);
    writeStringBinary("lambda", out);
    writeStringBinary("plus(x, x)", out);
    encodeDataType(u16, out);
    writeVarUInt(0, out);
    writeVarUInt(0, out);
    writeVarUInt(1, out);
    writeStringBinary("x", out);
    encodeDataType(u8, out);
    out.write(inner.data(), inner.size());
    writeIds(out, {});
    writeIds(out, {0});
    return out.str();
}

}

TEST(ActionsDAGDeserialization, ValidStreamMatchesSerialize)
{
    tryRegisterFunctions();
    ActionsDAG dag;
    const auto & a = dag.addInput("a", u8);
    const auto & plus = dag.addFunction(FunctionFactory::instance().get("plus", getContext().context), {&a, &a}, {});
    dag.getOutputs() = {&plus};

    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    dag.serialize(out, registry);
    const String valid = plusStream("a", false, {1});
    EXPECT_EQ(out.str(), valid);

    auto restored = deserializeDag(valid);
    ASSERT_EQ(restored.getOutputs().size(), 1);
    EXPECT_EQ(restored.getOutputs()[0]->result_name, "plus(a, a)");
    EXPECT_TRUE(restored.getOutputs()[0]->result_type->equals(*u16));
}

TEST(ActionsDAGDeserialization, RejectsForwardReference)
{
    expectRejected(plusStream("a", true, {0}));
}

TEST(ActionsDAGDeserialization, RejectsSelfReference)
{
    WriteBufferFromOwnString out;
    writeVarUInt(2, out);
    writeNode(out, ActionType::INPUT, "a", u8, {});
    writeNode(out, ActionType::FUNCTION, "f", u64, {1, 0});
    writeStringBinary("plus", out);
    writeIds(out, {0});
    writeIds(out, {1});
    expectRejected(out.str());
}

TEST(ActionsDAGDeserialization, RejectsCycle)
{
    WriteBufferFromOwnString out;
    writeVarUInt(2, out);
    writeNode(out, ActionType::ALIAS, "x", u8, {1});
    writeNode(out, ActionType::ALIAS, "y", u8, {0});
    writeIds(out, {});
    writeIds(out, {0});
    expectRejected(out.str());
}

TEST(ActionsDAGDeserialization, RejectsInputOutOfRange)
{
    WriteBufferFromOwnString out;
    writeVarUInt(1, out);
    writeNode(out, ActionType::INPUT, "a", u8, {});
    writeIds(out, {1});
    writeIds(out, {0});
    expectRejected(out.str());
}

TEST(ActionsDAGDeserialization, RejectsOutputOutOfRange)
{
    expectRejected(plusStream("a", false, {2}));
}

TEST(ActionsDAGDeserialization, NestedLambda)
{
    auto restored = deserializeDag(lambdaStream(plusStream("x", false, {1})));
    ASSERT_EQ(restored.getOutputs().size(), 1);
    EXPECT_EQ(restored.getOutputs()[0]->function_base->getName(), "lambda");

    expectRejected(lambdaStream(plusStream("x", true, {0})));
}
