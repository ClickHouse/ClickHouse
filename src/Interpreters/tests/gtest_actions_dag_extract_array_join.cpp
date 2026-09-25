#include <gtest/gtest.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>

using namespace DB;

/// An unused arrayJoin whose name a passenger already has: the element must cross the step under another name.
TEST(ActionsDAGExtractArrayJoin, UnusedResultDoesNotShadowPassenger)
{
    auto element_type = std::make_shared<DataTypeUInt8>();
    ActionsDAG dag;
    const auto & array = dag.addInput("a", std::make_shared<DataTypeArray>(element_type));
    const auto & passenger = dag.addInput("x", element_type);
    dag.addArrayJoin(array, "x");
    dag.getOutputs() = {&passenger};

    auto res = dag.extractFirstArrayJoin();
    ASSERT_TRUE(res.has_value());
    EXPECT_NE(res->array_join_column_name, "x");

    ASSERT_EQ(res->before.getOutputs().size(), 1);
    EXPECT_EQ(res->before.getOutputs().front()->result_name, res->array_join_column_name);

    bool has_passenger = false;
    bool has_element = false;
    for (const auto * input : res->after.getInputs())
    {
        has_passenger |= input->result_name == "x";
        has_element |= input->result_name == res->array_join_column_name;
    }
    EXPECT_TRUE(has_passenger);
    EXPECT_TRUE(has_element);
    ASSERT_EQ(res->after.getOutputs().size(), 1);
    EXPECT_EQ(res->after.getOutputs().front()->result_name, "x");
}
