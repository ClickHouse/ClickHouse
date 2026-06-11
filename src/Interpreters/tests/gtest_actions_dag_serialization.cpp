#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// Lambda capture DAGs reference constants from the enclosing DAG as INPUT nodes that
/// carry a pre-set constant column (see `addInputConstantColumnIfNecessary` in
/// `PlannerActionsVisitor`). The constant column must survive serialization: functions
/// that require constant arguments (e.g. `tupleElement` produced by `arrayMap(t -> t.2, ...)`)
/// are re-resolved on deserialization and fail without it.
TEST(ActionsDAGSerialization, InputWithConstantColumn)
{
    tryRegisterFunctions();
    const auto & context = getContext().context;

    auto tuple_type = std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>()});
    auto index_type = std::make_shared<DataTypeUInt8>();

    ActionsDAG dag;
    const auto & tuple_input = dag.addInput("t", tuple_type);
    const auto & index_input = dag.addInput(
        ColumnWithTypeAndName{index_type->createColumnConst(1, Field(UInt64(2))), index_type, "2_UInt8"});

    auto tuple_element = FunctionFactory::instance().get("tupleElement", context);
    const auto & function_node = dag.addFunction(tuple_element, {&tuple_input, &index_input}, {});
    dag.getOutputs().push_back(&function_node);

    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_registry;
    dag.serialize(out, serialized_registry);

    ReadBufferFromString in(out.str());
    DeserializedSetsRegistry deserialized_registry;
    auto restored = ActionsDAG::deserialize(in, deserialized_registry, context);

    /// The constant column on the input must be preserved.
    const ActionsDAG::Node * restored_index = nullptr;
    for (const auto * input : restored.getInputs())
        if (input->result_name == "2_UInt8")
            restored_index = input;

    ASSERT_NE(restored_index, nullptr);
    ASSERT_NE(restored_index->column, nullptr);
    EXPECT_TRUE(isColumnConst(*restored_index->column));

    /// The function must have been re-resolved with the right result type.
    ASSERT_EQ(restored.getOutputs().size(), 1u);
    EXPECT_EQ(restored.getOutputs().front()->result_type->getName(), "String");
}
