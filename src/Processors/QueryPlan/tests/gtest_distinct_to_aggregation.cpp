#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

QueryPlan makePlanWithDeserializedDistinct(bool preliminary)
{
    auto type = std::make_shared<DataTypeUInt64>();
    auto header = std::make_shared<const Block>(Block{{type->createColumn(), type, "k"}});
    DistinctStep distinct(header, SizeLimits{}, /*limit_hint=*/0, Names{"k"}, preliminary);

    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    IQueryPlanStep::Serialization serialization{out, serialized_sets};
    serialization.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    distinct.serialize(serialization);
    QueryPlanSerializationSettings settings;
    distinct.serializeSettings(settings, serialization.version);

    ReadBufferFromString in(out.str());
    DeserializedSetsRegistry deserialized_sets;
    ContextPtr context = getContext().context;
    SharedHeaders input_headers{header};
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, context, input_headers, header, settings, 0, serialization.version, false};

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadNothingStep>(header));
    plan.addStep(DistinctStep::deserialize(deserialization, preliminary));
    return plan;
}

}

TEST(DistinctToAggregation, DeserializedStepUsesOptimizationSettings)
{
    for (bool enabled : {false, true})
    {
        for (bool preliminary : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "enabled=" << enabled << ", preliminary=" << preliminary);
            auto plan = makePlanWithDeserializedDistinct(preliminary);
            QueryPlanOptimizationSettings settings(getContext().context);
            settings.convert_distinct_to_aggregation = enabled;
            QueryPlan::Nodes nodes;
            QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);
            EXPECT_EQ(plan.getRootNode()->step->getName(), enabled && !preliminary ? "Aggregating" : "Distinct");
        }
    }
}

TEST(DistinctToAggregation, DuplicateColumnsRequireIdenticalValues)
{
    enum class DuplicateColumns
    {
        RepeatedOutput,
        AliasedOutput,
        DifferentOutputs,
        PassthroughOutput,
        PassthroughInputs,
    };

    for (auto duplicate_columns : {DuplicateColumns::RepeatedOutput, DuplicateColumns::AliasedOutput,
             DuplicateColumns::DifferentOutputs, DuplicateColumns::PassthroughOutput, DuplicateColumns::PassthroughInputs})
    {
        for (bool preliminary : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "duplicate_columns=" << static_cast<int>(duplicate_columns)
                                             << ", preliminary=" << preliminary);
            auto type = std::make_shared<DataTypeUInt64>();
            Block input_header{{type->createColumn(), type, "x"}, {type->createColumn(), type, "y"}};
            ActionsDAG actions;
            const auto & x = actions.addInput("x", type);
            switch (duplicate_columns)
            {
                case DuplicateColumns::RepeatedOutput:
                    actions.getOutputs() = {&x, &x};
                    break;
                case DuplicateColumns::AliasedOutput:
                    actions.getOutputs() = {&actions.addAlias(x, "k"), &actions.addAlias(x, "k")};
                    break;
                case DuplicateColumns::DifferentOutputs:
                {
                    const auto & y = actions.addInput("y", type);
                    actions.getOutputs() = {&actions.addAlias(x, "k"), &actions.addAlias(y, "k")};
                    break;
                }
                case DuplicateColumns::PassthroughOutput:
                    actions.getOutputs() = {&actions.addAlias(x, "y")};
                    break;
                case DuplicateColumns::PassthroughInputs:
                    input_header.insert({type->createColumn(), type, "y"});
                    actions.getOutputs() = {&x};
                    break;
            }

            QueryPlan plan;
            plan.addStep(std::make_unique<ReadNothingStep>(std::make_shared<const Block>(std::move(input_header))));
            plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(actions)));
            const auto header = plan.getCurrentHeader();
            if (preliminary)
                plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, header->getNames(), true));
            plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, header->getNames(), false));

            QueryPlanOptimizationSettings settings(getContext().context);
            settings.convert_distinct_to_aggregation = true;
            QueryPlan::Nodes nodes;
            QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);
            const bool can_convert = duplicate_columns == DuplicateColumns::RepeatedOutput
                || duplicate_columns == DuplicateColumns::AliasedOutput;
            EXPECT_EQ(plan.getRootNode()->step->getName(), can_convert ? "Aggregating" : "Distinct");
            EXPECT_TRUE(blocksHaveEqualStructure(*plan.getCurrentHeader(), *header));
        }
    }
}

TEST(DistinctToAggregation, ConstantColumnsPreserveOutputHeader)
{
    for (bool all_constant : {false, true})
    {
        for (const Names & keys : {Names{"c", "k"}, Names{"k"}, Names{}, Names{"c"}})
        {
            SCOPED_TRACE(::testing::Message() << "all_constant=" << all_constant << ", keys=" << ::testing::PrintToString(keys));
            auto type = std::make_shared<DataTypeUInt64>();
            ColumnPtr key_column = type->createColumn();
            if (all_constant)
                key_column = type->createColumnConst(0, UInt64(3));
            auto header = std::make_shared<const Block>(Block{
                {type->createColumnConst(0, UInt64(7)), type, "c"}, {key_column, type, "k"}});
            QueryPlan plan;
            plan.addStep(std::make_unique<ReadNothingStep>(header));
            plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, keys, false));

            QueryPlanOptimizationSettings settings(getContext().context);
            settings.convert_distinct_to_aggregation = true;
            QueryPlan::Nodes nodes;
            QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);

            const auto & root = *plan.getRootNode();
            const bool can_convert = !all_constant && keys != Names{"c"};
            EXPECT_EQ(root.step->getName(), can_convert ? "Expression" : "Distinct");
            EXPECT_TRUE(blocksHaveEqualStructure(*root.step->getOutputHeader(), *header));
            if (can_convert)
            {
                const auto * aggregation = typeid_cast<const AggregatingStep *>(root.children.front()->step.get());
                ASSERT_NE(aggregation, nullptr);
                EXPECT_EQ(aggregation->getParams().keys, Names{"k"});
            }
        }
    }
}
