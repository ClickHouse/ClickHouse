#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/InterpolateDescription.h>
#include <Core/ProtocolDefines.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto fill_type = std::make_shared<DataTypeFloat32>();
    auto interpolate_type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block(
        {ColumnWithTypeAndName(fill_type->createColumn(), fill_type, "n"),
         ColumnWithTypeAndName(interpolate_type->createColumn(), interpolate_type, "inter")}));
}

SortDescription makeSortDescription()
{
    FillColumnDescription fill_description;
    fill_description.fill_step = Field(UInt64(1));

    SortDescription sort_description;
    sort_description.emplace_back("n", 1, 1, nullptr, true, fill_description);
    return sort_description;
}

/// `num_outputs` outputs, all named `inter` and all reading the header column of that name. Two of them
/// are what the planner built for `INTERPOLATE (inter AS inter, inter AS inter)`, and what a sender that
/// predates the analyzer-side rejection of that shape can still put on the wire.
InterpolateDescriptionPtr makeInterpolateDescription(size_t num_outputs)
{
    auto type = std::make_shared<DataTypeUInt64>();
    ActionsDAG actions(NamesAndTypesList{{"inter", type}});
    const auto & input = *actions.getInputs().front();

    ActionsDAG::NodeRawConstPtrs outputs;
    VectorWithMemoryTracking<std::string> result_columns_order;
    for (size_t i = 0; i < num_outputs; ++i)
    {
        outputs.push_back(&actions.addAlias(input, "inter"));
        result_columns_order.emplace_back("inter");
    }
    actions.getOutputs() = std::move(outputs);

    UnorderedMapWithMemoryTracking<std::string, NameAndTypePair> required_columns_map;
    required_columns_map["inter"] = NameAndTypePair("inter", type);

    return std::make_shared<InterpolateDescription>(
        std::move(actions), std::move(required_columns_map), std::move(result_columns_order));
}

String serializeStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    step.serialize(ctx);
    return out.str();
}

QueryPlanStepPtr deserializeStep(const String & bytes, const SharedHeader & header)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    SharedHeaders input_headers{header};
    QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, getContext().context, input_headers, header, settings, 0,
        DBMS_QUERY_PLAN_SERIALIZATION_VERSION, 0, false};

    return FillingStep::deserialize(ctx);
}

}

/// `ActionsDAG::deserialize` rejects a repeated reference to one input node, but reads the outputs with no
/// such check, so a stream can carry two outputs of one name; `FillingTransform` pairs the executed
/// outputs with destination columns by position, and resolves both of these to the first `inter` column.
TEST(FillingStepSerialization, RejectsRepeatedInterpolateOutput)
{
    auto header = makeHeader();
    FillingStep step(header, makeSortDescription(), makeInterpolateDescription(2), false);
    const String bytes = serializeStep(step);

    try
    {
        deserializeStep(bytes, header);
        FAIL() << "expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
        EXPECT_NE(e.message().find("more than once"), std::string::npos) << e.message();
    }
}

/// The same fixture with one output, so that the rejection above is attributed to the repetition rather
/// than to anything else this plan carries.
TEST(FillingStepSerialization, AcceptsSingleInterpolateOutput)
{
    auto header = makeHeader();
    FillingStep step(header, makeSortDescription(), makeInterpolateDescription(1), false);
    const String bytes = serializeStep(step);

    QueryPlanStepPtr restored = deserializeStep(bytes, header);
    const auto * filling = typeid_cast<const FillingStep *>(restored.get());
    ASSERT_TRUE(filling);
    ASSERT_TRUE(filling->getInterpolateDescription());

    const auto & result_columns_order = filling->getInterpolateDescription()->result_columns_order;
    ASSERT_EQ(result_columns_order.size(), 1u);
    EXPECT_EQ(result_columns_order.front(), "inter");
}
