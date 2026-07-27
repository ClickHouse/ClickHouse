#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <mutex>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// `ReadFromStorageStep::deserialize` reads a client-supplied plan (TCPHandler::receiveQueryPlan),
/// so it is a trust boundary: it must reject a payload naming another storage and a header that is
/// not the canonical `system.one` shape, because the chunk it builds is hardcoded to one UInt8
/// column. Those rejections are unreachable from SQL (no legitimate sender produces such a plan),
/// hence this gtest. The paired encoders are `ReadFromStorageStep::serialize` and
/// `ReadFromSystemOneStep::serialize`; both write exactly one string, "SystemOne".
namespace
{

/// A stand-in for the encoders: serializes under the registered "ReadFromStorage" name with a
/// caller-chosen payload and output header, which is what lets a test feed both well-formed and
/// hostile streams through the real registry.
class FakeReadFromStorageStep final : public ISourceStep
{
public:
    FakeReadFromStorageStep(SharedHeader output_header_, String payload_)
        : ISourceStep(std::move(output_header_)), payload(std::move(payload_))
    {
    }

    String getName() const override { return "FakeReadFromStorage"; }
    String getSerializationName() const override { return "ReadFromStorage"; }

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FakeReadFromStorageStep is not executable");
    }

    void serialize(Serialization & ctx) const override { writeStringBinary(payload, ctx.out); }
    bool isSerializable() const override { return true; }

private:
    String payload;
};

SharedHeader canonicalSystemOneHeader()
{
    auto type = std::make_shared<DataTypeUInt8>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(type->createColumn(), type, "dummy")});
}

String serializePlanWith(SharedHeader output_header, const String & payload)
{
    QueryPlan plan;
    plan.addStep(std::make_unique<FakeReadFromStorageStep>(std::move(output_header), payload));

    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    out.finalize();
    return out.str();
}

QueryPlan deserializePlan(const String & data, const ContextPtr & context)
{
    ReadBufferFromString in(data);
    return QueryPlan::makeSets(QueryPlan::deserialize(in, context, /*max_type_complexity=*/0), context);
}

}

namespace DB
{
void registerReadFromStorageStep(QueryPlanStepRegistry & registry);
}

class ReadFromStorageSystemOneDeserialize : public ::testing::Test
{
protected:
    void SetUp() override
    {
        /// The registry is a process-wide singleton and throws on a duplicate name, so register the
        /// one step under test exactly once. Registering the whole set (registerPlanSteps) would
        /// collide with gtest_distributed_query, which registers its own subset in the same binary.
        static std::once_flag registered;
        std::call_once(registered, [] { registerReadFromStorageStep(QueryPlanStepRegistry::instance()); });
    }
};

/// The legacy stream that senders already produce must keep working: a canonical header plus the
/// "SystemOne" payload deserializes into a source yielding the single `system.one` row.
TEST_F(ReadFromStorageSystemOneDeserialize, WellFormedStreamYieldsSingleRow)
{
    auto context = Context::createCopy(getContext().context);

    auto plan = deserializePlan(serializePlanWith(canonicalSystemOneHeader(), "SystemOne"), context);
    ASSERT_TRUE(plan.isInitialized());

    auto builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    QueryPipeline pipeline(QueryPipelineBuilder::getPipeline(std::move(*builder)));

    PullingPipelineExecutor executor(pipeline);
    Block block;
    size_t total_rows = 0;
    while (executor.pull(block))
        total_rows += block.rows();

    EXPECT_EQ(total_rows, 1u);
}

/// A payload naming any other storage is bad input, not a logical error: a logical error would
/// abort debug/sanitizer builds on a client-controlled path.
TEST_F(ReadFromStorageSystemOneDeserialize, ForeignStorageNamePayloadIsRejected)
{
    auto context = Context::createCopy(getContext().context);
    auto data = serializePlanWith(canonicalSystemOneHeader(), "NotSystemOne");

    try
    {
        deserializePlan(data, context);
        FAIL() << "expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.displayText();
    }
}

/// A non-canonical header must be rejected at deserialization. Without the check the step is built
/// from the wire header while its chunk stays hardcoded to one UInt8 column, and the mismatch only
/// surfaces much deeper, inside OutputPort, as a remote-triggerable logical error.
TEST_F(ReadFromStorageSystemOneDeserialize, NonCanonicalHeaderIsRejected)
{
    auto context = Context::createCopy(getContext().context);

    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto string_type = std::make_shared<DataTypeString>();

    std::vector<SharedHeader> hostile_headers = {
        /// Right count and name, wrong type.
        std::make_shared<const Block>(Block{ColumnWithTypeAndName(string_type->createColumn(), string_type, "dummy")}),
        /// Right count and type, wrong name.
        std::make_shared<const Block>(Block{ColumnWithTypeAndName(uint8_type->createColumn(), uint8_type, "not_dummy")}),
        /// Too many columns: this is the shape that reaches OutputPort unchecked.
        std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(uint8_type->createColumn(), uint8_type, "dummy"),
            ColumnWithTypeAndName(uint8_type->createColumn(), uint8_type, "extra")}),
    };

    for (const auto & header : hostile_headers)
    {
        auto data = serializePlanWith(header, "SystemOne");
        try
        {
            deserializePlan(data, context);
            FAIL() << "expected INCORRECT_DATA for header " << header->dumpStructure();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.displayText();
        }
    }
}
