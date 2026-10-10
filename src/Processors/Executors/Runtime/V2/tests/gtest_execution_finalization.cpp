#include <Processors/Executors/Runtime/V2/tests/Toolkit/RunPipeline.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Formats/LazyOutputFormat.h>

using namespace DB;
using namespace DB::Runtime;
using namespace DB::Runtime::V2;

WYRD_TEST(ExecutionFinalization, OutputFormatWritesItsEpilogue, .runs = 1000)
{
    auto source = makeSource("source", {emit(1), emit(2)});
    auto format = std::make_shared<LazyOutputFormat>(makeHeader());
    chain({source, format});

    EXPECT_NO_THROW(runPipeline({source, format}, 1));

    EXPECT_EQ(format->getChunk(0).getNumRows(), 1u);
    EXPECT_EQ(format->getChunk(0).getNumRows(), 1u);
    EXPECT_TRUE(format->isFinished());
}
