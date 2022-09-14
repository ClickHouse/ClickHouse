#include <gtest/gtest.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include "Processors/Executors/PullingPipelineExecutor.h"

#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include "Common/Exception.h"

using namespace DB;

static SharedContextHolder shared_context;
static ContextMutablePtr context;

static bool initialize()
{
    try
    {
        shared_context = Context::createShared();
        context = Context::createGlobal(shared_context.get());
        context->makeGlobalContext();
        context->setApplicationType(Context::ApplicationType::LOCAL);

        // registerFunctions();
        // registerAggregateFunctions();
        // registerTableFunctions();
        // registerStorages();
        // registerDictionaries();
        // registerDisks();
        // registerFormats();

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

[[ maybe_unused ]] static bool initialized = initialize();

TEST(AsyncInsertQueue, SimpleTest)
{
    try
    {
        auto io = executeQuery("CREATE TABLE SimpleTest ENGINE=Memory()", context, true, QueryProcessingStage::Complete);
        PullingPipelineExecutor executor(io.pipeline);
        Block res;
        while (!res && executor.pull(res));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

}
