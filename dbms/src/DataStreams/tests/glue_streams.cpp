#include <iostream>
#include <iomanip>

#include <common/ThreadPool.h>

#include <IO/WriteBufferFromFileDescriptor.h>

#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/executeQuery.h>

#include <DataStreams/glueBlockInputStreams.h>


using namespace DB;


void inputThread(BlockInputStreamPtr in, BlockOutputStreamPtr out, WriteBuffer & wb, std::mutex & mutex)
{
    while (Block block = in->read())
    {
        std::lock_guard<std::mutex> lock(mutex);

        out->write(block);
        wb.next();
    }
}

void forkThread(ForkPtr fork)
{
    fork->run();
}


int main(int argc, char ** argv)
try
{
    Context context = Context::createGlobal();

    context.setGlobalContext(context);
    context.setPath("./");

    loadMetadata(context);

    context.setCurrentDatabase("default");
    context.setSetting("max_threads", 1UL);

    BlockIO io1 = executeQuery(
        "SELECT SearchPhrase, count()"
            " FROM hits"
            " WHERE SearchPhrase != ''"
            " GROUP BY SearchPhrase"
            " ORDER BY count() DESC"
            " LIMIT 10",
        context, QueryProcessingStage::Complete);

    BlockIO io2 = executeQuery(
        "SELECT count()"
            " FROM hits"
            " WHERE SearchPhrase != ''",
        context, QueryProcessingStage::Complete);

    WriteBufferFromFileDescriptor wb(STDOUT_FILENO);

    BlockOutputStreamPtr out1 = context.getOutputFormat("TabSeparated", wb, io1.in_sample);
    BlockOutputStreamPtr out2 = context.getOutputFormat("TabSeparated", wb, io2.in_sample);

    BlockInputStreams inputs;
    inputs.push_back(io1.in);
    inputs.push_back(io2.in);

    for (size_t i = 0; i < inputs.size(); ++i)
        std::cerr << inputs[i]->getID() << std::endl;

    Forks forks;

    glueBlockInputStreams(inputs, forks);

    std::cerr << forks.size() << std::endl;

    std::mutex mutex;

    ThreadPool pool(inputs.size() + forks.size());

    pool.schedule(std::bind(inputThread, inputs[0], out1, std::ref(wb), std::ref(mutex)));
    pool.schedule(std::bind(inputThread, inputs[1], out2, std::ref(wb), std::ref(mutex)));

    for (size_t i = 0; i < forks.size(); ++i)
        pool.schedule(std::bind(forkThread, forks[i]));

    pool.wait();
    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}
