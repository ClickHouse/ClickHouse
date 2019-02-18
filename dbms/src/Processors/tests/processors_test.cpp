#include <iostream>
#include <thread>
#include <atomic>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/ISink.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/ForkProcessor.h>
#include <Processors/LimitTransform.h>
#include <Processors/QueueBuffer.h>
#include <Processors/Executors/SequentialPipelineExecutor.h>
#include <Processors/Executors/ParallelPipelineExecutor.h>
#include <Processors/printPipeline.h>

#include <Columns/ColumnsNumber.h>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Processors/Executors/PipelineExecutor.h>


using namespace DB;


class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource(UInt64 start_number, unsigned sleep_useconds)
        : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
        current_number(start_number), sleep_useconds(sleep_useconds)
    {
    }

private:
    UInt64 current_number = 0;
    unsigned sleep_useconds;

    Chunk generate() override
    {
        usleep(sleep_useconds);

        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, current_number));
        ++current_number;
        return Chunk(std::move(columns), 1);
    }
};


class SleepyNumbersSource : public IProcessor
{
protected:
    OutputPort & output;

public:
    String getName() const override { return "SleepyNumbers"; }

    SleepyNumbersSource(UInt64 start_number, unsigned sleep_useconds)
        : IProcessor({}, {Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})})
        , output(outputs.front()), current_number(start_number), sleep_useconds(sleep_useconds)
    {
    }

    Status prepare() override
    {
        if (active)
            return Status::Wait;

        if (output.isFinished())
            return Status::Finished;

        if (!output.canPush())
            return Status::PortFull;

        if (!current_chunk)
            return Status::Async;

        output.push(std::move(current_chunk));
        return Status::Async;
    }

    void schedule(EventCounter & watch) override
    {
        active = true;
        pool.schedule([&watch, this]
        {
            usleep(sleep_useconds);
            current_chunk = generate();
            active = false;
            watch.notify();
        });
    }

    OutputPort & getPort() { return output; }

private:
    ThreadPool pool{1, 1, 0};
    Chunk current_chunk;
    std::atomic_bool active {false};

    UInt64 current_number = 0;
    unsigned sleep_useconds;

    Chunk generate()
    {
        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, current_number));
        ++current_number;
        return Chunk(std::move(columns), 1);
    }
};


class PrintSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    PrintSink(String prefix)
        : ISink(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
        prefix(std::move(prefix))
    {
    }

private:
    String prefix;
    WriteBufferFromFileDescriptor out{STDOUT_FILENO};
    FormatSettings settings;

    void consume(Chunk chunk) override
    {
        size_t rows = chunk.getNumRows();
        size_t columns = chunk.getNumColumns();

        for (size_t row_num = 0; row_num < rows; ++row_num)
        {
            writeString(prefix, out);
            for (size_t column_num = 0; column_num < columns; ++column_num)
            {
                if (column_num != 0)
                    writeChar('\t', out);
                getPort().getHeader().getByPosition(column_num).type->serializeAsText(*chunk.getColumns()[column_num], row_num, out, settings);
            }
            writeChar('\n', out);
        }

        out.next();
    }
};


int main(int, char **)
try
{
    auto source0 = std::make_shared<NumbersSource>(0, 300000);
    auto header = source0->getPort().getHeader();
    auto limit0 = std::make_shared<LimitTransform>(header, 10, 0);

    connect(source0->getPort(), limit0->getInputPort());

    auto queue = std::make_shared<QueueBuffer>(header);

    connect(limit0->getOutputPort(), queue->getInputPort());

    auto source1 = std::make_shared<SleepyNumbersSource>(100, 100000);
    auto source2 = std::make_shared<SleepyNumbersSource>(1000, 200000);

    auto source3 = std::make_shared<NumbersSource>(10, 100000);
    auto limit3 = std::make_shared<LimitTransform>(header, 5, 0);

    connect(source3->getPort(), limit3->getInputPort());

    auto source4 = std::make_shared<NumbersSource>(10, 100000);
    auto limit4 = std::make_shared<LimitTransform>(header, 5, 0);

    connect(source4->getPort(), limit4->getInputPort());

    auto concat = std::make_shared<ConcatProcessor>(header, 2);

    connect(limit3->getOutputPort(), concat->getInputs()[0]);
    connect(limit4->getOutputPort(), concat->getInputs()[1]);

    auto fork = std::make_shared<ForkProcessor>(header, 2);

    connect(concat->getOutputPort(), fork->getInputPort());

    auto print_after_concat = std::make_shared<PrintSink>("---------- ");

    connect(fork->getOutputs()[1], print_after_concat->getPort());

    auto resize = std::make_shared<ResizeProcessor>(header, 4, 1);

    connect(queue->getOutputPort(), resize->getInputs()[0]);
    connect(source1->getPort(), resize->getInputs()[1]);
    connect(source2->getPort(), resize->getInputs()[2]);
    connect(fork->getOutputs()[0], resize->getInputs()[3]);

    auto limit = std::make_shared<LimitTransform>(header, 100, 0);

    connect(resize->getOutputs()[0], limit->getInputPort());

    auto sink = std::make_shared<PrintSink>("");

    connect(limit->getOutputPort(), sink->getPort());

    WriteBufferFromOStream out(std::cout);
    std::vector<ProcessorPtr> processors = {source0, source1, source2, source3, source4, limit0, limit3, limit4, limit,
                                            queue, concat, fork, print_after_concat, resize, sink};
    printPipeline(processors, out);

    // ThreadPool pool(4, 4, 10);
    PipelineExecutor executor(processors);
    /// SequentialPipelineExecutor executor({sink});

    executor.execute();

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
