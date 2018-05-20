#include <iostream>
#include <thread>
#include <atomic>
#include <Processors/Processor.h>
#include <Columns/ColumnsNumber.h>
#include <common/ThreadPool.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


using namespace DB;


class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource()
        : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))
    {
    }

private:
    UInt64 current_number = 0;

    Block generate() override
    {
        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, current_number));
        ++current_number;
        return getPort().getHeader().cloneWithColumns(std::move(columns));
    }
};


class SleepyNumbersSource : public IProcessor
{
protected:
    OutputPort & output;

public:
    String getName() const override { return "SleepyNumbers"; }

    SleepyNumbersSource()
        : IProcessor({}, {std::move(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))}), output(outputs.front())
    {
    }

    Status prepare() override
    {
        if (output.hasData())
            return Status::PortFull;

        if (!output.isNeeded())
            return Status::Unneeded;

        if (active)
            return Status::Wait;

        if (!current_block)
            return Status::Async;

        return Status::Ready;
    }

    void work() override
    {
        output.push(std::move(current_block));
    }

    void schedule(EventCounter & watch) override
    {
        active = true;
        pool.schedule([&watch, this]
        {
            usleep(100000);
            current_block = generate();
            active = false;
            watch.notify();
        });
    }

    OutputPort & getPort() { return output; }

private:
    ThreadPool pool{1};
    Block current_block;
    std::atomic_bool active {false};

    UInt64 current_number = 0;

    Block generate()
    {
        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, current_number));
        ++current_number;
        return getPort().getHeader().cloneWithColumns(std::move(columns));
    }
};


class PrintSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    PrintSink()
        : ISink(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))
    {
    }

private:
    WriteBufferFromFileDescriptor out{STDOUT_FILENO};

    void consume(Block && block) override
    {
        size_t rows = block.rows();
        size_t columns = block.columns();

        for (size_t row_num = 0; row_num < rows; ++row_num)
        {
            for (size_t column_num = 0; column_num < columns; ++column_num)
            {
                if (column_num != 0)
                    writeChar('\t', out);
                getPort().getHeader().getByPosition(column_num).type->serializeText(*block.getByPosition(column_num).column, row_num, out);
            }
            writeChar('\n', out);
        }

        out.next();
    }
};


int main(int, char **)
try
{
    auto source = std::make_shared<SleepyNumbersSource>();
    auto sink = std::make_shared<PrintSink>();
    auto limit = std::make_shared<LimitTransform>(source->getPort().getHeader(), 100, 0);

    connect(source->getPort(), limit->getInputPort());
    connect(limit->getOutputPort(), sink->getPort());

    SequentialPipelineExecutor executor({source, limit, sink});

    EventCounter watch;
    while (true)
    {
        IProcessor::Status status = executor.prepare();

        if (status == IProcessor::Status::Finished)
            break;
        else if (status == IProcessor::Status::Ready)
            executor.work();
        else if (status == IProcessor::Status::Async)
            executor.schedule(watch);
        else if (status == IProcessor::Status::Wait)
            watch.wait();
        else
            throw Exception("Bad status");
    }

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
