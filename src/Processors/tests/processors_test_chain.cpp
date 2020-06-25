#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/ISink.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/printPipeline.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatSettings.h>

#include <iostream>
#include <chrono>


using namespace DB;


class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource(UInt64 start_number, unsigned sleep_useconds_)
        : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
          current_number(start_number), sleep_useconds(sleep_useconds_)
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

class SleepyTransform : public ISimpleTransform
{
public:
    explicit SleepyTransform(unsigned sleep_useconds_)
        : ISimpleTransform(
                Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}),
                Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}),
                /*skip_empty_chunks =*/ false)
        , sleep_useconds(sleep_useconds_) {}

    String getName() const override { return "SleepyTransform"; }

protected:
    void transform(Chunk &) override
    {
        usleep(sleep_useconds);
    }

private:
    unsigned sleep_useconds;
};

class PrintSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    explicit PrintSink(String prefix_)
            : ISink(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
              prefix(std::move(prefix_))
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

template<typename TimeT = std::chrono::milliseconds>
struct Measure
{
    template<typename F, typename ...Args>
    static typename TimeT::rep execution(F&& func, Args&&... args)
    {
        auto start = std::chrono::steady_clock::now();
        std::forward<decltype(func)>(func)(std::forward<Args>(args)...);
        auto duration = std::chrono::duration_cast< TimeT>
                (std::chrono::steady_clock::now() - start);
        return duration.count();
    }
};

int main(int, char **)
try
{
    auto execute_chain = [](size_t num_threads)
    {
        std::cerr << "---------------------\n";

        auto source = std::make_shared<NumbersSource>(0, 100000);
        auto transform1 = std::make_shared<SleepyTransform>(100000);
        auto transform2 = std::make_shared<SleepyTransform>(100000);
        auto transform3 = std::make_shared<SleepyTransform>(100000);
        auto limit = std::make_shared<LimitTransform>(source->getPort().getHeader(), 20, 0);
        auto sink = std::make_shared<PrintSink>("");

        connect(source->getPort(), transform1->getInputPort());
        connect(transform1->getOutputPort(), transform2->getInputPort());
        connect(transform2->getOutputPort(), transform3->getInputPort());
        connect(transform3->getOutputPort(), limit->getInputPort());
        connect(limit->getOutputPort(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source, transform1, transform2, transform3, limit, sink};
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors);
        executor.execute(num_threads);
    };

    auto time_single = Measure<>::execution(execute_chain, 1);
    auto time_mt = Measure<>::execution(execute_chain, 4);

    std::cout << "Single Thread time: " << time_single << " ms.\n";
    std::cout << "Multiple Threads time: " << time_mt << " ms.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
