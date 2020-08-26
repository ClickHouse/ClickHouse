#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/ISink.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/printPipeline.h>
#include <Processors/Merges/MergingSortedTransform.h>
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

    NumbersSource(UInt64 start_number, UInt64 step_, UInt64 block_size_, unsigned sleep_useconds_)
            : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
              current_number(start_number), step(step_), block_size(block_size_), sleep_useconds(sleep_useconds_)
    {
    }

private:
    UInt64 current_number = 0;
    UInt64 step;
    UInt64 block_size;
    unsigned sleep_useconds;

    Chunk generate() override
    {
        usleep(sleep_useconds);

        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create());

        for (UInt64 i = 0; i < block_size; ++i, current_number += step)
            columns.back()->insert(Field(current_number));

        return Chunk(std::move(columns), block_size);
    }
};


class SleepyTransform : public ISimpleTransform
{
public:
    explicit SleepyTransform(unsigned sleep_useconds_)
            : ISimpleTransform(
            Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}),
            Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}),
            false)
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
    auto execute_chain = [](String msg, size_t start1, size_t start2, size_t start3, size_t num_threads)
    {
        std::cerr << msg << "\n";

        auto source1 = std::make_shared<NumbersSource>(start1, 3, 2, 100000);
        auto source2 = std::make_shared<NumbersSource>(start2, 3, 2, 100000);
        auto source3 = std::make_shared<NumbersSource>(start3, 3, 2, 100000);

        auto transform1 = std::make_shared<SleepyTransform>(100000);
        auto transform2 = std::make_shared<SleepyTransform>(100000);
        auto transform3 = std::make_shared<SleepyTransform>(100000);

        auto limit1 = std::make_shared<LimitTransform>(source1->getPort().getHeader(), 20, 0);
        auto limit2 = std::make_shared<LimitTransform>(source2->getPort().getHeader(), 20, 0);
        auto limit3 = std::make_shared<LimitTransform>(source3->getPort().getHeader(), 20, 0);

        SortDescription description = {{0, 1, 1}};
        auto merge = std::make_shared<MergingSortedTransform>(source1->getPort().getHeader(), 3, description, 2);
        auto limit_fin = std::make_shared<LimitTransform>(source1->getPort().getHeader(), 54, 0);
        auto sink = std::make_shared<PrintSink>("");

        connect(source1->getPort(), transform1->getInputPort());
        connect(source2->getPort(), transform2->getInputPort());
        connect(source3->getPort(), transform3->getInputPort());

        connect(transform1->getOutputPort(), limit1->getInputPort());
        connect(transform2->getOutputPort(), limit2->getInputPort());
        connect(transform3->getOutputPort(), limit3->getInputPort());

        auto it = merge->getInputs().begin();
        connect(limit1->getOutputPort(), *(it++));
        connect(limit2->getOutputPort(), *(it++));
        connect(limit3->getOutputPort(), *(it++));

        connect(merge->getOutputs().front(), limit_fin->getInputPort());
        connect(limit_fin->getOutputPort(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source1, source2, source3,
                                                transform1, transform2, transform3,
                                                limit1, limit2, limit3,
                                                merge, limit_fin, sink};
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors);
        executor.execute(num_threads);
    };

    auto even_time_single = Measure<>::execution(execute_chain, "Even distribution single thread", 0, 1, 2, 1);
    auto even_time_mt = Measure<>::execution(execute_chain, "Even distribution multiple threads", 0, 1, 2, 4);

    auto half_time_single = Measure<>::execution(execute_chain, "Half distribution single thread", 0, 31, 62, 1);
    auto half_time_mt = Measure<>::execution(execute_chain, "Half distribution multiple threads", 0, 31, 62, 4);

    auto ordered_time_single = Measure<>::execution(execute_chain, "Ordered distribution single thread", 0, 61, 122, 1);
    auto ordered_time_mt = Measure<>::execution(execute_chain, "Ordered distribution multiple threads", 0, 61, 122, 4);

    std::cout << "Single Thread [0:60:3] [1:60:3] [2:60:3] time: " << even_time_single << " ms.\n";
    std::cout << "Multiple Threads [0:60:3] [1:60:3] [2:60:3] time:" << even_time_mt << " ms.\n";

    std::cout << "Single Thread [0:60:3] [31:90:3] [62:120:3] time: " << half_time_single << " ms.\n";
    std::cout << "Multiple Threads [0:60:3] [31:90:3] [62:120:3] time: " << half_time_mt << " ms.\n";

    std::cout << "Single Thread [0:60:3] [61:120:3] [122:180:3] time: " << ordered_time_single << " ms.\n";
    std::cout << "Multiple Threads [0:60:3] [61:120:3] [122:180:3] time: " << ordered_time_mt << " ms.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
