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


class MergingSortedProcessor : public IProcessor
{
public:
    MergingSortedProcessor(const Block & header, size_t num_inputs)
        : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
        , chunks(num_inputs), positions(num_inputs, 0), finished(num_inputs, false)
    {
    }

    String getName() const override { return "MergingSortedProcessor"; }

    Status prepare() override
    {
        auto & output = outputs[0];

        /// Check can output.

        if (output.isFinished())
        {
            for (auto & in : inputs)
                in.close();

            return Status::Finished;
        }

        if (!output.isNeeded())
        {
            for (auto & in : inputs)
                in.setNotNeeded();

            return Status::PortFull;
        }

        if (output.hasData())
            return Status::PortFull;

        /// Push if has data.
        if (res)
        {
            output.push(std::move(res));
            return Status::PortFull;
        }

        /// Check for inputs we need.
        bool all_inputs_finished = true;
        bool all_inputs_has_data = true;
        for (size_t i = 0; i < inputs.size(); ++i)
        {
            if (!finished[i])
            {
                if (!inputs[i].isFinished())
                {
                    all_inputs_finished = false;
                    bool needed = positions[i] >= chunks[i].getNumRows();
                    if (needed)
                    {
                        inputs[i].setNeeded();
                        if (inputs[i].hasData())
                        {
                            chunks[i] = inputs[i].pull();
                            positions[i] = 0;
                        }
                        else
                            all_inputs_has_data = false;
                    }
                    else
                        inputs[i].setNotNeeded();
                }
                else
                    finished[i] = true;
            }
        }

        if (all_inputs_finished)
        {
            output.finish();
            return Status::Finished;
        }

        if (!all_inputs_has_data)
            return Status::NeedData;

        return Status::Ready;
    }

    void work() override
    {
        using Key = std::pair<UInt64, size_t>;
        std::priority_queue<Key, std::vector<Key>, std::greater<>> queue;
        for (size_t i = 0; i < chunks.size(); ++i)
        {
            if (finished[i])
                continue;

            if (positions[i] >= chunks[i].getNumRows())
                return;

            queue.push({chunks[i].getColumns()[0]->getUInt(positions[i]), i});
        }

        auto col = ColumnUInt64::create();

        while (!queue.empty())
        {
            size_t ps = queue.top().second;
            queue.pop();

            auto & cur_col = chunks[ps].getColumns()[0];
            col->insertFrom(*cur_col, positions[ps]);
            ++positions[ps];

            if (positions[ps] == cur_col->size())
                break;

            queue.push({cur_col->getUInt(positions[ps]), ps});
        }

        UInt64 num_rows = col->size();
        res.setColumns(Columns({std::move(col)}), num_rows);
    }

    OutputPort & getOutputPort() { return outputs[0]; }

private:
    Chunks chunks;
    Chunk res;
    std::vector<size_t> positions;
    std::vector<bool> finished;
};


class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource(UInt64 start_number, UInt64 step, unsigned sleep_useconds)
            : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
              current_number(start_number), step(step), sleep_useconds(sleep_useconds)
    {
    }

private:
    UInt64 current_number = 0;
    UInt64 step;
    unsigned sleep_useconds;

    Chunk generate() override
    {
        usleep(sleep_useconds);

        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, current_number));
        current_number += step;
        return Chunk(std::move(columns), 1);
    }
};


class SleepyTransform : public ISimpleTransform
{
public:
    explicit SleepyTransform(unsigned sleep_useconds)
            : ISimpleTransform(
            Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}),
            Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))
            , sleep_useconds(sleep_useconds) {}

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

template<typename TimeT = std::chrono::milliseconds>
struct measure
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
    auto execute_chain = [](String msg, size_t start1, size_t start2, size_t start3, ThreadPool * pool)
    {
        std::cerr << msg << "\n";

        auto source1 = std::make_shared<NumbersSource>(start1, 3, 100000);
        auto source2 = std::make_shared<NumbersSource>(start2, 3, 100000);
        auto source3 = std::make_shared<NumbersSource>(start3, 3, 100000);

        auto transform1 = std::make_shared<SleepyTransform>(100000);
        auto transform2 = std::make_shared<SleepyTransform>(100000);
        auto transform3 = std::make_shared<SleepyTransform>(100000);

        auto limit1 = std::make_shared<LimitTransform>(source1->getPort().getHeader(), 20, 0);
        auto limit2 = std::make_shared<LimitTransform>(source2->getPort().getHeader(), 20, 0);
        auto limit3 = std::make_shared<LimitTransform>(source3->getPort().getHeader(), 20, 0);

        auto merge = std::make_shared<MergingSortedProcessor>(source1->getPort().getHeader(), 3);
        auto limit_fin = std::make_shared<LimitTransform>(source1->getPort().getHeader(), 54, 0);
        auto sink = std::make_shared<PrintSink>("");

        connect(source1->getPort(), transform1->getInputPort());
        connect(source2->getPort(), transform2->getInputPort());
        connect(source3->getPort(), transform3->getInputPort());

        connect(transform1->getOutputPort(), limit1->getInputPort());
        connect(transform2->getOutputPort(), limit2->getInputPort());
        connect(transform3->getOutputPort(), limit3->getInputPort());

        connect(limit1->getOutputPort(), merge->getInputs()[0]);
        connect(limit2->getOutputPort(), merge->getInputs()[1]);
        connect(limit3->getOutputPort(), merge->getInputs()[2]);

        connect(merge->getOutputPort(), limit_fin->getInputPort());
        connect(limit_fin->getOutputPort(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source1, source2, source3,
                                                transform1, transform2, transform3,
                                                limit1, limit2, limit3,
                                                merge, limit_fin, sink};
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors, pool);
        executor.execute();
    };

    ThreadPool pool(4, 4, 10);

    auto even_time_single = measure<>::execution(execute_chain, "Even distribution single thread", 0, 1, 2, nullptr);
    auto even_time_mt = measure<>::execution(execute_chain, "Even distribution multiple threads", 0, 1, 2, &pool);

    auto half_time_single = measure<>::execution(execute_chain, "Half distribution single thread", 0, 31, 62, nullptr);
    auto half_time_mt = measure<>::execution(execute_chain, "Half distribution multiple threads", 0, 31, 62, &pool);

    auto ordered_time_single = measure<>::execution(execute_chain, "Ordered distribution single thread", 0, 61, 122, nullptr);
    auto ordered_time_mt = measure<>::execution(execute_chain, "Ordered distribution multiple threads", 0, 61, 122, &pool);

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
