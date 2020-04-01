#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/LimitTransform.h>
#include <Processors/printPipeline.h>
#include <Processors/Executors/PipelineExecutor.h>


#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatSettings.h>

#include <iostream>
#include <chrono>
#include <Processors/ISimpleTransform.h>

using namespace DB;

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


class OneNumberSource : public ISource
{
public:
    String getName() const override { return "OneNumber"; }

    explicit OneNumberSource(UInt64 number_)
            : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
              number(number_)
    {
    }

private:
    UInt64 number;
    bool done = false;

    Chunk generate() override
    {
        if (done)
            return Chunk();

        done = true;

        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, number));
        return Chunk(std::move(columns), 1);
    }
};


class ExpandingProcessor : public IProcessor
{
public:
    String getName() const override { return "Expanding"; }
    ExpandingProcessor()
    : IProcessor({Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})},
                 {Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})})
    {}

    Status prepare() override
    {
        auto & main_input = inputs.front();
        auto & main_output = outputs.front();
        auto & additional_input = inputs.back();
        auto & additional_output = outputs.back();
        /// Check can output.


        if (main_output.isFinished())
        {
            main_input.close();
            additional_input.close();
            additional_output.finish();
            return Status::Finished;
        }

        if (!main_output.canPush())
        {
            main_input.setNotNeeded();
            additional_input.setNotNeeded();
            return Status::PortFull;
        }

        if (chunk_from_add_inp && is_processed)
        {
            if (is_processed)
                main_output.push(std::move(chunk_from_add_inp));
            else
                return Status::Ready;
        }

        if (expanded)
        {
            if (chunk_from_main_inp)
            {
                if (additional_output.isFinished())
                {
                    main_input.close();
                    return Status::Finished;
                }

                if (!additional_output.canPush())
                {
                    main_input.setNotNeeded();
                    return Status::PortFull;
                }

                additional_output.push(std::move(chunk_from_main_inp));
                main_input.close();
            }

            if (additional_input.isFinished())
            {
                main_output.finish();
                return Status::Finished;
            }

            additional_input.setNeeded();

            if (!additional_input.hasData())
                return Status::NeedData;

            chunk_from_add_inp = additional_input.pull();
            is_processed = false;
            return Status::Ready;
        }
        else
        {
            if (!chunk_from_main_inp)
            {

                if (main_input.isFinished())
                {
                    main_output.finish();
                    return Status::Finished;
                }

                main_input.setNeeded();

                if (!main_input.hasData())
                    return Status::NeedData;

                chunk_from_main_inp = main_input.pull();
                main_input.close();
            }

            UInt64 val = chunk_from_main_inp.getColumns()[0]->getUInt(0);
            if (val)
            {
                --val;
                chunk_from_main_inp.setColumns(Columns{ColumnUInt64::create(1, val)}, 1);
                return Status::ExpandPipeline;
            }

            main_output.push(std::move(chunk_from_main_inp));
            main_output.finish();
            return Status::Finished;
        }
    }

    Processors expandPipeline() override
    {
        auto & main_input = inputs.front();
        auto & main_output = outputs.front();

        Processors processors = {std::make_shared<ExpandingProcessor>()};
        inputs.push_back({main_input.getHeader(), this});
        outputs.push_back({main_output.getHeader(), this});
        connect(outputs.back(), processors.back()->getInputs().front());
        connect(processors.back()->getOutputs().front(), inputs.back());
        inputs.back().setNeeded();

        expanded = true;
        return processors;
    }

    void work() override
    {
        auto num_rows = chunk_from_add_inp.getNumRows();
        auto columns = chunk_from_add_inp.mutateColumns();
        columns.front()->insert(Field(num_rows));
        chunk_from_add_inp.setColumns(std::move(columns), num_rows + 1);
        is_processed = true;
    }

private:
    bool expanded = false;
    Chunk chunk_from_main_inp;
    Chunk chunk_from_add_inp;
    bool is_processed = false;
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
    auto execute = [](String msg, size_t num, size_t num_threads)
    {
        std::cerr << msg << "\n";

        auto source = std::make_shared<OneNumberSource>(num);
        auto expanding = std::make_shared<ExpandingProcessor>();
        auto sink = std::make_shared<PrintSink>("");

        connect(source->getPort(), expanding->getInputs().front());
        connect(expanding->getOutputs().front(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source, expanding, sink};

        PipelineExecutor executor(processors);
        executor.execute(num_threads);

        WriteBufferFromOStream out(std::cout);
        printPipeline(executor.getProcessors(), out);
    };

    ThreadPool pool(4, 4, 10);

    auto time_single = Measure<>::execution(execute, "Single thread", 10, 1);
    auto time_mt = Measure<>::execution(execute, "Multiple threads", 10, 4);

    std::cout << "Single Thread time: " << time_single << " ms.\n";
    std::cout << "Multiple Threads time:" << time_mt << " ms.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
