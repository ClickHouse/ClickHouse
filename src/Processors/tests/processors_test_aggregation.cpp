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
#include <Processors/printPipeline.h>

#include <Columns/ColumnsNumber.h>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Disks/StoragePolicy.h>
#include <Disks/DiskLocal.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Common/CurrentThread.h>
#include <Poco/Path.h>


using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

class PrintSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    PrintSink(String prefix_, Block header)
            : ISink(std::move(header)),
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

class CheckSink : public ISink
{
public:
    String getName() const override { return "Check"; }

    CheckSink(Block header, size_t num_rows)
            : ISink(std::move(header)), read_rows(num_rows, false)
    {
    }

    void checkAllRead()
    {
        for (size_t i = 0; i < read_rows.size(); ++i)
        {
           if (!read_rows[i])
           {
               throw Exception("Check Failed. Row " + toString(i) + " was not read.", ErrorCodes::LOGICAL_ERROR);
           }
        }
    }

private:
    std::vector<bool> read_rows;

    void consume(Chunk chunk) override
    {
        size_t rows = chunk.getNumRows();
        size_t columns = chunk.getNumColumns();

        for (size_t row_num = 0; row_num < rows; ++row_num)
        {
            std::vector<UInt64> values(columns);
            for (size_t column_num = 0; column_num < columns; ++column_num)
            {
                values[column_num] = chunk.getColumns()[column_num]->getUInt(row_num);
            }

            if (values.size() >= 2 && 3 * values[0] != values[1])
                throw Exception("Check Failed. Got (" + toString(values[0]) + ", " + toString(values[1]) + ") in result,"
                               + "but "  + toString(values[0]) + " * 3 !=  " + toString(values[1]),
                               ErrorCodes::LOGICAL_ERROR);

            if (values[0] >= read_rows.size())
                throw Exception("Check Failed. Got string with number " + toString(values[0]) +
                                " (max " + toString(read_rows.size()), ErrorCodes::LOGICAL_ERROR);

            if (read_rows[values[0]])
                throw Exception("Row " + toString(values[0]) + " was already read.", ErrorCodes::LOGICAL_ERROR);

            read_rows[values[0]] = true;
        }
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
    ThreadStatus thread_status;
    CurrentThread::initializeQuery();
    auto thread_group = CurrentThread::getGroup();

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Logger::root().setChannel(channel);
    Logger::root().setLevel("trace");

    registerAggregateFunctions();
    auto & factory = AggregateFunctionFactory::instance();

    auto cur_path = Poco::Path().absolute().toString();
    auto disk = std::make_shared<DiskLocal>("tmp", cur_path, 0);
    auto tmp_volume = std::make_shared<VolumeJBOD>("tmp", std::vector<DiskPtr>{disk}, 0);

    auto execute_one_stream = [&](String msg, size_t num_threads, bool two_level, bool external)
    {
        std::cerr << '\n' << msg << "\n";

        size_t num_rows = 1000000;
        size_t block_size = 1000;

        auto source1 = std::make_shared<NumbersSource>(0, 1, block_size, 0);
        auto source2 = std::make_shared<NumbersSource>(0, 1, block_size, 0);
        auto source3 = std::make_shared<NumbersSource>(0, 1, block_size, 0);

        auto limit1 = std::make_shared<LimitTransform>(source1->getPort().getHeader(), num_rows, 0);
        auto limit2 = std::make_shared<LimitTransform>(source2->getPort().getHeader(), num_rows, 0);
        auto limit3 = std::make_shared<LimitTransform>(source3->getPort().getHeader(), num_rows, 0);

        auto resize = std::make_shared<ResizeProcessor>(source1->getPort().getHeader(), 3, 1);

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes sum_types = { std::make_shared<DataTypeUInt64>() };
        aggregate_descriptions[0].function = factory.get("sum", sum_types);
        aggregate_descriptions[0].arguments = {0};

        bool overflow_row = false; /// Without overflow row.
        size_t max_rows_to_group_by = 0; /// All.
        size_t group_by_two_level_threshold = two_level ? 10 : 0;
        size_t group_by_two_level_threshold_bytes = two_level ? 128 : 0;
        size_t max_bytes_before_external_group_by = external ? 10000000 : 0;

        Aggregator::Params params(
                source1->getPort().getHeader(),
                {0},
                aggregate_descriptions,
                overflow_row,
                max_rows_to_group_by,
                OverflowMode::THROW,
                group_by_two_level_threshold,
                group_by_two_level_threshold_bytes,
                max_bytes_before_external_group_by,
                false, /// empty_result_for_aggregation_by_empty_set
                tmp_volume,
                1, /// max_threads
                0
            );

        auto agg_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ false);
        auto merge_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ true);
        auto aggregating = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params);
        auto merging = std::make_shared<MergingAggregatedTransform>(aggregating->getOutputs().front().getHeader(), merge_params, 4);
        auto sink = std::make_shared<CheckSink>(merging->getOutputPort().getHeader(), num_rows);

        connect(source1->getPort(), limit1->getInputPort());
        connect(source2->getPort(), limit2->getInputPort());
        connect(source3->getPort(), limit3->getInputPort());

        auto it = resize->getInputs().begin();
        connect(limit1->getOutputPort(), *(it++));
        connect(limit2->getOutputPort(), *(it++));
        connect(limit3->getOutputPort(), *(it++));

        connect(resize->getOutputs().front(), aggregating->getInputs().front());
        connect(aggregating->getOutputs().front(), merging->getInputPort());
        connect(merging->getOutputPort(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source1, source2, source3,
                                                limit1, limit2, limit3,
                                                resize, aggregating, merging, sink};
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors);
        executor.execute(num_threads);
        sink->checkAllRead();
    };

    auto execute_mult_streams = [&](String msg, size_t num_threads, bool two_level, bool external)
    {
        std::cerr << '\n' << msg << "\n";

        size_t num_rows = 1000000;
        size_t block_size = 1000;

        auto source1 = std::make_shared<NumbersSource>(0, 1, block_size, 0);
        auto source2 = std::make_shared<NumbersSource>(0, 1, block_size, 0);
        auto source3 = std::make_shared<NumbersSource>(0, 1, block_size, 0);

        auto limit1 = std::make_shared<LimitTransform>(source1->getPort().getHeader(), num_rows, 0);
        auto limit2 = std::make_shared<LimitTransform>(source2->getPort().getHeader(), num_rows, 0);
        auto limit3 = std::make_shared<LimitTransform>(source3->getPort().getHeader(), num_rows, 0);

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes sum_types = { std::make_shared<DataTypeUInt64>() };
        aggregate_descriptions[0].function = factory.get("sum", sum_types);
        aggregate_descriptions[0].arguments = {0};

        bool overflow_row = false; /// Without overflow row.
        size_t max_rows_to_group_by = 0; /// All.
        size_t group_by_two_level_threshold = two_level ? 10 : 0;
        size_t group_by_two_level_threshold_bytes = two_level ? 128 : 0;
        size_t max_bytes_before_external_group_by = external ? 10000000 : 0;

        Aggregator::Params params(
                source1->getPort().getHeader(),
                {0},
                aggregate_descriptions,
                overflow_row,
                max_rows_to_group_by,
                OverflowMode::THROW,
                group_by_two_level_threshold,
                group_by_two_level_threshold_bytes,
                max_bytes_before_external_group_by,
                false, /// empty_result_for_aggregation_by_empty_set
                tmp_volume,
                1, /// max_threads
                0
        );

        auto agg_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ false);
        auto merge_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ true);

        ManyAggregatedDataPtr data = std::make_unique<ManyAggregatedData>(3);

        auto aggregating1 = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params, data, 0, 4, 4);
        auto aggregating2 = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params, data, 1, 4, 4);
        auto aggregating3 = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params, data, 2, 4, 4);

        Processors merging_pipe = createMergingAggregatedMemoryEfficientPipe(
                aggregating1->getOutputs().front().getHeader(),
                merge_params,
                3, 2);

        auto sink = std::make_shared<CheckSink>(merging_pipe.back()->getOutputs().back().getHeader(), num_rows);

        connect(source1->getPort(), limit1->getInputPort());
        connect(source2->getPort(), limit2->getInputPort());
        connect(source3->getPort(), limit3->getInputPort());

        connect(limit1->getOutputPort(), aggregating1->getInputs().front());
        connect(limit2->getOutputPort(), aggregating2->getInputs().front());
        connect(limit3->getOutputPort(), aggregating3->getInputs().front());

        auto it = merging_pipe.front()->getInputs().begin();
        connect(aggregating1->getOutputs().front(), *(it++));
        connect(aggregating2->getOutputs().front(), *(it++));
        connect(aggregating3->getOutputs().front(), *(it++));

        connect(merging_pipe.back()->getOutputs().back(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source1, source2, source3,
                                                limit1, limit2, limit3,
                                                aggregating1, aggregating2, aggregating3, sink};

        processors.insert(processors.end(), merging_pipe.begin(), merging_pipe.end());
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors);
        executor.execute(num_threads);
        sink->checkAllRead();
    };

    std::vector<String> messages;
    std::vector<Int64> times;

    auto exec = [&](auto func, String msg, size_t num_threads, bool two_level, bool external)
    {
        msg += ", two_level = " + toString(two_level) + ", external = " + toString(external);
        Int64 time = 0;

        auto wrapper = [&]()
        {
            ThreadStatus cur_status;

            CurrentThread::attachToIfDetached(thread_group);
            time = Measure<>::execution(func, msg, num_threads, two_level, external);
        };

        std::thread thread(wrapper);
        thread.join();

        messages.emplace_back(msg);
        times.emplace_back(time);
    };

    size_t num_threads = 4;

    exec(execute_one_stream, "One stream, single thread", 1, false, false);
    exec(execute_one_stream, "One stream, multiple threads", num_threads, false, false);

    exec(execute_mult_streams, "Multiple streams, single thread", 1, false, false);
    exec(execute_mult_streams, "Multiple streams, multiple threads", num_threads, false, false);

    exec(execute_one_stream, "One stream, single thread", 1, true, false);
    exec(execute_one_stream, "One stream, multiple threads", num_threads, true, false);

    exec(execute_mult_streams, "Multiple streams, single thread", 1, true, false);
    exec(execute_mult_streams, "Multiple streams, multiple threads", num_threads, true, false);

    exec(execute_one_stream, "One stream, single thread", 1, true, true);
    exec(execute_one_stream, "One stream, multiple threads", num_threads, true, true);

    exec(execute_mult_streams, "Multiple streams, single thread", 1, true, true);
    exec(execute_mult_streams, "Multiple streams, multiple threads", num_threads, true, true);

    for (size_t i = 0; i < messages.size(); ++i)
        std::cout << messages[i] << " time: " << times[i] << " ms.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
