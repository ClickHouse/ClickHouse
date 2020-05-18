#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <Disks/StoragePolicy.h>
#include <Disks/DiskLocal.h>

#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/ISink.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/printPipeline.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatSettings.h>

#include <iostream>
#include <chrono>
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource(UInt64 count_, UInt64 block_size_, unsigned sleep_useconds_)
            : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
            count(count_), block_size(block_size_), sleep_useconds(sleep_useconds_)
    {
    }

private:
    UInt64 current_number = 0;
    UInt64 count;
    UInt64 block_size;
    unsigned sleep_useconds;

    Chunk generate() override
    {
        if (current_number == count)
            return {};

        usleep(sleep_useconds);

        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create());

        UInt64 number = current_number++;
        for (UInt64 i = 0; i < block_size; ++i, number += count)
            columns.back()->insert(Field(number));

        return Chunk(std::move(columns), block_size);
    }
};

class CheckSortedSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    CheckSortedSink()
            : ISink(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))
    {
    }

private:
    FormatSettings settings;
    UInt64 current_number = 0;

    void consume(Chunk chunk) override
    {
        size_t rows = chunk.getNumRows();

        UInt64 prev = current_number;
        const auto & col = chunk.getColumns().at(0);
        for (size_t row_num = 0; row_num < rows; ++row_num)
        {
            UInt64 val = col->getUInt(row_num);
            if (val != current_number)
                throw Exception("Invalid value. Expected " + toString(current_number) + ", got " + toString(val),
                        ErrorCodes::LOGICAL_ERROR);

            ++current_number;
        }

        std::cout << "CheckSortedSink: " << prev << " - " << current_number << std::endl;
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

}


using namespace DB;

int main(int, char **)
try
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Logger::root().setChannel(channel);
    Logger::root().setLevel("trace");

    auto disk = std::make_shared<DiskLocal>("tmp", ".", 0);
    auto tmp_volume = std::make_shared<VolumeJBOD>("tmp", std::vector<DiskPtr>{disk}, 0);

    auto execute_chain = [tmp_volume](
        String msg,
        UInt64 source_block_size,
        UInt64 blocks_count,
        size_t max_merged_block_size,
        UInt64 limit,
        size_t max_bytes_before_remerge,
        size_t max_bytes_before_external_sort,
        size_t num_threads)
    {
        std::cerr << "------------------------\n";
        std::cerr << msg << "\n";

        auto source = std::make_shared<NumbersSource>(blocks_count, source_block_size, 100);
        SortDescription description = {{0, 1, 1}};
        auto transform = std::make_shared<MergeSortingTransform>(
                source->getPort().getHeader(), description,
                max_merged_block_size, limit,
                max_bytes_before_remerge, max_bytes_before_external_sort,
                tmp_volume, 0);
        auto sink = std::make_shared<CheckSortedSink>();

        connect(source->getPort(), transform->getInputs().front());
        connect(transform->getOutputs().front(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source, transform, sink};
        PipelineExecutor executor(processors);
        executor.execute(num_threads);

        WriteBufferFromOStream out(std::cout);
        printPipeline(executor.getProcessors(), out);
    };

    std::map<std::string, Int64> times;

    for (size_t num_threads : {1, 4})
    {
        {
            UInt64 source_block_size = 100;
            UInt64 blocks_count = 10;
            size_t max_merged_block_size = 100;
            UInt64 limit = 0;
            size_t max_bytes_before_remerge = 10000000;
            size_t max_bytes_before_external_sort = 10000000;
            std::string msg = num_threads > 1 ? "multiple threads" : "single thread";
            msg += ", " + toString(blocks_count) + " blocks per " + toString(source_block_size) + " numbers" +
                    ", no remerge and external sorts.";

            Int64 time = Measure<>::execution(execute_chain, msg,
                                        source_block_size,
                                        blocks_count,
                                        max_merged_block_size,
                                        limit,
                                        max_bytes_before_remerge,
                                        max_bytes_before_external_sort,
                                        num_threads);

            times[msg] = time;
        }

        {
            UInt64 source_block_size = 1024;
            UInt64 blocks_count = 10;
            size_t max_merged_block_size = 1024;
            UInt64 limit = 2048;
            size_t max_bytes_before_remerge = sizeof(UInt64) * source_block_size * 4;
            size_t max_bytes_before_external_sort = 10000000;
            std::string msg = num_threads > 1 ? "multiple threads" : "single thread";
            msg += ", " + toString(blocks_count) + " blocks per " + toString(source_block_size) + " numbers" +
                   ", with remerge, no external sorts.";

            Int64 time = Measure<>::execution(execute_chain, msg,
                                              source_block_size,
                                              blocks_count,
                                              max_merged_block_size,
                                              limit,
                                              max_bytes_before_remerge,
                                              max_bytes_before_external_sort,
                                              num_threads);

            times[msg] = time;
        }

        {
            UInt64 source_block_size = 1024;
            UInt64 blocks_count = 10;
            size_t max_merged_block_size = 1024;
            UInt64 limit = 0;
            size_t max_bytes_before_remerge = 0;
            size_t max_bytes_before_external_sort = sizeof(UInt64) * source_block_size * 4;
            std::string msg = num_threads > 1 ? "multiple threads" : "single thread";
            msg += ", " + toString(blocks_count) + " blocks per " + toString(source_block_size) + " numbers" +
                   ", no remerge, with external sorts.";

            Int64 time = Measure<>::execution(execute_chain, msg,
                                              source_block_size,
                                              blocks_count,
                                              max_merged_block_size,
                                              limit,
                                              max_bytes_before_remerge,
                                              max_bytes_before_external_sort,
                                              num_threads);

            times[msg] = time;
        }
    }

    for (auto & item : times)
        std::cout << item.first << ' ' << item.second << " ms.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
