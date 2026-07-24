#include <Stats.h>
#include <iostream>

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

void Stats::StatsCollector::add(uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    requests += requests_inc;
    requests_bytes += bytes_inc;
    sampler.insert(static_cast<double>(microseconds));
}

void Stats::addRead(uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    std::lock_guard lock(mutex);
    read_collector.add(microseconds, requests_inc, bytes_inc);
}

void Stats::addWrite(uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    std::lock_guard lock(mutex);
    write_collector.add(microseconds, requests_inc, bytes_inc);
}

void Stats::addOp(Coordination::OpNum op_num, uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    std::lock_guard lock(mutex);
    op_collectors[op_num].add(microseconds, requests_inc, bytes_inc);
}

void Stats::StatsCollector::clear()
{
    requests = 0;
    requests_bytes = 0;
    sampler.clear();
}

void Stats::clear()
{
    read_collector.clear();
    write_collector.clear();
    op_collectors.clear();
    errors = 0;
    elapsed.restart();
}

std::pair<double, double> Stats::StatsCollector::getThroughput(double elapsed_seconds) const
{
    assert(requests != 0);
    return {static_cast<double>(requests) / elapsed_seconds, static_cast<double>(requests_bytes) / elapsed_seconds};
}

double Stats::StatsCollector::getPercentile(double percent)
{
    return sampler.quantileNearest(percent / 100.0) / 1000.0;
}

void Stats::report()
{
    std::lock_guard lock(mutex);
    std::cerr << "\n";

    const auto & read_requests = read_collector.requests;
    const auto & write_requests = write_collector.requests;

    /// Avoid zeros, nans or exceptions
    if (0 == read_requests && 0 == write_requests)
        return;

    double seconds = elapsed.elapsedSeconds();
    if (seconds == 0)
        return;

    double read_rps = 0;
    double read_bps = 0;
    double write_rps = 0;
    double write_bps = 0;
    if (read_requests != 0)
        std::tie(read_rps, read_bps) = read_collector.getThroughput(seconds);
    if (write_requests != 0)
        std::tie(write_rps, write_bps) = write_collector.getThroughput(seconds);

    std::cerr << "read requests " << read_requests << ", write requests " << write_requests << ", ";
    if (errors)
        std::cerr << "errors " << errors << ", ";

    if (0 != read_requests)
    {
        std::cerr
            << "Read RPS: " << read_rps << ", "
            << "Read MiB/s: " << read_bps / 1048576;

        if (0 != write_requests)
            std::cerr << ", ";
    }

    if (0 != write_requests)
    {
        std::cerr
            << "Write RPS: " << write_rps << ", "
            << "Write MiB/s: " << write_bps / 1048576 << ". "
            << "\n";
    }
    std::cerr << "\n";

    auto print_percentile = [&](double percent, Stats::StatsCollector & collector)
    {
        std::cerr << percent << "%\t\t";
        std::cerr << collector.getPercentile(percent) << " msec.\t";
        std::cerr << "\n";
    };

    const auto print_all_percentiles = [&](auto & collector)
    {
        for (int percent = 0; percent <= 90; percent += 10)
            print_percentile(percent, collector);

        print_percentile(95, collector);
        print_percentile(99, collector);
        print_percentile(99.9, collector);
        print_percentile(99.99, collector);
    };

    if (0 != read_requests)
    {
        std::cerr << "Read sampler:\n";
        print_all_percentiles(read_collector);
    }

    if (0 != write_requests)
    {
        std::cerr << "Write sampler:\n";
        print_all_percentiles(write_collector);
    }

    /// Per-operation-type breakdown
    if (!op_collectors.empty())
    {
        std::cerr << "\nPer-operation breakdown:\n";
        for (auto & [op_num, collector] : op_collectors)
        {
            if (collector.requests == 0)
                continue;

            auto op_name = Coordination::opNumToString(op_num);
            auto [rps, bps] = collector.getThroughput(seconds);
            std::cerr << "  " << op_name << ": " << collector.requests << " requests, "
                      << rps << " RPS, p50 " << collector.getPercentile(50) << " ms, "
                      << "p99 " << collector.getPercentile(99) << " ms\n";
        }
    }
}

void Stats::writeJSON(DB::WriteBuffer & out, int64_t start_timestamp)
{
    std::lock_guard lock(mutex);
    using namespace rapidjson;
    Document results;
    auto & allocator = results.GetAllocator();
    results.SetObject();

    double seconds = elapsed.elapsedSeconds();
    if (seconds == 0)
        return;

    results.AddMember("timestamp", Value(start_timestamp), allocator);
    results.AddMember("errors", Value(static_cast<uint64_t>(errors.load())), allocator);
    results.AddMember("ops", Value(static_cast<uint64_t>(read_collector.requests + write_collector.requests)), allocator);

    const auto get_results = [&](auto & collector)
    {
        Value specific_results(kObjectType);

        specific_results.AddMember("total_requests", Value(static_cast<uint64_t>(collector.requests)), allocator);

        auto [rps, bps] = collector.getThroughput(seconds);
        specific_results.AddMember("requests_per_second", Value(rps), allocator);
        specific_results.AddMember("bytes_per_second", Value(bps), allocator);

        Value percentiles(kArrayType);

        const auto add_percentile = [&](double percent)
        {
            Value percentile(kObjectType);
            Value percent_key(fmt::format("{:.2f}", percent).c_str(), allocator);
            percentile.AddMember(percent_key, Value(collector.getPercentile(percent)), allocator);
            percentiles.PushBack(percentile, allocator);
        };

        for (int percent = 0; percent <= 90; percent += 10)
            add_percentile(percent);

        add_percentile(95);
        add_percentile(99);
        add_percentile(99.9);
        add_percentile(99.99);

        specific_results.AddMember("percentiles", percentiles, allocator);

        return specific_results;
    };

    if (read_collector.requests != 0)
        results.AddMember("read_results", get_results(read_collector), results.GetAllocator());

    if (write_collector.requests != 0)
        results.AddMember("write_results", get_results(write_collector), results.GetAllocator());

    /// Per-operation-type breakdown
    if (!op_collectors.empty())
    {
        Value op_results(kObjectType);
        for (auto & [op_num, collector] : op_collectors)
        {
            if (collector.requests == 0)
                continue;

            auto op_name = Coordination::opNumToString(op_num);
            Value key(std::string(op_name).c_str(), allocator);
            op_results.AddMember(key, get_results(collector), allocator);
        }
        results.AddMember("per_op_results", op_results, allocator);
    }

    StringBuffer strbuf;
    strbuf.Clear();
    Writer<StringBuffer> writer(strbuf);
    results.Accept(writer);

    const char * output_string = strbuf.GetString();
    out.write(output_string, strlen(output_string));
}
