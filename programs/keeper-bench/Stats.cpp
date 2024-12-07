#include "Stats.h"
#include <iostream>

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

void Stats::StatsCollector::add(uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    work_time += microseconds;
    requests += requests_inc;
    requests_bytes += bytes_inc;
    sampler.insert(microseconds);
}

void Stats::addRead(uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    read_collector.add(microseconds, requests_inc, bytes_inc);
}

void Stats::addWrite(uint64_t microseconds, size_t requests_inc, size_t bytes_inc)
{
    write_collector.add(microseconds, requests_inc, bytes_inc);
}

void Stats::StatsCollector::clear()
{
    requests = 0;
    work_time = 0;
    requests_bytes = 0;
    sampler.clear();
}

void Stats::clear()
{
    read_collector.clear();
    write_collector.clear();
}

std::pair<double, double> Stats::StatsCollector::getThroughput(size_t concurrency)
{
    assert(requests != 0);
    double seconds = work_time / 1'000'000.0 / concurrency;

    return {requests / seconds, requests_bytes / seconds};
}

double Stats::StatsCollector::getPercentile(double percent)
{
    return sampler.quantileNearest(percent / 100.0) / 1000.0;
}

void Stats::report(size_t concurrency)
{
    std::cerr << "\n";

    const auto & read_requests = read_collector.requests;
    const auto & write_requests = write_collector.requests;

    /// Avoid zeros, nans or exceptions
    if (0 == read_requests && 0 == write_requests)
        return;

    auto [read_rps, read_bps] = read_collector.getThroughput(concurrency);
    auto [write_rps, write_bps] = write_collector.getThroughput(concurrency);

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
}

void Stats::writeJSON(DB::WriteBuffer & out, size_t concurrency, int64_t start_timestamp)
{
    using namespace rapidjson;
    Document results;
    auto & allocator = results.GetAllocator();
    results.SetObject();

    results.AddMember("timestamp", Value(start_timestamp), allocator);

    const auto get_results = [&](auto & collector)
    {
        Value specific_results(kObjectType);

        specific_results.AddMember("total_requests", Value(static_cast<uint64_t>(collector.requests)), allocator);

        auto [rps, bps] = collector.getThroughput(concurrency);
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

    StringBuffer strbuf;
    strbuf.Clear();
    Writer<StringBuffer> writer(strbuf);
    results.Accept(writer);

    const char * output_string = strbuf.GetString();
    out.write(output_string, strlen(output_string));
}
