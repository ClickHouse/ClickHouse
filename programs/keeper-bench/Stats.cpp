#include <Stats.h>
#include <iostream>

#include <rapidjson/document.h>
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

void Stats::merge(Stats & from)
{
    std::scoped_lock lock(mutex, from.mutex);

    errors += from.errors;
    watches_fired += from.watches_fired;
    read_collector.merge(from.read_collector);
    write_collector.merge(from.write_collector);
}

void Stats::extractInto(Stats & target)
{
    std::lock_guard lock(mutex);
    target.read_collector.merge(read_collector);
    target.write_collector.merge(write_collector);
    target.errors += errors.exchange(0);
    target.watches_fired += watches_fired.exchange(0);

    read_collector.clear();
    write_collector.clear();
}

void Stats::StatsCollector::clear()
{
    requests = 0;
    requests_bytes = 0;
    sampler.clear();
}

void Stats::StatsCollector::merge(const StatsCollector & from)
{
    requests += from.requests;
    requests_bytes += from.requests_bytes;
    sampler.merge(from.sampler);
}

void Stats::clear()
{
    std::lock_guard lock(mutex);
    read_collector.clear();
    write_collector.clear();
    errors = 0;
    watches_fired = 0;
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

void Stats::report(const Stats & cumulative)
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

    std::cerr << "Total: read " << cumulative.read_collector.requests
              << ", write " << cumulative.write_collector.requests;
    if (cumulative.errors)
        std::cerr << ", errors " << cumulative.errors;
    if (cumulative.watches_fired)
        std::cerr << ", watches fired " << cumulative.watches_fired;
    std::cerr << "\n";

    std::cerr << "Last " << seconds << "s:";
    if (0 != read_requests)
        std::cerr << " Read " << read_rps << " RPS, " << read_bps / 1048576 << " MiB/s.";
    if (0 != write_requests)
        std::cerr << " Write " << write_rps << " RPS, " << write_bps / 1048576 << " MiB/s.";
    std::cerr << "\n";

    auto print_percentile = [&](double percent, Stats::StatsCollector & collector)
    {
        std::cerr << percent << "%\t\t";
        std::cerr << collector.getPercentile(percent) << " ms.\t";
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
        std::cerr << "Read latency:\n";
        print_all_percentiles(read_collector);
    }

    if (0 != write_requests)
    {
        std::cerr << "Write latency:\n";
        print_all_percentiles(write_collector);
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

    if (watches_fired)
        results.AddMember("watches_fired", Value(static_cast<uint64_t>(watches_fired.load())), allocator);

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

    StringBuffer strbuf;
    strbuf.Clear();
    Writer<StringBuffer> writer(strbuf);
    results.Accept(writer);

    const char * output_string = strbuf.GetString();
    out.write(output_string, strlen(output_string));
}
