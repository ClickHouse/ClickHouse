/// A small standalone monitoring agent.
///
/// It collects the same system-wide metrics as `system.asynchronous_metrics` of the server (CPU, memory, disks,
/// network, cgroups, pressure stall information, sensors, ...) using `AsynchronousMetrics`, and sends them to
/// a remote ClickHouse server over HTTP(S) with asynchronous inserts, enriched with the host name.
/// The rows have the same shape as `system.asynchronous_metric_log`.

#include <Common/AsynchronousMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/getFQDNOrHostName.h>

#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include "config.h"

#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/HTTPSClientSession.h>
#endif

#include <zstd.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int SUPPORT_IS_DISABLED;
}

}

namespace
{

using namespace DB;

constexpr const char * usage = R"(Usage: clickhouse-metrics-agent --url <URL> [options]

Collects system metrics (the same as `system.asynchronous_metrics` of ClickHouse) and sends them
to a ClickHouse server over HTTP(S) with asynchronous inserts.

Options:
    --url <URL>             HTTP(S) interface of the destination server, e.g. https://host:8443/
                            Additional query parameters (settings) in the URL are passed through.
    --user <name>           User name (default: `default`, or the CLICKHOUSE_USER environment variable).
    --password <password>   Password (default: the CLICKHOUSE_PASSWORD environment variable).
    --table <name>          Destination table (default: `system_metrics`).
    --hostname <name>       Value of the `hostname` column (default: FQDN of this host).
    --interval <seconds>    How often to collect the metrics (default: 1).
    --send-interval <sec>   How often to send the collected rows (default: 10).
    --exclude <prefix>      Do not send metrics with this name prefix; can be repeated
                            (default: `jemalloc.`, the allocator statistics of the agent itself).
    --max-pending-bytes <n> How much unsent data to keep while the server is unavailable (default: 64 MiB).
    --ca-file <path>        File with CA certificates for HTTPS (default: the system certificates).
    --log-level <level>     Logging level: `error`, `warning`, `information`, `debug`, `trace` (default: `information`).
    --print-schema          Print the `CREATE TABLE` query for the destination table and exit.
    --once                  Collect two samples, print the rows in TSV to stdout and exit (for debugging).
    --help                  Print this message.

The rows are: `event_time DateTime, hostname String, metric String, key String, value Float64`.
)";

struct Options
{
    std::string url;
    std::string user;
    std::string password;
    std::string table = "system_metrics";
    std::string hostname;
    unsigned interval = 1;
    unsigned send_interval = 10;
    std::vector<std::string> exclude{"jemalloc."};
    size_t max_pending_bytes = 64 << 20;
    std::string ca_file;
    std::string log_level = "information";
    bool print_schema = false;
    bool once = false;
};

std::string getEnv(const char * name)
{
    const char * value = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return value ? value : "";
}

Options parseOptions(int argc, char ** argv)
{
    Options options;
    options.user = getEnv("CLICKHOUSE_USER");
    if (options.user.empty())
        options.user = "default";
    options.password = getEnv("CLICKHOUSE_PASSWORD");

    bool user_excludes = false;
    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg = argv[i];
        auto value = [&]() -> std::string
        {
            if (i + 1 >= argc)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Option {} requires a value", arg);
            return argv[++i];
        };
        auto positive = [&]() -> unsigned
        {
            auto str = value();
            unsigned res = 0;
            auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), res);
            if (ec != std::errc{} || ptr != str.data() + str.size() || res == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Option {} requires a positive integer, got '{}'", arg, str);
            return res;
        };

        if (arg == "--help" || arg == "-h")
        {
            std::cout << usage;
            std::exit(0); // NOLINT(concurrency-mt-unsafe)
        }
        else if (arg == "--url")
            options.url = value();
        else if (arg == "--user")
            options.user = value();
        else if (arg == "--password")
            options.password = value();
        else if (arg == "--table")
            options.table = value();
        else if (arg == "--hostname")
            options.hostname = value();
        else if (arg == "--interval")
            options.interval = positive();
        else if (arg == "--send-interval")
            options.send_interval = positive();
        else if (arg == "--exclude")
        {
            if (!user_excludes)
                options.exclude.clear();
            user_excludes = true;
            options.exclude.push_back(value());
        }
        else if (arg == "--max-pending-bytes")
            options.max_pending_bytes = positive();
        else if (arg == "--ca-file")
            options.ca_file = value();
        else if (arg == "--log-level")
            options.log_level = value();
        else if (arg == "--print-schema")
            options.print_schema = true;
        else if (arg == "--once")
            options.once = true;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown option '{}'. Run with --help to see the usage", arg);
    }

    if (options.url.empty() && !options.print_schema && !options.once)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The --url option is required. Run with --help to see the usage");
    if (options.hostname.empty())
        options.hostname = getFQDNOrHostName();
    return options;
}

std::string getSchema(const std::string & table)
{
    return fmt::format(R"(CREATE TABLE {}
(
    event_date Date MATERIALIZED toDate(event_time),
    event_time DateTime CODEC(Delta(4), ZSTD(1)),
    hostname LowCardinality(String) CODEC(ZSTD(1)),
    metric LowCardinality(String) CODEC(ZSTD(1)),
    key LowCardinality(String) CODEC(ZSTD(1)),
    value Float64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (hostname, metric, key, event_time);
)", table);
}


/// The metrics that `AsynchronousMetrics` collects by itself: everything except the server-specific ones.
class AgentAsynchronousMetrics final : public AsynchronousMetrics
{
public:
    explicit AgentAsynchronousMetrics(unsigned update_period_seconds)
        : AsynchronousMetrics(
            update_period_seconds,
            [] { return std::vector<ProtocolServerMetrics>{}; },
            /* update_jemalloc_epoch_ = */ true,
            /* update_rss_ = */ false,
            /* collect_psi_metrics = */ true)
    {
    }

    ~AgentAsynchronousMetrics() override { stop(); }

private:
    void updateImpl(TimePoint, TimePoint, bool, bool, AsynchronousMetricValues &) override { }
};


/// Appends the values as TSV rows in the form of `system.asynchronous_metric_log`.
void formatRows(
    WriteBuffer & out, const AsynchronousMetricValues & values, time_t event_time, const Options & options)
{
    /// Round the values to make them compress better, as the server does in `system.asynchronous_metric_log`.
    static constexpr double precision = 1000.0;

    auto write_row = [&](const std::string & metric, const std::string & key, double value)
    {
        if (!std::isfinite(value))
            return;
        writeIntText(event_time, out);
        writeChar('\t', out);
        writeEscapedString(options.hostname, out);
        writeChar('\t', out);
        writeEscapedString(metric, out);
        writeChar('\t', out);
        writeEscapedString(key, out);
        writeChar('\t', out);
        writeFloatText(std::round(value * precision) / precision, out);
        writeChar('\n', out);
    };

    static const std::string empty_key;
    for (const auto & [name, value] : values)
    {
        bool excluded = false;
        for (const auto & prefix : options.exclude)
            excluded = excluded || name.starts_with(prefix);
        if (excluded)
            continue;

        if (value.isMap())
            for (const auto & [key, key_value] : value.key_values)
                write_row(name, key, key_value);
        else
            write_row(name, empty_key, value.value);
    }
}


/// Sends the collected rows in a background thread, so that a slow or unavailable server does not delay the collection.
class Sender
{
public:
    explicit Sender(const Options & options_)
        : options(options_), log(getLogger("Sender")), uri(options.url)
    {
        if (uri.getScheme() != "http" && uri.getScheme() != "https")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The URL must start with http:// or https://, got '{}'", options.url);
#if !USE_SSL
        if (uri.getScheme() == "https")
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "HTTPS is not supported: the agent is built without SSL");
#endif

        /// Settings already present in the URL take precedence.
        auto params = uri.getQueryParameters();
        auto has_param = [&](const std::string & name)
        {
            for (const auto & [key, _] : params)
                if (key == name)
                    return true;
            return false;
        };
        uri.addQueryParameter("query", fmt::format("INSERT INTO {} (event_time, hostname, metric, key, value) FORMAT TabSeparated", options.table));
        if (!has_param("async_insert"))
            uri.addQueryParameter("async_insert", "1");
        if (!has_param("wait_for_async_insert"))
            uri.addQueryParameter("wait_for_async_insert", "1");

        thread = std::thread([this] { run(); });
    }

    ~Sender()
    {
        {
            std::lock_guard lock(mutex);
            quit = true;
        }
        cv.notify_all();
        thread.join();
    }

    /// Adds the rows to send. Rows that do not fit into the limit of the pending data are dropped.
    void push(std::string && rows)
    {
        {
            std::lock_guard lock(mutex);
            if (pending.size() + rows.size() > options.max_pending_bytes)
            {
                LOG_WARNING(log, "Dropping {} bytes of metrics: {} bytes are already pending to be sent", rows.size(), pending.size());
                return;
            }
            pending += rows;
        }
        cv.notify_all();
    }

private:
    void run()
    {
        std::string data;
        std::unique_lock lock(mutex);
        while (true)
        {
            cv.wait(lock, [&] { return quit || !pending.empty(); });
            if (pending.empty())
                return;

            data.swap(pending);
            pending.clear();
            bool stopping = quit;
            lock.unlock();

            bool sent = false;
            try
            {
                send(data);
                sent = true;
            }
            catch (...)
            {
                LOG_ERROR(log, "Cannot send metrics, will retry with the next batch: {}", getCurrentExceptionMessage(/* with_stacktrace = */ false));
                session = nullptr;
            }

            lock.lock();
            if (!sent && !stopping)
            {
                /// Keep the data for the next attempt, before the rows that arrived meanwhile.
                if (data.size() + pending.size() <= options.max_pending_bytes)
                    pending.insert(0, data);
                else
                    LOG_WARNING(log, "Dropping {} bytes of metrics that could not be sent", data.size());
                /// Retry on the next batch instead of in a tight loop.
                cv.wait(lock, [&] { return quit || pending.size() > data.size(); });
            }
            if (stopping)
                return;
        }
    }

    Poco::Net::HTTPClientSession & getSession()
    {
        if (session)
            return *session;

#if USE_SSL
        if (uri.getScheme() == "https")
        {
            Poco::Net::Context::Ptr context = new Poco::Net::Context(
                Poco::Net::Context::CLIENT_USE, options.ca_file, Poco::Net::Context::VERIFY_RELAXED, 9, /* loadDefaultCAs = */ options.ca_file.empty());
            session = std::make_unique<Poco::Net::HTTPSClientSession>(uri.getHost(), uri.getPort(), context);
        }
        else
#endif
            session = std::make_unique<Poco::Net::HTTPClientSession>(uri.getHost(), uri.getPort());

        session->setTimeout(Poco::Timespan(30, 0));
        session->setKeepAlive(true);
        return *session;
    }

    void send(const std::string & data)
    {
        compressed.resize(ZSTD_compressBound(data.size()));
        size_t compressed_size = ZSTD_compress(compressed.data(), compressed.size(), data.data(), data.size(), /* compressionLevel = */ 3);
        if (ZSTD_isError(compressed_size))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot compress the metrics: {}", ZSTD_getErrorName(compressed_size));
        compressed.resize(compressed_size);

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
        request.setHost(uri.getHost());
        request.set("X-ClickHouse-User", options.user);
        if (!options.password.empty())
            request.set("X-ClickHouse-Key", options.password);
        request.set("Content-Encoding", "zstd");
        request.setContentType("text/tab-separated-values");
        request.setContentLength(static_cast<std::streamsize>(compressed.size()));

        auto & http = getSession();
        Stopwatch watch;
        http.sendRequest(request).write(compressed.data(), static_cast<std::streamsize>(compressed.size()));

        Poco::Net::HTTPResponse response;
        auto & response_body = http.receiveResponse(response);
        std::string body;
        Poco::StreamCopier::copyToString(response_body, body);

        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "The server returned HTTP {} {}: {}", static_cast<int>(response.getStatus()), response.getReason(), body);

        LOG_DEBUG(log, "Sent {} bytes ({} compressed) in {:.3f} sec", data.size(), compressed.size(), watch.elapsedSeconds());
    }

    const Options & options;
    LoggerPtr log;
    Poco::URI uri;
    std::unique_ptr<Poco::Net::HTTPClientSession> session;
    std::string compressed;

    std::mutex mutex;
    std::condition_variable cv;
    std::string pending;
    bool quit = false;
    std::thread thread;
};


/// Termination signals are handled by a dedicated thread with `sigwait`, so that the main loop can be woken up
/// with a condition variable.
std::mutex stop_mutex;
std::condition_variable stop_cv;
bool stop_requested = false;

void startSignalThread()
{
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    std::thread([signals]
    {
        int sig = 0;
        sigwait(&signals, &sig);
        {
            std::lock_guard lock(stop_mutex);
            stop_requested = true;
        }
        stop_cv.notify_all();
    }).detach();
}

int run(int argc, char ** argv)
{
    Options options = parseOptions(argc, argv);

    if (options.print_schema)
    {
        std::cout << getSchema(options.table);
        return 0;
    }

    Poco::Logger::root().setChannel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setLevel(options.log_level);
    auto log = getLogger("MetricsAgent");

    startSignalThread();

    AgentAsynchronousMetrics metrics(options.interval);

    if (options.once)
    {
        /// The first update only remembers the counters to compute the deltas.
        auto now = std::chrono::system_clock::now();
        metrics.update(now);
        {
            std::unique_lock lock(stop_mutex);
            stop_cv.wait_until(lock, now + std::chrono::seconds(options.interval), [] { return stop_requested; });
        }
        now = std::chrono::system_clock::now();
        metrics.update(now);
        std::string rows;
        {
            WriteBufferFromString out(rows);
            formatRows(out, metrics.getValues(), std::chrono::system_clock::to_time_t(now), options);
        }
        std::cout << rows;
        return 0;
    }

    Sender sender(options);

    LOG_INFO(log, "Sending metrics of host {} to table {} every {} sec, collected every {} sec",
        options.hostname, options.table, options.send_interval, options.interval);

    const auto interval = std::chrono::seconds(options.interval);
    const auto send_interval = std::chrono::seconds(options.send_interval);

    std::string rows;
    auto batch_start = std::chrono::system_clock::now();
    /// Align the updates to the interval, as the server does.
    auto next_update = std::chrono::time_point_cast<std::chrono::seconds>(batch_start) + interval;

    while (true)
    {
        {
            std::unique_lock lock(stop_mutex);
            if (stop_cv.wait_until(lock, next_update, [] { return stop_requested; }))
                break;
        }

        metrics.update(next_update);
        {
            WriteBufferFromString out(rows, AppendModeTag{});
            formatRows(out, metrics.getValues(), std::chrono::system_clock::to_time_t(next_update), options);
        }

        if (next_update - batch_start >= send_interval)
        {
            sender.push(std::move(rows));
            rows.clear();
            batch_start = next_update;
        }

        next_update += interval;
        /// If the process was suspended or the update took too long, skip the missed updates.
        auto now = std::chrono::system_clock::now();
        if (next_update < now)
            next_update = std::chrono::time_point_cast<std::chrono::seconds>(now) + interval;
    }

    LOG_INFO(log, "Stopping");
    if (!rows.empty())
        sender.push(std::move(rows));
    return 0;
}

}


int main(int argc, char ** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(false) << "\n";
        return 1;
    }
}
