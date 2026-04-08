#pragma once
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/CacheLine.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/InterruptListener.h>
#include <Interpreters/Context.h>

#include <Generator.h>
#include <Stats.h>

#include <filesystem>

using Ports = std::vector<UInt16>;
using Strings = std::vector<std::string>;

struct BenchmarkContext
{
public:
    void initializeFromConfig(const Poco::Util::AbstractConfiguration & config);

    void startup(Coordination::ZooKeeper & zookeeper);
    void cleanup(Coordination::ZooKeeper & zookeeper);

private:
    struct Node
    {
        StringGetter name;
        std::optional<StringGetter> data;
        std::vector<std::shared_ptr<Node>> children;
        size_t repeat_count = 0;

        std::shared_ptr<Node> clone() const;

        void createNode(Coordination::ZooKeeper & zookeeper, const std::string & parent_path, const Coordination::ACLs & acls) const;
        void dumpTree(int level = 0) const;
    };

    static std::shared_ptr<Node> parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::vector<std::shared_ptr<Node>> root_nodes;
    Coordination::ACLs default_acls;
};

class Runner
{
private:
    struct alignas(DB::CH_CACHE_LINE_SIZE) ThreadState
    {
        size_t thread_idx;
        Stats thread_info;
    };

public:
    Runner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const std::string & input_request_log_,
        const std::string & setup_nodes_snapshot_path_,
        const Strings & hosts_strings_,
        std::optional<double> max_time_,
        std::optional<double> delay_,
        std::optional<bool> continue_on_error_,
        std::optional<size_t> max_iterations_);

    void thread(std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers, ThreadState & thread_state);

    void printNumberOfRequestsExecuted(size_t num)
    {
        std::cerr << "Requests executed: " << num << ".\n";
    }

    void runBenchmark();

    ~Runner();
private:
    struct ConnectionInfo
    {
        std::string host;

        bool secure = false;
        int32_t session_timeout_ms = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
        int32_t connection_timeout_ms = Coordination::DEFAULT_CONNECTION_TIMEOUT_MS;
        int32_t operation_timeout_ms = Coordination::DEFAULT_OPERATION_TIMEOUT_MS;
        bool use_compression = false;
        bool use_xid_64 = false;

        size_t sessions = 1;
    };

    void parseHostsFromConfig(const Poco::Util::AbstractConfiguration & config);

    void runBenchmarkWithGenerator();
    void runBenchmarkFromLog();

    void writeOutputString(const std::string & output_string, int64_t start_timestamp_ms);

    void createConnections();
    std::vector<std::shared_ptr<Coordination::ZooKeeper>> refreshConnections();
    std::shared_ptr<Coordination::ZooKeeper> getConnection(const ConnectionInfo & connection_info, size_t connection_info_idx) const;

    std::shared_ptr<Stats> mergeThreadInfos();

    std::string input_request_log;
    std::string setup_nodes_snapshot_path;

    size_t concurrency = 1;
    size_t pipeline_depth = 1;

    std::optional<ThreadPool> pool;

    DB::ConfigurationPtr config_ptr;

    double max_time = 0;
    double delay = 1;
    bool continue_on_error = false;
    bool enable_tracing = false;
    size_t max_iterations = 0;

    /// Iteration counter, excluding requests during warmup. This is what max_iterations limits.
    std::atomic<size_t> requests_started = 0;
    std::atomic<bool> shutdown = false;

    double warmup_seconds = 0;
    std::atomic<bool> warmup_complete = false;

    std::shared_ptr<Stats> info;
    bool print_to_stdout = false;
    std::optional<std::filesystem::path> file_output;
    bool output_file_with_timestamp = false;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    std::vector<ThreadState> threads;

    std::mutex mutex; // for writing to stdout

    std::mutex connection_mutex;
    ConnectionInfo default_connection_info;
    std::vector<ConnectionInfo> connection_infos;
    std::vector<std::shared_ptr<Coordination::ZooKeeper>> connections;
    std::unordered_map<size_t, size_t> connections_to_info_map;

    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr global_context;

    BenchmarkContext benchmark_context;
};
