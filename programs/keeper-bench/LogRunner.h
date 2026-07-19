#pragma once

#include <Common/Config/ConfigProcessor.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Interpreters/Context.h>

#include <NodesSetup.h>
#include <RunnerCommon.h>
#include <Stats.h>

#include <optional>

/// Replays requests recorded in a request log file against a Keeper cluster,
/// comparing results with the recorded ones.
/// With `--setup-nodes-snapshot-path`, doesn't connect anywhere; instead builds
/// a Keeper snapshot containing the nodes the replayed requests expect to exist.
class LogRunner
{
public:
    LogRunner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const std::string & input_request_log_,
        const std::string & setup_nodes_snapshot_path_,
        const Strings & hosts_strings_,
        std::optional<double> delay_);

    void runBenchmark();

    ~LogRunner();

private:
    void collectSetupNodes();
    void replay();

    std::string input_request_log;
    std::string setup_nodes_snapshot_path;

    size_t concurrency = 1;
    double delay = 1;

    std::optional<ThreadPool> pool;

    DB::ConfigurationPtr config_ptr;

    std::shared_ptr<Stats> info;
    BenchmarkOutput output;

    Stopwatch delay_watch;

    ConnectionFactory connection_factory;

    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr global_context;

    NodesSetup nodes_setup;
};
