#pragma once

#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <functional>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <Poco/Process.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Version.h>
#include <common/types.h>
#include <common/logger_useful.h>
#include <common/getThreadId.h>
#include <daemon/GraphiteWriter.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/StatusFile.h>
#include <loggers/Loggers.h>


/// \brief Base class for applications that can run as daemons.
///
/// \code
/// # Some possible command line options:
/// #    --config-file, -C or --config - path to configuration file. By default - config.xml in the current directory.
/// #    --log-file
/// #    --errorlog-file
/// #    --daemon - run as daemon; without this option, the program will be attached to the terminal and also output logs to stderr.
/// <daemon_name> --daemon --config-file=localfile.xml --log-file=log.log --errorlog-file=error.log
/// \endcode
///
/// You can configure different log options for different loggers used inside program
///  by providing subsections to "logger" in configuration file.
class BaseDaemon : public Poco::Util::ServerApplication, public Loggers
{
    friend class SignalListener;

public:
    static inline constexpr char DEFAULT_GRAPHITE_CONFIG_NAME[] = "graphite";

    BaseDaemon();
    ~BaseDaemon() override;

    /// Load configuration, prepare loggers, etc.
    void initialize(Poco::Util::Application &) override;

    void reloadConfiguration();

    /// Process command line parameters
    void defineOptions(Poco::Util::OptionSet & new_options) override;

    /// Graceful shutdown
    static void terminate();

    /// Forceful shutdown
    [[noreturn]] void kill();

    /// Cancellation request has been received.
    bool isCancelled() const
    {
        return is_cancelled;
    }

    static BaseDaemon & instance()
    {
        return dynamic_cast<BaseDaemon &>(Poco::Util::Application::instance());
    }

    /// return none if daemon doesn't exist, reference to the daemon otherwise
    static std::optional<std::reference_wrapper<BaseDaemon>> tryGetInstance() { return tryGetInstance<BaseDaemon>(); }

    /// В Graphite компоненты пути(папки) разделяются точкой.
    /// У нас принят путь формата root_path.hostname_yandex_ru.key
    /// root_path по умолчанию one_min
    /// key - лучше группировать по смыслу. Например "meminfo.cached" или "meminfo.free", "meminfo.total"
    template <class T>
    void writeToGraphite(const std::string & key, const T & value, const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME, time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        auto *writer = getGraphiteWriter(config_name);
        if (writer)
            writer->write(key, value, timestamp, custom_root_path);
    }

    template <class T>
    void writeToGraphite(const GraphiteWriter::KeyValueVector<T> & key_vals, const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME, time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        auto *writer = getGraphiteWriter(config_name);
        if (writer)
            writer->write(key_vals, timestamp, custom_root_path);
    }

    template <class T>
    void writeToGraphite(const GraphiteWriter::KeyValueVector<T> & key_vals, const std::chrono::system_clock::time_point & current_time, const std::string & custom_root_path)
    {
        auto *writer = getGraphiteWriter();
        if (writer)
            writer->write(key_vals, std::chrono::system_clock::to_time_t(current_time), custom_root_path);
    }

    GraphiteWriter * getGraphiteWriter(const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME)
    {
        if (graphite_writers.count(config_name))
            return graphite_writers[config_name].get();
        return nullptr;
    }

    /// close all process FDs except
    /// 0-2 -- stdin, stdout, stderr
    /// also doesn't close global internal pipes for signal handling
    static void closeFDs();

    /// If this method is called after initialization and before run,
    /// will fork child process and setup watchdog that will print diagnostic info, if the child terminates.
    /// argv0 is needed to change process name (consequently, it is needed for scripts involving "pgrep", "pidof" to work correctly).
    void shouldSetupWatchdog(char * argv0_);

    /// Hash of the binary for integrity checks.
    String getStoredBinaryHash() const;

protected:
    virtual void logRevision() const;

    /// thread safe
    virtual void handleSignal(int signal_id);

    /// initialize termination process and signal handlers
    virtual void initializeTerminationAndSignalProcessing();

    /// fork the main process and watch if it was killed
    void setupWatchdog();

    void waitForTerminationRequest()
#if defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION >= 0x02000000 // in old upstream poco not vitrual
    override
#endif
    ;
    /// thread safe
    virtual void onInterruptSignals(int signal_id);

    template <class Daemon>
    static std::optional<std::reference_wrapper<Daemon>> tryGetInstance();

    virtual std::string getDefaultCorePath() const;

    virtual std::string getDefaultConfigFileName() const;

    std::optional<DB::StatusFile> pid_file;

    std::atomic_bool is_cancelled{false};

    bool log_to_console = false;

    /// A thread that acts on HUP and USR1 signal (close logs).
    Poco::Thread signal_listener_thread;
    std::unique_ptr<Poco::Runnable> signal_listener;

    std::map<std::string, std::unique_ptr<GraphiteWriter>> graphite_writers;

    std::mutex signal_handler_mutex;
    std::condition_variable signal_event;
    std::atomic_size_t terminate_signals_counter{0};
    std::atomic_size_t sigint_signals_counter{0};

    std::string config_path;
    DB::ConfigProcessor::LoadedConfig loaded_config;
    Poco::Util::AbstractConfiguration * last_configuration = nullptr;

    String build_id_info;
    String stored_binary_hash;

    std::vector<int> handled_signals;

    bool should_setup_watchdog = false;
    char * argv0 = nullptr;
};


template <class Daemon>
std::optional<std::reference_wrapper<Daemon>> BaseDaemon::tryGetInstance()
{
    Daemon * ptr = nullptr;
    try
    {
        ptr = dynamic_cast<Daemon *>(&Poco::Util::Application::instance());
    }
    catch (const Poco::NullPointerException &)
    {
        /// if daemon doesn't exist than instance() throw NullPointerException
    }

    if (ptr)
        return std::optional<std::reference_wrapper<Daemon>>(*ptr);
    else
        return {};
}
