#include <BackgroundQuery.h>

#include <Access/AccessControl.h>
#include <Client/ClientBase.h>
#include <Client/Connection.h>
#include <Common/ErrnoException.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/Exception.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/Throttler.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Formats/FormatFactory.h>

#include <base/scope_guard.h>

#include <Poco/Path.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <fstream>
#include <map>
#include <mutex>
#include <system_error>
#include <thread>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CREATE_FILE;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int LOGICAL_ERROR;
}

namespace
{

bool isTerminal(BackgroundQueryManager::State state)
{
    return state == BackgroundQueryManager::State::Succeeded || state == BackgroundQueryManager::State::Failed
        || state == BackgroundQueryManager::State::Cancelled;
}

class FileDescriptor
{
public:
    explicit FileDescriptor(int fd_)
        : fd(fd_)
    {
    }
    ~FileDescriptor()
    {
        if (fd >= 0)
            ::close(fd);
    }

    FileDescriptor(const FileDescriptor &) = delete;
    FileDescriptor & operator=(const FileDescriptor &) = delete;

    int get() const { return fd; }

private:
    int fd;
};

class FileDescriptorStreamBuf final : public std::streambuf
{
public:
    explicit FileDescriptorStreamBuf(int fd_)
        : fd(fd_)
    {
        setp(buffer.data(), buffer.data() + buffer.size());
    }

    ~FileDescriptorStreamBuf() override { sync(); }

protected:
    int_type overflow(int_type character) override
    {
        if (flushBuffer() != 0)
            return traits_type::eof();
        if (!traits_type::eq_int_type(character, traits_type::eof()))
        {
            *pptr() = traits_type::to_char_type(character);
            pbump(1);
        }
        return traits_type::not_eof(character);
    }

    std::streamsize xsputn(const char * data, std::streamsize size) override
    {
        if (flushBuffer() != 0)
            return 0;
        return writeAll(data, size);
    }

    int sync() override { return flushBuffer(); }

private:
    std::streamsize writeAll(const char * data, std::streamsize size)
    {
        std::streamsize written = 0;
        while (written < size)
        {
            const ssize_t result = ::write(fd, data + written, static_cast<size_t>(size - written));
            if (result > 0)
            {
                written += result;
                continue;
            }
            if (result < 0 && errno == EINTR)
                continue;
            break;
        }
        return written;
    }

    int flushBuffer()
    {
        const auto size = static_cast<std::streamsize>(pptr() - pbase());
        if (size == 0)
            return 0;
        const auto written = writeAll(pbase(), size);
        pbump(-static_cast<int>(size));
        return written == size ? 0 : -1;
    }

    int fd;
    std::array<char, 4096> buffer;
};

/// Create a mode-0600 spool, open both interfaces that ClientBase needs, and
/// unlink it immediately. The descriptors keep the contents alive without
/// leaving query results behind in the temporary directory after a crash.
class SecureTemporaryFile
{
public:
    explicit SecureTemporaryFile(std::string_view suffix)
    {
        file_path = Poco::Path(Poco::Path::temp(), "clickhouse-client-background-XXXXXX-" + String(suffix)).toString();

        /// mkstemps preserves the suffix, which is useful while diagnosing
        /// leaked descriptors without making the file name predictable.
        const int suffix_length = static_cast<int>(suffix.size() + 1);
        fd = ::mkstemps(file_path.data(), suffix_length);
        if (fd < 0)
            throw ErrnoException(ErrorCodes::CANNOT_CREATE_FILE, "Cannot create background-query spool");

        bool initialized = false;
        SCOPE_EXIT({
            if (!initialized)
            {
                output_stream.reset();
                stream_buffer.reset();
                ::close(fd);
                ::unlink(file_path.c_str());
            }
        });

        const int descriptor_flags = ::fcntl(fd, F_GETFD);
        if (descriptor_flags < 0 || ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0)
            throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot make background-query spool close-on-exec");

        const int status_flags = ::fcntl(fd, F_GETFL);
        if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_APPEND) != 0)
            throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set append mode on background-query spool");

        stream_buffer = std::make_unique<FileDescriptorStreamBuf>(fd);
        output_stream = std::make_unique<std::ostream>(stream_buffer.get());

        if (::unlink(file_path.c_str()) != 0)
            throw ErrnoException(ErrorCodes::CANNOT_CREATE_FILE, "Cannot unlink background-query spool {}", file_path);
        file_path.clear();
        initialized = true;
    }

    ~SecureTemporaryFile()
    {
        if (output_stream)
            output_stream->flush();
        output_stream.reset();
        stream_buffer.reset();
        if (fd >= 0)
            ::close(fd);
        if (!file_path.empty())
            ::unlink(file_path.c_str());
    }

    SecureTemporaryFile(const SecureTemporaryFile &) = delete;
    SecureTemporaryFile & operator=(const SecureTemporaryFile &) = delete;

    int getFD() const { return fd; }
    std::ostream & stream() { return *output_stream; }

    void flush()
    {
        output_stream->flush();
        if (!*output_stream)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot flush background-query spool");
    }

    void replayTo(WriteBuffer & output)
    {
        flush();
        if (::lseek(fd, 0, SEEK_SET) < 0)
            throw ErrnoException(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Cannot rewind background-query spool");

        ReadBufferFromFileDescriptor input(fd);
        copyData(input, output);
    }

    UInt64 size() const
    {
        struct stat file_stat{};
        if (::fstat(fd, &file_stat) != 0)
            return 0;
        return static_cast<UInt64>(file_stat.st_size);
    }

private:
    String file_path;
    int fd = -1;
    std::unique_ptr<FileDescriptorStreamBuf> stream_buffer;
    std::unique_ptr<std::ostream> output_stream;
};

class BackgroundClient final : public ClientBase
{
public:
    BackgroundClient(
        int input_fd,
        int output_fd,
        int error_fd,
        std::istream & input_stream_,
        std::ostream & output_stream_,
        std::ostream & error_stream_,
        ContextMutablePtr context,
        ConnectionParameters connection_parameters_,
        Poco::AutoPtr<Poco::Util::AbstractConfiguration> configuration_,
        String default_database_,
        size_t max_client_network_bandwidth_,
        String client_local_timezone_,
        String default_output_format_,
        bool is_default_format_,
        CompressionMethod default_output_compression_method_,
        bool has_vertical_output_suffix_,
        bool inline_insert_data_,
        bool allow_merge_tree_settings_,
        QueryProcessingStage::Enum query_processing_stage_,
        ClientInfo::QueryKind query_kind_,
        const std::atomic_bool & cancellation_requested_)
        : ClientBase(input_fd, output_fd, error_fd, input_stream_, output_stream_, error_stream_)
        , configuration_snapshot(std::move(configuration_))
        , configuration(new Poco::Util::LayeredConfiguration)
        , cancellation_requested(cancellation_requested_)
    {
        configuration->addWriteable(configuration_snapshot, 0);
        connection_parameters = std::move(connection_parameters_);
        connection_parameters.adopted_socket.reset();
        default_database = std::move(default_database_);
        connection_parameters.default_database = default_database;
        max_client_network_bandwidth = max_client_network_bandwidth_;
        client_local_timezone = std::move(client_local_timezone_);
        query_parameters = context->getQueryParameters();
        query_processing_stage = query_processing_stage_;
        query_kind = query_kind_;
        inline_insert_data = inline_insert_data_;
        allow_merge_tree_settings = allow_merge_tree_settings_;
        /// sendQuery advertises pending external data. Even with no external
        /// tables, ClientBase must send the terminating empty Data block.
        send_external_tables = true;

        is_interactive = false;
        need_render_progress = false;
        need_render_progress_table = false;
        need_render_profile_events = false;
        print_stack_trace = configuration->getBool("stacktrace", false);

        /// Populate the client-owned defaults and command-line settings before
        /// installing the copied interactive context. Otherwise configured
        /// format options could overwrite a later `SET output_format` captured
        /// in that context.
        setDefaultFormatsAndCompressionFromConfiguration();
        initClientContext(std::move(context));
        default_output_format = std::move(default_output_format_);
        is_default_format = is_default_format_;
        default_output_compression_method = default_output_compression_method_;
        has_vertical_output_suffix = has_vertical_output_suffix_;
    }

    bool execute(const String & query)
    {
        if (isQueryCancellationRequested())
            return false;
        connect();
        if (isQueryCancellationRequested())
            return false;
        return processTextAsSingleQuery(query);
    }

    String errorMessage() const
    {
        if (server_exception)
            return getExceptionMessageForLogging(*server_exception, print_stack_trace, true);
        if (client_exception)
            return client_exception->message();
        return {};
    }

    const String & resultFormat() const { return result_format; }
    bool resultIsTTYFriendly() const { return result_is_tty_friendly; }
    bool cancellationWasObserved() const { return cancellation_was_observed || cancelled.load(std::memory_order_acquire); }

private:
    Poco::Util::LayeredConfiguration & getClientConfiguration() override { return *configuration; }

    void connect() override
    {
        connection = Connection::createConnection(connection_parameters, client_context);
        connection_parameters.adopted_socket.reset();

        if (max_client_network_bandwidth)
            connection->setThrottler(std::make_shared<Throttler>(max_client_network_bandwidth, 0, ""));

        String server_name;
        UInt64 server_version_major = 0;
        UInt64 server_version_minor = 0;
        UInt64 server_version_patch = 0;
        connection->getServerVersion(
            connection_parameters.timeouts, server_name, server_version_major, server_version_minor, server_version_patch, server_revision);

        server_version = std::to_string(server_version_major) + "." + std::to_string(server_version_minor) + "."
            + std::to_string(server_version_patch);
        settings_from_server = assert_cast<Connection &>(*connection).settingsFromServer();
        connection->setDefaultDatabase(connection_parameters.default_database);
        client_context->getAccessControl().setPasswordComplexityRules(connection->getPasswordComplexityRules());
    }

    void processError(std::string_view query) const override
    {
        if (server_exception)
            error_stream << "Received exception from server (version " << server_version << "):\n"
                         << getExceptionMessageForLogging(*server_exception, print_stack_trace, true) << '\n';
        if (client_exception)
            error_stream << "Error on processing query: " << client_exception->message() << '\n';
        if (server_exception || client_exception)
            error_stream << "(query: " << query << ")\n";
        error_stream.flush();
    }

    String getName() const override { return "background-client"; }
    void setupSignalHandler() override { }
    void printHelpMessage(const OptionsDescription &) override { }
    void addExtraOptions(OptionsDescription &) override { }
    void processOptions(
        const OptionsDescription &, const CommandLineOptions &, const std::vector<Arguments> &, const std::vector<Arguments> &) override
    {
    }
    void processConfig() override { }
    bool isEmbeeddedClient() const override { return false; }
    bool isQueryCancellationRequested() const override
    {
        const bool requested = cancellation_requested.load(std::memory_order_acquire);
        cancellation_was_observed |= requested;
        return requested;
    }
    void onOutputFormatSelected(std::string_view format, bool writes_to_stdout) override
    {
        result_format = format;
        result_is_tty_friendly = !writes_to_stdout || FormatFactory::instance().checkIfOutputFormatIsTTYFriendly(result_format);
    }

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> configuration_snapshot;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> configuration;
    const std::atomic_bool & cancellation_requested;
    String result_format;
    bool result_is_tty_friendly = true;
    mutable bool cancellation_was_observed = false;
};

}

struct BackgroundQueryManager::Impl
{
    struct Job
    {
        Job(JobId id_, String query_, String display_query_, String query_id_, ContextMutablePtr context_, Snapshot snapshot_)
            : id(id_)
            , query(std::move(query_))
            , display_query(std::move(display_query_))
            , query_id(std::move(query_id_))
            , context(std::move(context_))
            , snapshot(std::make_unique<Snapshot>(std::move(snapshot_)))
            , output("result")
            , diagnostics("error")
            , started_at(std::chrono::steady_clock::now())
        {
        }

        JobId id;
        String query;
        String display_query;
        String query_id;
        ContextMutablePtr context;
        std::unique_ptr<Snapshot> snapshot;
        SecureTemporaryFile output;
        SecureTemporaryFile diagnostics;
        std::chrono::steady_clock::time_point started_at;
        std::chrono::steady_clock::time_point finished_at;
        std::atomic<State> state{State::Starting};
        std::atomic_bool cancel_requested{false};
        String error;
        String output_format;
        bool output_is_tty_friendly = true;
        bool cancellation_was_observed = false;
        std::mutex client_mutex;
        BackgroundClient * active_client = nullptr;
        std::thread worker;
    };

    mutable std::mutex mutex;
    std::map<JobId, std::shared_ptr<Job>> jobs;
    JobId next_id = 1;
    bool shutting_down = false;

    static JobInfo info(const Job & job)
    {
        JobInfo result;
        result.id = job.id;
        result.query_id = job.query_id;
        result.query = job.display_query;
        result.state = job.state.load(std::memory_order_acquire);
        const auto end = isTerminal(result.state) ? job.finished_at : std::chrono::steady_clock::now();
        result.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - job.started_at);
        result.spool_bytes = job.output.size() + job.diagnostics.size();
        if (isTerminal(result.state))
        {
            result.output_format = job.output_format;
            result.output_is_tty_friendly = job.output_is_tty_friendly;
            result.error = job.error;
        }
        return result;
    }

    static void finish(const std::shared_ptr<Job> & job, State state, String error = {})
    {
        job->error = std::move(error);
        job->finished_at = std::chrono::steady_clock::now();
        job->state.store(state, std::memory_order_release);
    }

    static void run(const std::shared_ptr<Job> & job) noexcept
    {
        try
        {
            /// Credentials are needed only while the worker connects. Moving
            /// the snapshot out first prevents every early-exit path from
            /// retaining a second copy of connection secrets in the job list.
            auto snapshot = std::move(job->snapshot);
            auto context = std::move(job->context);
            auto query = std::move(job->query);

            ThreadStatus thread_status;
            setThreadName(ThreadName::BACKGROUND_QUERY);
            QueryScope query_scope = QueryScope::create(context);

            if (job->cancel_requested.load(std::memory_order_acquire))
            {
                finish(job, State::Cancelled);
                return;
            }

            const int null_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
            if (null_fd < 0)
                throw std::system_error(errno, std::generic_category(), "Cannot open /dev/null for a background query");
            FileDescriptor input_descriptor(null_fd);
            std::ifstream input_stream("/dev/null");

            auto client = std::make_unique<BackgroundClient>(
                input_descriptor.get(),
                job->output.getFD(),
                job->diagnostics.getFD(),
                input_stream,
                job->output.stream(),
                job->diagnostics.stream(),
                std::move(context),
                std::move(snapshot->connection_parameters),
                std::move(snapshot->configuration),
                std::move(snapshot->default_database),
                snapshot->max_client_network_bandwidth,
                std::move(snapshot->client_local_timezone),
                std::move(snapshot->default_output_format),
                snapshot->is_default_format,
                snapshot->default_output_compression_method,
                snapshot->has_vertical_output_suffix,
                snapshot->inline_insert_data,
                snapshot->allow_merge_tree_settings,
                snapshot->query_processing_stage,
                snapshot->query_kind,
                job->cancel_requested);
            snapshot.reset();

            /// Preserve the effective format even when execute() throws after
            /// writing a partial result. The terminal state is published only
            /// after this guard runs, so foreground() can safely decide
            /// whether replaying the spool to a TTY needs confirmation.
            SCOPE_EXIT({
                if (client)
                {
                    job->output_format = client->resultFormat();
                    job->output_is_tty_friendly = client->resultIsTTYFriendly();
                    job->cancellation_was_observed = client->cancellationWasObserved();
                }
            });

            {
                std::lock_guard lock(job->client_mutex);
                if (job->cancel_requested.load(std::memory_order_acquire))
                {
                    job->output_format = client->resultFormat();
                    job->output_is_tty_friendly = client->resultIsTTYFriendly();
                    job->cancellation_was_observed = true;
                    client.reset();
                    finish(job, State::Cancelled);
                    return;
                }
                job->active_client = client.get();
                job->state.store(State::Running, std::memory_order_release);
            }

            SCOPE_EXIT({
                std::lock_guard lock(job->client_mutex);
                job->active_client = nullptr;
            });

            const bool succeeded = client->execute(query);
            const bool cancellation_was_observed = client->cancellationWasObserved();
            String error = client->errorMessage();
            job->output_format = client->resultFormat();
            job->output_is_tty_friendly = client->resultIsTTYFriendly();
            job->cancellation_was_observed = cancellation_was_observed;

            {
                std::lock_guard lock(job->client_mutex);
                job->active_client = nullptr;
            }
            client.reset();
            job->output.flush();
            job->diagnostics.flush();

            if (cancellation_was_observed)
                finish(job, State::Cancelled, std::move(error));
            else if (succeeded)
                finish(job, State::Succeeded);
            else
                finish(job, State::Failed, std::move(error));
        }
        catch (...)
        {
            try
            {
                String error = getCurrentExceptionMessage(false);
                job->diagnostics.stream() << error << '\n';
                job->diagnostics.flush();
                finish(
                    job,
                    job->cancellation_was_observed ? State::Cancelled : State::Failed,
                    std::move(error));
            }
            catch (...)
            {
                /// Never let an exception escape a std::thread entry point.
                job->finished_at = std::chrono::steady_clock::now();
                job->state.store(
                    job->cancellation_was_observed ? State::Cancelled : State::Failed,
                    std::memory_order_release);
            }
        }
    }

    static void requestCancellation(const std::shared_ptr<Job> & job)
    {
        job->cancel_requested.store(true, std::memory_order_release);
        std::lock_guard lock(job->client_mutex);
        if (job->active_client)
            job->active_client->stopQuery();
    }
};

BackgroundQueryManager::Snapshot::Snapshot(
    ContextPtr context_,
    const ConnectionParameters & connection_parameters_,
    const Poco::Util::AbstractConfiguration & configuration_,
    String default_database_,
    size_t max_client_network_bandwidth_,
    String client_local_timezone_,
    String default_output_format_,
    bool is_default_format_,
    CompressionMethod default_output_compression_method_,
    bool has_vertical_output_suffix_,
    bool inline_insert_data_,
    bool allow_merge_tree_settings_,
    QueryProcessingStage::Enum query_processing_stage_,
    ClientInfo::QueryKind query_kind_)
    : context(std::move(context_))
    , connection_parameters(connection_parameters_)
    , configuration(ConfigHelper::createEmpty())
    , default_database(std::move(default_database_))
    , max_client_network_bandwidth(max_client_network_bandwidth_)
    , client_local_timezone(std::move(client_local_timezone_))
    , default_output_format(std::move(default_output_format_))
    , is_default_format(is_default_format_)
    , default_output_compression_method(default_output_compression_method_)
    , has_vertical_output_suffix(has_vertical_output_suffix_)
    , inline_insert_data(inline_insert_data_)
    , allow_merge_tree_settings(allow_merge_tree_settings_)
    , query_processing_stage(query_processing_stage_)
    , query_kind(query_kind_)
{
    /// BackgroundClient only needs presentation-related client options. Do
    /// not retain the full configuration, which can include plaintext
    /// credentials and unrelated connection profiles.
    static constexpr std::array<std::string_view, 13> copied_configuration_keys{
        "quota_key",
        "stacktrace",
        "format",
        "output-format",
        "input-format",
        "vertical",
        "insert_format_max_block_size",
        "insert_format_max_block_size_bytes",
        "insert_format_min_block_size_rows",
        "insert_format_min_block_size_bytes",
        "print-time-to-stderr",
        "print-memory-to-stderr",
        "print-num-processed-rows",
    };
    for (const auto key : copied_configuration_keys)
        if (configuration_.has(String(key)))
            configuration->setString(String(key), configuration_.getString(String(key)));

    connection_parameters.default_database = default_database;
    connection_parameters.adopted_socket.reset();
}

BackgroundQueryManager::BackgroundQueryManager()
    : impl(std::make_unique<Impl>())
{
}

BackgroundQueryManager::~BackgroundQueryManager()
{
    shutdown();
}

BackgroundQueryManager::JobId BackgroundQueryManager::start(String query, String display_query, Snapshot snapshot)
{
    if (!snapshot.context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A background query requires a client context");

    /// Copy before launching the worker so later interactive SET/USE commands
    /// cannot race with or change the new job's session snapshot.
    auto context = Context::createCopy(snapshot.context);
    snapshot.context.reset();
    context->setCurrentQueryId("");
    context->setSetting("run_query_in_background", String("0"));

    std::shared_ptr<Impl::Job> job;
    {
        std::lock_guard lock(impl->mutex);
        if (impl->shutting_down)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Background-query manager is shutting down");

        const JobId id = impl->next_id++;
        job = std::make_shared<Impl::Job>(
            id, std::move(query), std::move(display_query), context->getCurrentQueryId(), std::move(context), std::move(snapshot));
        impl->jobs.emplace(id, job);
    }

    try
    {
        job->worker = std::thread([job] { Impl::run(job); });
    }
    catch (...)
    {
        const String error = getCurrentExceptionMessage(false);
        {
            std::lock_guard lock(impl->mutex);
            impl->jobs.erase(job->id);
        }
        throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot start background-query worker: {}", error);
    }
    return job->id;
}

std::vector<BackgroundQueryManager::JobInfo> BackgroundQueryManager::list() const
{
    std::vector<JobInfo> result;
    std::lock_guard lock(impl->mutex);
    result.reserve(impl->jobs.size());
    for (const auto & [_, job] : impl->jobs)
        result.push_back(Impl::info(*job));
    return result;
}

std::optional<BackgroundQueryManager::JobInfo> BackgroundQueryManager::get(JobId id) const
{
    std::lock_guard lock(impl->mutex);
    auto it = impl->jobs.find(id);
    if (it == impl->jobs.end())
        return {};
    return Impl::info(*it->second);
}

BackgroundQueryManager::ForegroundResult BackgroundQueryManager::foreground(
    JobId id, WriteBuffer & output, WriteBuffer & diagnostics, bool allow_unsafe_output)
{
    std::shared_ptr<Impl::Job> job;
    JobInfo job_info;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->jobs.find(id);
        if (it == impl->jobs.end())
            return {};

        job = it->second;
        job_info = Impl::info(*job);
        if (!isTerminal(job_info.state))
            return {ForegroundStatus::Running, std::move(job_info)};
        if (!job_info.output_is_tty_friendly && !allow_unsafe_output)
            return {ForegroundStatus::UnsafeOutput, std::move(job_info)};
        impl->jobs.erase(it);
    }

    if (job->worker.joinable())
        job->worker.join();

    job->output.replayTo(output);
    output.next();

    job->diagnostics.replayTo(diagnostics);
    diagnostics.next();

    return {ForegroundStatus::Replayed, std::move(job_info)};
}

BackgroundQueryManager::CancelStatus BackgroundQueryManager::cancel(JobId id)
{
    std::shared_ptr<Impl::Job> job;
    bool discard = false;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->jobs.find(id);
        if (it == impl->jobs.end())
            return CancelStatus::NotFound;
        job = it->second;
        if (isTerminal(job->state.load(std::memory_order_acquire)))
        {
            impl->jobs.erase(it);
            discard = true;
        }
    }

    if (discard)
    {
        if (job->worker.joinable())
            job->worker.join();
        return CancelStatus::Discarded;
    }

    Impl::requestCancellation(job);
    return CancelStatus::Requested;
}

void BackgroundQueryManager::shutdown()
{
    std::vector<std::shared_ptr<Impl::Job>> jobs;
    {
        std::lock_guard lock(impl->mutex);
        if (impl->shutting_down && impl->jobs.empty())
            return;
        impl->shutting_down = true;
        for (const auto & [_, job] : impl->jobs)
            jobs.push_back(job);
    }

    for (const auto & job : jobs)
        if (!isTerminal(job->state.load(std::memory_order_acquire)))
            Impl::requestCancellation(job);

    for (const auto & job : jobs)
        if (job->worker.joinable())
            job->worker.join();

    std::lock_guard lock(impl->mutex);
    impl->jobs.clear();
}

std::string_view BackgroundQueryManager::stateName(State state)
{
    switch (state)
    {
        case State::Starting: return "starting";
        case State::Running: return "running";
        case State::Succeeded: return "succeeded";
        case State::Failed: return "failed";
        case State::Cancelled: return "cancelled";
    }
    return "unknown";
}

}
