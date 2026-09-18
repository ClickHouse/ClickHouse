#include <BackgroundQuery.h>

#include <Access/AccessControl.h>
#include <Client/ClientBase.h>
#include <Client/Connection.h>
#include <Client/InternalTextLogs.h>
#include <Common/ErrnoException.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
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
#include <condition_variable>
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

/// Routes ostream diagnostics to the terminal until detachment, then to the spool.
/// This covers output which does not pass through ClientBase::std_out.
class RedirectableStreamBuf final : public std::streambuf
{
public:
    RedirectableStreamBuf(std::ostream & foreground_, std::ostream & spool_)
        : foreground(foreground_)
        , spool(spool_)
        , destination(&foreground)
    {
    }

    void detach()
    {
        if (detached)
            return;
        sync();
        destination = &spool;
        detached = true;
    }

protected:
    int_type overflow(int_type character) override
    {
        if (!traits_type::eq_int_type(character, traits_type::eof()))
            destination->put(traits_type::to_char_type(character));
        return *destination ? traits_type::not_eof(character) : traits_type::eof();
    }

    std::streamsize xsputn(const char * data, std::streamsize size) override
    {
        destination->write(data, size);
        return *destination ? size : 0;
    }

    int sync() override
    {
        destination->flush();
        return *destination ? 0 : -1;
    }

private:
    std::ostream & foreground;
    std::ostream & spool;
    std::ostream * destination;
    bool detached = false;
};

/// Creates and immediately unlinks a mode-0600 spool.
/// Its descriptors retain the contents without leaving files after a crash.
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

/// Keeps the formatter sink stable while the worker redirects completed flushes.
/// The destination lock serializes helper-thread flushes with detachment.
class RedirectableWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    RedirectableWriteBuffer(WriteBuffer & foreground_output_, int spool_fd)
        : foreground_output(foreground_output_)
        , spool_output(spool_fd)
        , destination(&foreground_output)
    {
    }

    void detach()
    {
        std::lock_guard lock(destination_mutex);
        if (detached)
            return;
        destination = &spool_output;
        detached = true;
    }

private:
    void nextImpl() override
    {
        std::lock_guard lock(destination_mutex);
        destination->write(working_buffer.begin(), offset());
        destination->next();
    }

    void finalizeImpl() override
    {
        next();
        std::lock_guard lock(destination_mutex);
        if (detached)
            spool_output.finalize();
        else
            foreground_output.next();
    }

    void cancelImpl() noexcept override
    {
        /// The foreground buffer is borrowed; only cancel the owned spool buffer.
        spool_output.cancel();
    }

    WriteBuffer & foreground_output;
    AutoCanceledWriteBuffer<WriteBufferFromFileDescriptor> spool_output;
    std::mutex destination_mutex;
    WriteBuffer * destination;
    bool detached = false;
};

struct BackgroundQueryMetrics
{
    Progress progress;
    std::atomic<double> cpu_usage{0};
    std::atomic<double> average_cpu_usage{0};
    std::atomic_bool cpu_usage_available{false};
    std::atomic<UInt64> memory_usage{0};
    std::atomic<UInt64> max_host_memory_usage{0};
    std::atomic<Int64> peak_memory_usage{-1};
    std::atomic<UInt64> temporary_data_on_disk{0};
    std::atomic<UInt64> max_host_temporary_data_on_disk{0};
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
        const std::atomic_bool & cancellation_requested_,
        BackgroundQueryMetrics & metrics_,
        std::unique_ptr<WriteBuffer> output_buffer_ = {},
        RedirectableWriteBuffer * redirectable_output_ = nullptr,
        ServerConnectionPtr adopted_connection_ = {},
        UInt64 adopted_server_revision_ = 0,
        String adopted_server_version_ = {},
        const std::atomic_bool * detachment_requested_ = nullptr,
        std::atomic_bool * detachment_acknowledged_ = nullptr,
        std::condition_variable * state_changed_ = nullptr,
        RedirectableStreamBuf * redirectable_output_stream_ = nullptr,
        RedirectableStreamBuf * redirectable_error_stream_ = nullptr,
        int diagnostic_spool_fd_ = -1,
        int foreground_stderr_fd_ = -1,
        String server_logs_file_ = {},
        const std::atomic_bool * progress_table_toggle_source_ = nullptr,
        bool logical_stdout_is_a_tty = false,
        bool logical_stderr_is_a_tty = false,
        uint16_t logical_terminal_width = 0,
        int foreground_tty_fd = -1,
        bool render_progress = false,
        bool render_progress_table = false,
        bool progress_table_toggle_enabled_ = true,
        bool progress_table_toggle_on_ = false)
        : ClientBase(input_fd, output_fd, error_fd, input_stream_, output_stream_, error_stream_)
        , configuration_snapshot(std::move(configuration_))
        , configuration(new Poco::Util::LayeredConfiguration)
        , cancellation_requested(cancellation_requested_)
        , metrics(metrics_)
        , redirectable_output(redirectable_output_)
        , detachment_requested(detachment_requested_)
        , detachment_acknowledged(detachment_acknowledged_)
        , state_changed(state_changed_)
        , redirectable_output_stream(redirectable_output_stream_)
        , redirectable_error_stream(redirectable_error_stream_)
        , diagnostic_spool_fd(diagnostic_spool_fd_)
        , foreground_stderr_fd(foreground_stderr_fd_)
        , progress_table_toggle_source(progress_table_toggle_source_)
    {
        if (output_buffer_)
            replaceOutputBuffer(std::move(output_buffer_));

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
        attached_mode = redirectable_output != nullptr;
        print_interactive_query_summary = attached_mode;
        poll_for_query_detachment = attached_mode;
        need_render_progress = attached_mode && render_progress;
        need_render_progress_table = attached_mode && render_progress_table;
        need_render_profile_events = false;
        print_stack_trace = configuration->getBool("stacktrace", false);

        if (attached_mode)
        {
            /// Use the attached terminal for formatting, not the spool descriptor.
            stdout_is_a_tty = logical_stdout_is_a_tty;
            stderr_is_a_tty = logical_stderr_is_a_tty;
            terminal_width = logical_terminal_width;
            progress_table_toggle_enabled = progress_table_toggle_enabled_;
            progress_table_toggle_on = progress_table_toggle_on_;
            server_logs_file = std::move(server_logs_file_);

            if (foreground_tty_fd >= 0 && (need_render_progress || need_render_progress_table))
                tty_buf = std::make_unique<AutoCanceledWriteBuffer<WriteBufferFromFileDescriptor>>(foreground_tty_fd, 1024);
        }

        /// Load configured defaults before the copied context so later SET values win.
        setDefaultFormatsAndCompressionFromConfiguration();
        initClientContext(std::move(context));
        default_output_format = std::move(default_output_format_);
        is_default_format = is_default_format_;
        default_output_compression_method = default_output_compression_method_;
        has_vertical_output_suffix = has_vertical_output_suffix_;

        if (adopted_connection_)
        {
            connection = std::move(adopted_connection_);
            server_revision = adopted_server_revision_;
            server_version = std::move(adopted_server_version_);
            settings_from_server = assert_cast<Connection &>(*connection).settingsFromServer();
        }
    }

    ~BackgroundClient() override
    {
        /// Destroy the base log router before its derived foreground buffer.
        logs_out_stream.reset();
        out_logs_buf.reset();
        onLogsOutputBufferReset();
    }

    bool execute(const String & query)
    {
        if (isQueryCancellationRequested())
            return false;
        if (!connection)
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
    bool cancellationWasObserved() const
    {
        return cancellation_was_observed || cancelled.load(std::memory_order_acquire)
            || cancellation_requested.load(std::memory_order_acquire);
    }
    bool needsResynchronization() const { return connection_needs_resynchronization; }
    ServerConnectionPtr releaseConnection() { return std::move(connection); }
    void flushResultOutput() { std_out->next(); }
    void snapshotFinalMetrics() { snapshotMetrics(); }
    std::unique_ptr<Exception> cloneServerException() const
    {
        return server_exception ? std::unique_ptr<Exception>(server_exception->clone()) : nullptr;
    }
    std::unique_ptr<Exception> cloneClientException() const
    {
        return client_exception ? std::unique_ptr<Exception>(client_exception->clone()) : nullptr;
    }

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
        /// Attached errors return to Client; detached errors stay spooled for `\fg`.
        if (attached_mode && !detachment_acknowledged->load(std::memory_order_acquire))
            return;

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
        /// Drain attached cancellation to preserve its session; detached jobs may disconnect.
        return requested && (!attached_mode
            || (detachment_acknowledged && detachment_acknowledged->load(std::memory_order_acquire))
            || !cancelled.load(std::memory_order_acquire));
    }
    bool supportsQueryDetachment() const override { return attached_mode; }
    void onQueryProgress(const Progress & value) override { metrics.progress.incrementPiecewiseAtomically(value); }
    void onQueryProfileEvents() override
    {
        snapshotMetrics();
        metrics.cpu_usage_available.store(true, std::memory_order_release);
    }
    void snapshotMetrics()
    {
        const auto memory = progress_indication.getMemoryUsage();
        const auto temporary_data = progress_indication.getTempDataOnDiskUsage();
        metrics.cpu_usage.store(progress_indication.getCPUUsage(), std::memory_order_relaxed);
        metrics.average_cpu_usage.store(progress_indication.getAverageCPUUsage(), std::memory_order_relaxed);
        metrics.memory_usage.store(memory.total, std::memory_order_relaxed);
        metrics.max_host_memory_usage.store(memory.max, std::memory_order_relaxed);
        metrics.peak_memory_usage.store(memory.peak, std::memory_order_relaxed);
        metrics.temporary_data_on_disk.store(temporary_data.total, std::memory_order_relaxed);
        metrics.max_host_temporary_data_on_disk.store(temporary_data.max, std::memory_order_relaxed);
    }
    std::unique_ptr<WriteBuffer> createDefaultLogsOutputBuffer() override
    {
        if (!attached_mode || foreground_stderr_fd < 0)
            return std::make_unique<AutoCanceledWriteBuffer<WriteBufferFromFileDescriptor>>(stderr_fd);

        foreground_logs_output
            = std::make_unique<AutoCanceledWriteBuffer<WriteBufferFromFileDescriptor>>(foreground_stderr_fd);
        auto redirectable_logs = std::make_unique<AutoCanceledWriteBuffer<RedirectableWriteBuffer>>(
            *foreground_logs_output, diagnostic_spool_fd);
        redirectable_logs_output = redirectable_logs.get();
        if (detachment_acknowledged->load(std::memory_order_acquire))
            redirectable_logs_output->detach();
        return redirectable_logs;
    }
    void onLogsOutputBufferReset() override
    {
        redirectable_logs_output = nullptr;
        foreground_logs_output.reset();
    }
    void checkQueryDetachment() override
    {
        if (progress_table_toggle_source)
            progress_table_toggle_on.store(progress_table_toggle_source->load(std::memory_order_acquire), std::memory_order_release);

        if (!attached_mode
            || !detachment_requested->load(std::memory_order_acquire)
            || detachment_acknowledged->load(std::memory_order_acquire))
            return;

        /// No terminal output may survive the acknowledgement: the prompt is
        /// free to redraw as soon as the caller observes it.
        if (tty_buf)
        {
            std::unique_lock lock(tty_mutex);
            if (need_render_progress)
                progress_indication.clearProgressOutput(*tty_buf, lock);
            if (need_render_progress_table)
                progress_table.clearTableOutput(*tty_buf, lock);
            need_render_progress = false;
            need_render_progress_table = false;
            tty_buf->next();
            tty_buf.reset();
        }

        redirectable_output->detach();
        if (redirectable_logs_output)
            redirectable_logs_output->detach();
        redirectable_output_stream->detach();
        redirectable_error_stream->detach();
        progress_table_toggle_source = nullptr;
        detachment_acknowledged->store(true, std::memory_order_release);
        state_changed->notify_all();
    }
    void onOutputFormatSelected(std::string_view format, bool writes_to_stdout) override
    {
        result_format = format;
        result_is_tty_friendly = !writes_to_stdout || FormatFactory::instance().checkIfOutputFormatIsTTYFriendly(result_format);
    }

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> configuration_snapshot;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> configuration;
    const std::atomic_bool & cancellation_requested;
    BackgroundQueryMetrics & metrics;
    RedirectableWriteBuffer * redirectable_output = nullptr;
    const std::atomic_bool * detachment_requested = nullptr;
    std::atomic_bool * detachment_acknowledged = nullptr;
    std::condition_variable * state_changed = nullptr;
    RedirectableStreamBuf * redirectable_output_stream = nullptr;
    RedirectableStreamBuf * redirectable_error_stream = nullptr;
    int diagnostic_spool_fd = -1;
    int foreground_stderr_fd = -1;
    std::unique_ptr<WriteBuffer> foreground_logs_output;
    RedirectableWriteBuffer * redirectable_logs_output = nullptr;
    const std::atomic_bool * progress_table_toggle_source = nullptr;
    String result_format;
    bool result_is_tty_friendly = true;
    bool attached_mode = false;
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

        Job(
            String query_,
            String display_query_,
            String query_id_,
            ContextMutablePtr context_,
            Snapshot snapshot_,
            WriteBuffer & foreground_output_,
            std::ostream & foreground_output_stream_,
            std::ostream & foreground_error_stream_,
            String server_logs_file_,
            UInt64 server_revision_,
            String server_version_,
            bool stdout_is_a_tty_,
            bool stderr_is_a_tty_,
            uint16_t terminal_width_,
            int foreground_stderr_fd_,
            int foreground_tty_fd_,
            bool render_progress_,
            bool render_progress_table_,
            bool progress_table_toggle_enabled_,
            const std::atomic_bool & progress_table_toggle_on_)
            : query(std::move(query_))
            , display_query(std::move(display_query_))
            , query_id(std::move(query_id_))
            , context(std::move(context_))
            , snapshot(std::make_unique<Snapshot>(std::move(snapshot_)))
            , output("result")
            , diagnostics("error")
            , started_at(std::chrono::steady_clock::now())
            , attached(true)
            , foreground_output(&foreground_output_)
            , foreground_output_stream(&foreground_output_stream_)
            , foreground_error_stream(&foreground_error_stream_)
            , server_logs_file(std::move(server_logs_file_))
            , adopted_server_revision(server_revision_)
            , adopted_server_version(std::move(server_version_))
            , logical_stdout_is_a_tty(stdout_is_a_tty_)
            , logical_stderr_is_a_tty(stderr_is_a_tty_)
            , logical_terminal_width(terminal_width_)
            , foreground_stderr_fd(foreground_stderr_fd_)
            , foreground_tty_fd(foreground_tty_fd_)
            , render_progress(render_progress_)
            , render_progress_table(render_progress_table_)
            , progress_table_toggle_enabled(progress_table_toggle_enabled_)
            , progress_table_toggle_source(&progress_table_toggle_on_)
        {
        }

        JobId id = 0;
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
        BackgroundQueryMetrics metrics;
        bool attached = false;
        WriteBuffer * foreground_output = nullptr;
        std::ostream * foreground_output_stream = nullptr;
        std::ostream * foreground_error_stream = nullptr;
        String server_logs_file;
        ServerConnectionPtr adopted_connection;
        UInt64 adopted_server_revision = 0;
        String adopted_server_version;
        bool logical_stdout_is_a_tty = false;
        bool logical_stderr_is_a_tty = false;
        uint16_t logical_terminal_width = 0;
        int foreground_stderr_fd = -1;
        int foreground_tty_fd = -1;
        bool render_progress = false;
        bool render_progress_table = false;
        bool progress_table_toggle_enabled = true;
        const std::atomic_bool * progress_table_toggle_source = nullptr;
        std::atomic_bool detach_requested{false};
        std::atomic_bool detach_acknowledged{false};
        std::condition_variable state_changed;
        std::mutex state_mutex;
        ServerConnectionPtr returned_connection;
        bool returned_connection_needs_resynchronization = false;
        std::unique_ptr<Exception> server_exception;
        std::unique_ptr<Exception> client_exception;
        std::mutex client_mutex;
        BackgroundClient * active_client = nullptr;
        std::thread worker;
    };

    mutable std::mutex mutex;
    std::map<JobId, std::shared_ptr<Job>> jobs;
    std::map<UInt64, std::shared_ptr<Job>> attached_jobs;
    JobId next_id = 1;
    UInt64 next_attached_handle = 1;
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
        result.metrics.progress = job.metrics.progress.getValues();
        result.metrics.cpu_usage_available = job.metrics.cpu_usage_available.load(std::memory_order_acquire);
        result.metrics.cpu_usage_is_average = isTerminal(result.state);
        result.metrics.cpu_usage = result.metrics.cpu_usage_is_average ? job.metrics.average_cpu_usage.load(std::memory_order_relaxed)
                                                                       : job.metrics.cpu_usage.load(std::memory_order_relaxed);
        result.metrics.memory_usage = job.metrics.memory_usage.load(std::memory_order_relaxed);
        result.metrics.max_host_memory_usage = job.metrics.max_host_memory_usage.load(std::memory_order_relaxed);
        result.metrics.peak_memory_usage = job.metrics.peak_memory_usage.load(std::memory_order_relaxed);
        result.metrics.temporary_data_on_disk = job.metrics.temporary_data_on_disk.load(std::memory_order_relaxed);
        result.metrics.max_host_temporary_data_on_disk = job.metrics.max_host_temporary_data_on_disk.load(std::memory_order_relaxed);
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
        job->state_changed.notify_all();
    }

    static void harvestClient(const std::shared_ptr<Job> & job, BackgroundClient & client)
    {
        client.snapshotFinalMetrics();
        job->output_format = client.resultFormat();
        job->output_is_tty_friendly = client.resultIsTTYFriendly();
        job->cancellation_was_observed = client.cancellationWasObserved();

        if (job->attached)
        {
            job->server_exception = client.cloneServerException();
            job->client_exception = client.cloneClientException();

            /// Only a query completed before detach may return its session.
            if (!job->detach_acknowledged.load(std::memory_order_acquire))
            {
                job->returned_connection_needs_resynchronization = client.needsResynchronization();
                job->returned_connection = client.releaseConnection();
            }
        }
    }

    static void run(const std::shared_ptr<Job> & job) noexcept
    {
        std::unique_ptr<RedirectableStreamBuf> output_stream_buffer;
        std::unique_ptr<RedirectableStreamBuf> error_stream_buffer;
        std::unique_ptr<std::ostream> attached_output_stream;
        std::unique_ptr<std::ostream> attached_error_stream;
        std::unique_ptr<BackgroundClient> client;
        try
        {
            /// Move connection secrets out before any worker early exit can retain them.
            auto snapshot = std::move(job->snapshot);
            auto context = std::move(job->context);
            auto query = std::move(job->query);

            ThreadStatus thread_status;
            setThreadName(ThreadName::BACKGROUND_QUERY);
            QueryScope query_scope = QueryScope::create(context);

            if (job->cancel_requested.load(std::memory_order_acquire))
            {
                if (job->attached)
                    job->returned_connection = std::move(job->adopted_connection);
                finish(job, State::Cancelled);
                return;
            }

            const int null_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
            if (null_fd < 0)
                throw std::system_error(errno, std::generic_category(), "Cannot open /dev/null for a background query");
            FileDescriptor input_descriptor(null_fd);
            std::ifstream input_stream("/dev/null");

            std::unique_ptr<WriteBuffer> attached_output;
            RedirectableWriteBuffer * redirectable_output = nullptr;
            std::ostream * client_output_stream = &job->output.stream();
            std::ostream * client_error_stream = &job->diagnostics.stream();
            int client_input_fd = input_descriptor.get();
            int client_error_fd = job->diagnostics.getFD();
            if (job->attached)
            {
                auto redirectable = std::make_unique<AutoCanceledWriteBuffer<RedirectableWriteBuffer>>(
                    *job->foreground_output, job->output.getFD());
                redirectable_output = redirectable.get();
                attached_output = std::move(redirectable);

                output_stream_buffer
                    = std::make_unique<RedirectableStreamBuf>(*job->foreground_output_stream, job->output.stream());
                error_stream_buffer
                    = std::make_unique<RedirectableStreamBuf>(*job->foreground_error_stream, job->diagnostics.stream());
                attached_output_stream = std::make_unique<std::ostream>(output_stream_buffer.get());
                attached_error_stream = std::make_unique<std::ostream>(error_stream_buffer.get());
                attached_output_stream->flags(job->foreground_output_stream->flags());
                attached_output_stream->precision(job->foreground_output_stream->precision());
                attached_output_stream->fill(job->foreground_output_stream->fill());
                attached_output_stream->imbue(job->foreground_output_stream->getloc());
                attached_error_stream->flags(job->foreground_error_stream->flags());
                attached_error_stream->precision(job->foreground_error_stream->precision());
                attached_error_stream->fill(job->foreground_error_stream->fill());
                attached_error_stream->imbue(job->foreground_error_stream->getloc());
                client_output_stream = attached_output_stream.get();
                client_error_stream = attached_error_stream.get();

                /// Progress helpers capture this descriptor but eligible SELECTs never read it.
                if (job->foreground_tty_fd >= 0)
                {
                    client_input_fd = job->foreground_tty_fd;
                    client_error_fd = job->foreground_tty_fd;
                }
            }

            client = std::make_unique<BackgroundClient>(
                client_input_fd,
                job->output.getFD(),
                client_error_fd,
                input_stream,
                *client_output_stream,
                *client_error_stream,
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
                job->cancel_requested,
                job->metrics,
                std::move(attached_output),
                redirectable_output,
                std::move(job->adopted_connection),
                job->adopted_server_revision,
                std::move(job->adopted_server_version),
                job->attached ? &job->detach_requested : nullptr,
                job->attached ? &job->detach_acknowledged : nullptr,
                job->attached ? &job->state_changed : nullptr,
                output_stream_buffer.get(),
                error_stream_buffer.get(),
                job->diagnostics.getFD(),
                job->foreground_stderr_fd,
                std::move(job->server_logs_file),
                job->progress_table_toggle_source,
                job->logical_stdout_is_a_tty,
                job->logical_stderr_is_a_tty,
                job->logical_terminal_width,
                job->foreground_tty_fd,
                job->render_progress,
                job->render_progress_table,
                job->progress_table_toggle_enabled,
                job->progress_table_toggle_source ? job->progress_table_toggle_source->load(std::memory_order_acquire) : false);
            snapshot.reset();

            {
                std::lock_guard lock(job->client_mutex);
                if (job->cancel_requested.load(std::memory_order_acquire))
                {
                    job->output_format = client->resultFormat();
                    job->output_is_tty_friendly = client->resultIsTTYFriendly();
                    job->cancellation_was_observed = true;
                    if (job->attached)
                        job->returned_connection = client->releaseConnection();
                    client.reset();
                    finish(job, State::Cancelled);
                    return;
                }
                job->active_client = client.get();
                job->state.store(State::Running, std::memory_order_release);
                job->state_changed.notify_all();
            }

            SCOPE_EXIT({
                std::lock_guard lock(job->client_mutex);
                job->active_client = nullptr;
            });

            const bool succeeded = client->execute(query);
            String error = client->errorMessage();
            client->flushResultOutput();
            harvestClient(job, *client);

            {
                std::lock_guard lock(job->client_mutex);
                job->active_client = nullptr;
            }
            client.reset();
            job->output.flush();
            job->diagnostics.flush();

            if (job->cancellation_was_observed)
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

                if (client)
                {
                    try
                    {
                        client->flushResultOutput();
                    }
                    catch (...)
                    {
                    }
                    harvestClient(job, *client);
                }
                else if (job->attached
                    && !job->detach_acknowledged.load(std::memory_order_acquire)
                    && job->adopted_connection)
                {
                    job->returned_connection = std::move(job->adopted_connection);
                }

                {
                    std::lock_guard lock(job->client_mutex);
                    job->active_client = nullptr;
                }
                client.reset();
                if (job->attached && !job->server_exception && !job->client_exception)
                    job->client_exception
                        = std::make_unique<Exception>(Exception::createRuntime(getCurrentExceptionCode(), error));

                if (!job->attached || job->detach_acknowledged.load(std::memory_order_acquire))
                {
                    job->diagnostics.stream() << error << '\n';
                    job->diagnostics.flush();
                }
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
                job->state_changed.notify_all();
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
    /// Copy only presentation options; the full config can contain credentials.
    static constexpr std::array<std::string_view, 17> copied_configuration_keys{
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
        "chime-threshold-seconds",
        "enable-progress-table-toggle",
        "print-profile-events",
        "profile-events-delay-ms",
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

BackgroundQueryManager::AttachedHandle BackgroundQueryManager::startAttached(
    String query,
    String display_query,
    Snapshot snapshot,
    ServerConnectionPtr & connection,
    WriteBuffer & foreground_output,
    std::ostream & foreground_output_stream,
    std::ostream & foreground_error_stream,
    String server_logs_file,
    UInt64 server_revision,
    String server_version,
    bool stdout_is_a_tty,
    bool stderr_is_a_tty,
    uint16_t terminal_width,
    int foreground_stderr_fd,
    int foreground_tty_fd,
    bool render_progress,
    bool render_progress_table,
    bool progress_table_toggle_enabled,
    const std::atomic_bool & progress_table_toggle_on)
{
    if (!snapshot.context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An attached query requires a client context");
    if (!connection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An attached query requires an established server connection");

    /// Preserve the foreground query ID while isolating later SET/USE changes.
    auto context = Context::createCopy(snapshot.context);
    snapshot.context.reset();
    String query_id = context->getCurrentQueryId();

    std::shared_ptr<Impl::Job> job;
    UInt64 handle_value = 0;
    {
        std::lock_guard lock(impl->mutex);
        if (impl->shutting_down)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Background-query manager is shutting down");

        handle_value = impl->next_attached_handle++;
        job = std::make_shared<Impl::Job>(
            std::move(query),
            std::move(display_query),
            std::move(query_id),
            std::move(context),
            std::move(snapshot),
            foreground_output,
            foreground_output_stream,
            foreground_error_stream,
            std::move(server_logs_file),
            server_revision,
            std::move(server_version),
            stdout_is_a_tty,
            stderr_is_a_tty,
            terminal_width,
            foreground_stderr_fd,
            foreground_tty_fd,
            render_progress,
            render_progress_table,
            progress_table_toggle_enabled,
            progress_table_toggle_on);

        /// Publish the handle before the no-throw connection transfer.
        impl->attached_jobs.emplace(handle_value, job);
        job->adopted_connection = std::move(connection);
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
            impl->attached_jobs.erase(handle_value);
            connection = std::move(job->adopted_connection);
        }
        throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot start attached-query worker: {}", error);
    }

    return AttachedHandle(handle_value);
}

BackgroundQueryManager::AttachedWaitStatus BackgroundQueryManager::waitAttached(
    AttachedHandle handle, std::chrono::milliseconds timeout) const
{
    std::shared_ptr<Impl::Job> job;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->attached_jobs.find(handle.value);
        if (it == impl->attached_jobs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown attached-query handle");
        job = it->second;
    }

    auto ready = [&]
    {
        return job->detach_acknowledged.load(std::memory_order_acquire)
            || isTerminal(job->state.load(std::memory_order_acquire));
    };

    if (!ready() && timeout > std::chrono::milliseconds::zero())
    {
        std::unique_lock lock(job->state_mutex);
        job->state_changed.wait_for(lock, timeout, ready);
    }

    /// Load state first so its release sequence also exposes an earlier detach ack.
    const auto state = job->state.load(std::memory_order_acquire);
    if (job->detach_acknowledged.load(std::memory_order_acquire))
        return AttachedWaitStatus::DetachAcknowledged;
    if (isTerminal(state))
        return AttachedWaitStatus::Terminal;
    return AttachedWaitStatus::Running;
}

void BackgroundQueryManager::requestDetach(AttachedHandle handle)
{
    std::shared_ptr<Impl::Job> job;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->attached_jobs.find(handle.value);
        if (it == impl->attached_jobs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown attached-query handle");
        job = it->second;
    }

    if (isTerminal(job->state.load(std::memory_order_acquire)))
        return;

    job->detach_requested.store(true, std::memory_order_release);
}

std::optional<BackgroundQueryManager::JobId> BackgroundQueryManager::promoteDetached(AttachedHandle handle)
{
    std::lock_guard lock(impl->mutex);
    auto it = impl->attached_jobs.find(handle.value);
    if (it == impl->attached_jobs.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown attached-query handle");

    const auto & job = it->second;
    if (!job->detach_acknowledged.load(std::memory_order_acquire))
        return {};

    const JobId id = impl->next_id;
    impl->jobs.emplace(id, job);
    ++impl->next_id;
    job->id = id;
    impl->attached_jobs.erase(it);
    return id;
}

bool BackgroundQueryManager::cancelAttached(AttachedHandle handle)
{
    std::shared_ptr<Impl::Job> job;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->attached_jobs.find(handle.value);
        if (it == impl->attached_jobs.end())
            return false;
        job = it->second;
        if (isTerminal(job->state.load(std::memory_order_acquire)))
            return false;
    }

    Impl::requestCancellation(job);
    return true;
}

void BackgroundQueryManager::discardAttached(AttachedHandle handle) noexcept
{
    std::shared_ptr<Impl::Job> job;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->attached_jobs.find(handle.value);
        if (it == impl->attached_jobs.end())
            return;
        job = it->second;
    }

    /// An exceptional caller abandons the session, so detach before cancellation.
    job->detach_requested.store(true, std::memory_order_release);
    Impl::requestCancellation(job);
    if (job->worker.joinable())
        job->worker.join();

    std::lock_guard lock(impl->mutex);
    auto it = impl->attached_jobs.find(handle.value);
    if (it != impl->attached_jobs.end() && it->second == job)
        impl->attached_jobs.erase(it);
}

BackgroundQueryManager::AttachedResult BackgroundQueryManager::collectAttached(
    AttachedHandle handle, WriteBuffer & output, WriteBuffer & diagnostics)
{
    std::shared_ptr<Impl::Job> job;
    JobInfo job_info;
    {
        std::lock_guard lock(impl->mutex);
        auto it = impl->attached_jobs.find(handle.value);
        if (it == impl->attached_jobs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown attached-query handle");

        job = it->second;
        if (!isTerminal(job->state.load(std::memory_order_acquire)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot collect an attached query before it is terminal");

        job_info = Impl::info(*job);
        impl->attached_jobs.erase(it);
    }

    if (job->worker.joinable())
        job->worker.join();

    /// Query rows have already gone directly to `output`. The result spool for
    /// a never-detached query contains only ClientBase's interactive summary.
    job->output.replayTo(output);
    output.next();
    job->diagnostics.replayTo(diagnostics);
    diagnostics.next();

    AttachedResult result;
    result.connection = std::move(job->returned_connection);
    if (!result.connection)
        result.connection = std::move(job->adopted_connection);
    result.connection_needs_resynchronization = job->returned_connection_needs_resynchronization;
    result.server_exception = std::move(job->server_exception);
    result.client_exception = std::move(job->client_exception);
    result.job = std::move(job_info);
    return result;
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
        if (impl->shutting_down && impl->jobs.empty() && impl->attached_jobs.empty())
            return;
        impl->shutting_down = true;
        jobs.reserve(impl->jobs.size() + impl->attached_jobs.size());
        for (const auto & [_, job] : impl->jobs)
            jobs.push_back(job);
        for (const auto & [_, job] : impl->attached_jobs)
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
    impl->attached_jobs.clear();
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
