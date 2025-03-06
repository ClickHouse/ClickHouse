#include "Loggers.h"

#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/OwnJSONPatternFormatter.h>
#include <Loggers/TextLogSink.h>

#include <atomic>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>

#include <IO/ReadHelpers.h>
#include <quill/core/LogLevel.h>
#include <quill/sinks/Sink.h>
#include <Common/QuillLoggerHelper.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/LockMemoryExceptionInThread.h>

#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/RemoteSyslogChannel.h>
#include <Poco/SyslogChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
#define POCO_UNBUNDLED_ZLIB 1
#include <Poco/DeflatingStream.h>
#undef POCO_UNBUNDLED_ZLIB
#include <Poco/StreamCopier.h>
#include <Poco/FileStream.h>
#include <Poco/File.h>


#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>
#include <quill/sinks/FileSink.h>
#include <quill/sinks/StreamSink.h>

#ifndef WITHOUT_TEXT_LOG
    #include <Interpreters/TextLog.h>
#endif

#include <filesystem>
#include <future>

namespace fs = std::filesystem;

namespace DB
{
    class SensitiveDataMasker;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace CurrentMetrics
{
    extern const Metric LogCompressionThreads;
    extern const Metric LogCompressionActiveThreads;
    extern const Metric LogCompressionScheduledThreads;
}

namespace
{
bool renameFile(const fs::path & previous_file, const fs::path & new_file) noexcept
{
    std::error_code ec;
    fs::rename(previous_file, new_file, ec);
    return ec.value() == 0;
}

bool removeFile(const fs::path & filename) noexcept
{
    std::error_code ec;
    fs::remove(filename, ec);
    return ec.value() == 0;
}

bool compressLog(const std::string & path, const std::string & compressed_path)
{
    Poco::FileInputStream istr(path);
    Poco::FileOutputStream ostr(compressed_path);
    try
    {
        Poco::DeflatingOutputStream deflater(ostr, Poco::DeflatingStreamBuf::STREAM_GZIP);
        Poco::StreamCopier::copyStream(istr, static_cast<std::ostream &>(deflater));
        if (!deflater.good() || !ostr.good()) throw Poco::WriteFileException(compressed_path);
        deflater.close();
        ostr.close();
        istr.close();
    }
    catch (...)
    {
        // deflating failed - remove gz file and leave uncompressed log file
        ostr.close();
        Poco::File gzf(compressed_path);
        gzf.remove();
        return false;
    }
    Poco::File f(path);
    f.remove();
    return true;
}

class RotatingSinkConfiguration : public quill::FileSinkConfig
{
public:
    size_t max_file_size = 0;
    size_t max_backup_files = 0;
    bool compress = false;
    std::optional<quill::LogLevel> log_level_filter;
    bool rotate_on_open = false;
};


// TODO: move to libcommon
std::string createDirectory(const std::string & file)
{
    auto path = fs::path(file).parent_path();
    if (path.empty())
        return "";
    fs::create_directories(path);
    return path;
}

std::string renderFileNameTemplate(time_t now, const std::string & file_path)
{
    fs::path path{file_path};
    std::tm buf;
    localtime_r(&now, &buf); /// NOLINT(cert-err33-c)
    std::ostringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss << std::put_time(&buf, path.filename().c_str());
    return path.replace_filename(ss.str());
}

size_t getLogFileMaxSize(const std::string & max_size_str)
{
    if (max_size_str == "never")
        return 0;

    std::string_view max_size_view{max_size_str};
    size_t multiplier = 1;
    if (max_size_view.ends_with('M'))
    {
        max_size_view.remove_suffix(1);
        multiplier = 1_MiB;
    }

    return DB::parse<size_t>(max_size_view) * multiplier;
}

}


class RotatingFileSink : public quill::FileSink
{
    using Base = quill::FileSink;
public:
    explicit RotatingFileSink(const fs::path & filename, RotatingSinkConfiguration config_)
        : Base(filename, static_cast<const quill::FileSinkConfig &>(config_), quill::FileEventNotifier{}, /*do_fopen=*/false)
        , config(config_)
    {
        recoverFiles();

        this->open_file(this->_filename, "a");
        created_files.emplace_front(this->_filename, 0);

        if (!this->is_null())
            current_log_size = fs::file_size(this->_filename);

        if (config.compress)
            pool.emplace(
                CurrentMetrics::LogCompressionThreads,
                CurrentMetrics::LogCompressionActiveThreads,
                CurrentMetrics::LogCompressionScheduledThreads,
                /*max_threads_=*/1);

        if (config.rotate_on_open)
            rotateFiles();
    }

    QUILL_ATTRIBUTE_HOT void write_log(
        quill::MacroMetadata const * log_metadata,
        uint64_t log_timestamp,
        std::string_view thread_id,
        std::string_view thread_name,
        std::string const & process_id,
        std::string_view logger_name,
        quill::LogLevel log_level,
        std::string_view log_level_description,
        std::string_view log_level_short_code,
        std::vector<std::pair<std::string, std::string>> const * named_args,
        std::string_view log_message,
        std::string_view log_statement) override
    {
        if (config.log_level_filter && log_level < *config.log_level_filter)
            return;

        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
        if (this->is_null())
        {
            Base::write_log(
                log_metadata,
                log_timestamp,
                thread_id,
                thread_name,
                process_id,
                logger_name,
                log_level,
                log_level_description,
                log_level_short_code,
                named_args,
                log_message,
                log_statement);
            return;
        }

        if (config.max_file_size && current_log_size + log_statement.size() > config.max_file_size)
            rotateFiles();

        Base::write_log(
            log_metadata,
            log_timestamp,
            thread_id,
            thread_name,
            process_id,
            logger_name,
            log_level,
            log_level_description,
            log_level_short_code,
            named_args,
            log_message,
            log_statement);

        current_log_size += log_statement.size();
    }

    QUILL_ATTRIBUTE_HOT void flush_sink() override
    {
        if (close_current_file.exchange(false, std::memory_order_relaxed))
            rotateFiles();

        Base::flush_sink();
    }

    void closeFile()
    {
        close_current_file.store(true, std::memory_order_relaxed);
    }
private:
    void recoverFiles()
    {
        for (const auto & entry : fs::directory_iterator(fs::current_path() / this->_filename.parent_path()))
        {
            if (entry.is_directory())
                continue;

            const auto extension = entry.path().extension().string();

            if (extension == ".gz")
            {
                if (!entry.path().filename().string().starts_with(this->_filename.filename().string() + "."))
                    continue;
            }
            else if (extension == this->_filename.extension().string())
            {
                if (!entry.path().filename().string().starts_with(this->_filename.stem().string() + "."))
                    continue;
            }
            else
            {
                continue;
            }

            if (const size_t pos = entry.path().stem().string().find_last_of('.'); pos != std::string::npos)
            {
                const std::string index = entry.path().stem().string().substr(pos + 1, entry.path().stem().string().length());
                std::string current_filename = entry.path().filename().string().substr(0, pos) + extension;
                fs::path current_file = entry.path().parent_path();
                current_file.append(current_filename);

                try
                {
                    created_files.emplace_front(current_file, static_cast<size_t>(DB::parseFromString<size_t>(index) + 1));
                }
                catch (...) /// NOLINT(bugprone-empty-catch)
                {
                    /// this not the file we are looking for
                }
            }
        }

        // finally we need to sort the deque
        std::sort(created_files.begin(), created_files.end(), [](FileInfo const & a, FileInfo const & b) { return a.index < b.index; });
    }

    void rotateFiles()
    {
        if (!current_log_size)
            return;

        Base::flush_sink();
        Base::fsync_file(/*force_fsync=*/true);

        this->close_file();

        for (auto it = created_files.rbegin(); it != created_files.rend(); ++it)
        {
            if (config.compress && it->index == 1)
            {
                if (compression_result.has_value())
                {
                    try
                    {
                        if (compression_result->get())
                            it->base_filename += ".gz";
                    }
                    catch (...)
                    {
                        std::cerr << "Failed to fetch compression result: " << DB::getCurrentExceptionMessage(true) << std::endl;
                    }
                    compression_result.reset();
                }
            }

            fs::path existing_file = getFilename(it->base_filename, it->index);

            // increment the index if needed and rename the file
            uint32_t index_to_use = it->index + 1;
            fs::path renamed_file = getFilename(it->base_filename, index_to_use);
            it->index = index_to_use;
            renameFile(existing_file, renamed_file);

            if (config.compress && it->index == 1)
            {
                chassert(!compression_result.has_value());

                auto compression_result_promise = std::make_shared<std::promise<bool>>();
                compression_result.emplace(compression_result_promise->get_future());
                auto compressed_path = getFilename(existing_file.string() + ".gz", it->index);
                try
                {
                    pool->scheduleOrThrowOnError(
                        [current_path = std::move(renamed_file),
                         new_path = std::move(compressed_path),
                         compression_result_promise]() mutable
                        {
                            try
                            {
                                compression_result_promise->set_value(compressLog(current_path, new_path));
                            }
                            catch (...)
                            {
                                std::cerr << "Failed to compress log: " << DB::getCurrentExceptionMessage(true) << std::endl;
                                compression_result_promise->set_exception(std::current_exception());
                            }
                        });
                }
                catch (...)
                {
                    compression_result.reset();
                    compression_result_promise.reset();
                    std::cerr << "Failed to schedule thread for log compression: " << DB::getCurrentExceptionMessage(true) << std::endl;
                }
            }
        }

        // Check if we have too many files in the queue remove_file the oldest one
        if (created_files.size() > config.max_backup_files)
        {
            // remove_file that file from the system and also pop it from the queue
            const fs::path removed_file = getFilename(
                created_files.back().base_filename, created_files.back().index);
            removeFile(removed_file);
            created_files.pop_back();
        }

        // add the current file back to the list with index 0
        created_files.emplace_front(this->_filename, 0);

        // Open file for logging
        this->open_file(this->_filename, "w");
        current_log_size = 0;
    }

    static fs::path appendIndexToFilename(const fs::path & filename, size_t index)
    {
        if (index == 0)
            return filename;

        auto const [stem, ext] = Base::extract_stem_and_extension(filename);
        return fs::path{stem + "." + std::to_string(index - 1) + ext};
    }

    static fs::path getFilename(fs::path base_filename, size_t index)
    {
        if (index > 0)
            return appendIndexToFilename(base_filename, index);
        return base_filename;
    }

    struct FileInfo
    {
        FileInfo(fs::path base_filename_, uint32_t index_)
            : base_filename{std::move(base_filename_)}, index{index_}
        {
        }

        fs::path base_filename;
        uint32_t index;
    };

    std::deque<FileInfo> created_files;
    size_t current_log_size{0};
    RotatingSinkConfiguration config;

    std::optional<FreeThreadPool> pool;
    std::optional<std::future<bool>> compression_result;

    std::atomic<bool> close_current_file{false};
};

/// NOLINTBEGIN(readability-static-accessed-through-instance)

void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & /*_root*/, const std::string & /*cmd_name*/)
{
    quill::Backend::start();
    std::vector<std::shared_ptr<quill::Sink>> sinks;

    auto current_logger = config.getString("logger", "");
    if (config_logger.has_value())// && *config_logger == current_logger)
        return;

    config_logger = current_logger;

    bool is_daemon = config.getBool("application.runAsDaemon", false);

    auto log_level_string = config.getString("logger.level", "trace");

    /// different channels (log, console, syslog) may have different loglevels configured
    /// The maximum (the most verbose) of those will be used as default for Poco loggers
    quill::LogLevel min_log_level = quill::LogLevel::None;

    time_t now = std::time({});

    const auto log_path_prop = config.getString("logger.log", "");
    if (!log_path_prop.empty())
    {
        const auto log_path = renderFileNameTemplate(now, log_path_prop);
        createDirectory(log_path);

        std::cerr << "Logging " << log_level_string << " to " << log_path << std::endl;

        auto log_level = DB::parseQuillLogLevel(log_level_string);
        min_log_level = std::min(log_level, min_log_level);

        RotatingSinkConfiguration file_config;
        file_config.compress = config.getBool("logger.compress", true);
        file_config.max_backup_files = config.getUInt64("logger.count", 1);
        file_config.max_file_size = getLogFileMaxSize(config.getString("logger.size", "100M")); /// TODO: support readable size like 100M
        file_config.rotate_on_open = config.getBool("logger.rotateOnOpen", false);

        auto & sink = sinks.emplace_back(quill::Frontend::create_or_get_sink<RotatingFileSink>(fs::weakly_canonical(log_path), file_config));

        log_file = std::static_pointer_cast<RotatingFileSink>(sink);
    }

    const auto errorlog_path_prop = config.getString("logger.errorlog", "");
    if (!errorlog_path_prop.empty())
    {
        const auto errorlog_path = renderFileNameTemplate(now, errorlog_path_prop);
        createDirectory(errorlog_path);

        // NOTE: we don't use notice & critical in the code, so in practice error log collects fatal & error & warning.
        // (!) Warnings are important, they require attention and should never be silenced / ignored.
        auto errorlog_level = DB::parseQuillLogLevel(config.getString("logger.errorlog_level", "notice"));
        min_log_level = std::min(errorlog_level, min_log_level);

        std::string ext;
        if (config.getRawString("logger.stream_compress", "false") == "true")
            ext = ".lz4";

        std::cerr << "Logging errors to " << errorlog_path << ext << std::endl;

        RotatingSinkConfiguration file_config;
        file_config.compress = config.getBool("logger.compress", true);
        file_config.max_backup_files = config.getUInt64("logger.count", 1);
        file_config.max_file_size = getLogFileMaxSize(config.getString("logger.size", "100M")); /// TODO: support readable size like 100M

        auto & sink = sinks.emplace_back(quill::Frontend::create_or_get_sink<RotatingFileSink>(fs::weakly_canonical(errorlog_path), file_config));
        sink->set_log_level_filter(errorlog_level);
        error_log_file = std::static_pointer_cast<RotatingFileSink>(sink);
    }

    // if (config.getBool("logger.use_syslog", false))
    // {
    //     auto syslog_level = Poco::Logger::parseLevel(config.getString("logger.syslog_level", log_level_string));
    //     max_log_level = std::max(syslog_level, max_log_level);

    //     if (config.has("logger.syslog.address"))
    //     {
    //         syslog_channel = new Poco::Net::RemoteSyslogChannel();
    //         // syslog address
    //         syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_LOGHOST, config.getString("logger.syslog.address"));
    //         if (config.has("logger.syslog.hostname"))
    //         {
    //             syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_HOST, config.getString("logger.syslog.hostname"));
    //         }
    //         syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_FORMAT, config.getString("logger.syslog.format", "syslog"));
    //         syslog_channel->setProperty(
    //             Poco::Net::RemoteSyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_USER"));
    //     }
    //     else
    //     {
    //         syslog_channel = new Poco::SyslogChannel();
    //         syslog_channel->setProperty(Poco::SyslogChannel::PROP_NAME, cmd_name);
    //         syslog_channel->setProperty(Poco::SyslogChannel::PROP_OPTIONS, config.getString("logger.syslog.options", "LOG_CONS|LOG_PID"));
    //         syslog_channel->setProperty(Poco::SyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_DAEMON"));
    //     }
    //     syslog_channel->open();

    //     Poco::AutoPtr<OwnPatternFormatter> pf;

    //     if (config.getString("logger.formatting.type", "") == "json")
    //         pf = new OwnJSONPatternFormatter(config);
    //     else
    //         pf = new OwnPatternFormatter;

    //     Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, syslog_channel);
    //     log->setLevel(syslog_level);

    //     split->addChannel(log, "syslog");
    // }

    bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    // bool color_logs_by_default = isatty(STDERR_FILENO);

    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
    {
        auto console_log_level_string = config.getString("logger.console_log_level", log_level_string);
        auto console_log_level = DB::parseQuillLogLevel(console_log_level_string);
        min_log_level = std::min(console_log_level, min_log_level);
        auto & sink = sinks.emplace_back(quill::Frontend::create_or_get_sink<quill::ConsoleSink>("ConsoleSink", quill::ConsoleSink::ColourMode::Never, /*stream=*/"stderr"));
        sink->set_log_level_filter(console_log_level);
    }

    if (config.getString("logger.formatting.type", "") == "json")
        Logger::setFormatter(std::make_unique<OwnJSONPatternFormatter>(config));
    else
        Logger::setFormatter(std::make_unique<OwnPatternFormatter>());


    auto logger = createLogger("root", sinks);
    logger->getQuillLogger()->set_log_level(min_log_level);

    // // Set level and channel to all already created loggers
    // std::vector<std::string> names;
    // logger.names(names);

    // for (const auto & name : names)
    // {
    //     logger.get(name).setLevel(max_log_level);
    //     logger.get(name).setChannel(split);
    // }

    // // Explicitly specified log levels for specific loggers.
    // {
    //     Poco::Util::AbstractConfiguration::Keys loggers_level;
    //     config.keys("logger.levels", loggers_level);

    //     if (!loggers_level.empty())
    //     {
    //         for (const auto & key : loggers_level)
    //         {
    //             if (key == "logger" || key.starts_with("logger["))
    //             {
    //                 const std::string name(config.getString("logger.levels." + key + ".name"));
    //                 const std::string level(config.getString("logger.levels." + key + ".level"));
    //                 logger.root().get(name).setLevel(level);
    //             }
    //             else
    //             {
    //                 // Legacy syntax
    //                 const std::string level(config.getString("logger.levels." + key, "trace"));
    //                 logger.root().get(key).setLevel(level);
    //             }
    //         }
    //     }
    // }
#ifndef WITHOUT_TEXT_LOG
    if (allowTextLog() && config.has("text_log"))
    {
        String text_log_level_str = config.getString("text_log.level", "trace");
        int text_log_level = Poco::Logger::parseLevel(text_log_level_str);

        DB::SystemLogQueueSettings log_settings;
        log_settings.flush_interval_milliseconds = config.getUInt64("text_log.flush_interval_milliseconds",
                                                                    DB::TextLog::getDefaultFlushIntervalMilliseconds());

        log_settings.max_size_rows = config.getUInt64("text_log.max_size_rows",
                                                      DB::TextLog::getDefaultMaxSize());

        if (log_settings.max_size_rows< 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "text_log.max_size_rows {} should be 1 at least",
                                log_settings.max_size_rows);

        log_settings.reserved_size_rows = config.getUInt64("text_log.reserved_size_rows", DB::TextLog::getDefaultReservedSize());

        if (log_settings.max_size_rows < log_settings.reserved_size_rows)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                                "text_log.max_size {0} should be greater or equal to text_log.reserved_size_rows {1}",
                                log_settings.max_size_rows,
                                log_settings.reserved_size_rows);
        }

        log_settings.buffer_size_rows_flush_threshold = config.getUInt64("text_log.buffer_size_rows_flush_threshold",
                                                                         log_settings.max_size_rows / 2);

        log_settings.notify_flush_on_crash = config.getBool("text_log.flush_on_crash",
                                                            DB::TextLog::shouldNotifyFlushOnCrash());

        log_settings.turn_off_logger = DB::TextLog::shouldTurnOffLogger();

        log_settings.database = config.getString("text_log.database", "system");
        log_settings.table = config.getString("text_log.table", "text_log");

        Logger::getTextLogSink().addTextLog(DB::TextLog::getLogQueue(log_settings), text_log_level);
    }
#endif
}

void Loggers::updateLevels(Poco::Util::AbstractConfiguration & config, Poco::Logger &)
{
    //int max_log_level = 0;

    const auto log_level_string = config.getString("logger.level", "trace");
    auto quill_level = DB::parseQuillLogLevel(log_level_string);
    for (auto * quill_logger : quill::Frontend::get_all_loggers())
    {
        quill_logger->set_log_level(quill_level);
    }

    // int log_level = Poco::Logger::parseLevel(log_level_string);
    // max_log_level = std::max(log_level, max_log_level);

    // if (log_file)
    //     split->setLevel("log", log_level);

    // // Set level to console
    // bool is_daemon = config.getBool("application.runAsDaemon", false);
    // bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    // if (config.getBool("logger.console", false)
    //     || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
    // {
    //     auto console_log_level_string = config.getString("logger.console_log_level", log_level_string);
    //     auto console_log_level = Poco::Logger::parseLevel(console_log_level_string);
    //     max_log_level = std::max(console_log_level, max_log_level);
    //     split->setLevel("console", console_log_level);
    // }
    // else
    //     split->setLevel("console", 0);

    // // Set level to errorlog
    // if (error_log_file)
    // {
    //     int errorlog_level = Poco::Logger::parseLevel(config.getString("logger.errorlog_level", "notice"));
    //     max_log_level = std::max(errorlog_level, max_log_level);
    //     split->setLevel("errorlog", errorlog_level);
    // }

    // // Set level to syslog
    // int syslog_level = 0;
    // if (config.getBool("logger.use_syslog", false))
    // {
    //     syslog_level = Poco::Logger::parseLevel(config.getString("logger.syslog_level", log_level_string));
    //     max_log_level = std::max(syslog_level, max_log_level);
    // }
    // split->setLevel("syslog", syslog_level);

    // // Global logging level (it can be overridden for specific loggers).
    // logger.setLevel(max_log_level);

    // // Set level to all already created loggers
    // std::vector<std::string> names;

    // logger.root().names(names);
    // for (const auto & name : names)
    //     logger.root().get(name).setLevel(max_log_level);

    // logger.root().setLevel(max_log_level);

    // // Explicitly specified log levels for specific loggers.
    // {
    //     Poco::Util::AbstractConfiguration::Keys loggers_level;
    //     config.keys("logger.levels", loggers_level);

    //     if (!loggers_level.empty())
    //     {
    //         for (const auto & key : loggers_level)
    //         {
    //             if (key == "logger" || key.starts_with("logger["))
    //             {
    //                 const std::string name(config.getString("logger.levels." + key + ".name"));
    //                 const std::string level(config.getString("logger.levels." + key + ".level"));
    //                 logger.root().get(name).setLevel(level);
    //             }
    //             else
    //             {
    //                 // Legacy syntax
    //                 const std::string level(config.getString("logger.levels." + key, "trace"));
    //                 logger.root().get(key).setLevel(level);
    //             }
    //         }
    //     }
    // }
}

/// NOLINTEND(readability-static-accessed-through-instance)

void Loggers::closeLogs(Poco::Logger & logger)
{
    if (log_file)
        log_file->closeFile();
    if (error_log_file)
        error_log_file->closeFile();
    // Shouldn't syslog_channel be closed here too?

    if (!log_file)
        logger.warning("Logging to console but received signal to close log file (ignoring).");
}
