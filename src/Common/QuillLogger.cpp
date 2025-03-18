#include "Common/Arena.h"
#include <Common/QuillLogger.h>

#include <boost/algorithm/string/predicate.hpp>
#include <quill/core/Common.h>
#include <quill/Backend.h>

#define POCO_UNBUNDLED_ZLIB 1
#include <Poco/DeflatingStream.h>
#undef POCO_UNBUNDLED_ZLIB
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>

#include <base/terminalColors.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ThreadPool.h>
#include <Common/typeid_cast.h>
#include <Loggers/OwnPatternFormatter.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace CurrentMetrics
{
extern const Metric LogCompressionThreads;
extern const Metric LogCompressionActiveThreads;
extern const Metric LogCompressionScheduledThreads;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

void startQuillBackend()
{
    quill::Backend::start();
}

quill::LogLevel parseQuillLogLevel(std::string_view level)
{
    if (boost::iequals(level, "none"))
        return quill::LogLevel::None;
    else if (boost::iequals(level, "fatal"))
        return quill::LogLevel::Critical;
    else if (boost::iequals(level, "critical"))
        return quill::LogLevel::Critical;
    else if (boost::iequals(level, "error"))
        return quill::LogLevel::Error;
    else if (boost::iequals(level, "warning"))
        return quill::LogLevel::Warning;
    else if (boost::iequals(level, "notice"))
        return quill::LogLevel::Notice;
    else if (boost::iequals(level, "information"))
        return quill::LogLevel::Info;
    else if (boost::iequals(level, "debug"))
        return quill::LogLevel::Debug;
    else if (boost::iequals(level, "trace"))
        return quill::LogLevel::TraceL1;
    else if (boost::iequals(level, "test"))
        return quill::LogLevel::TraceL2;
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not a valid log level {}", level);
}

ConsoleSink::ConsoleSink(Stream stream, bool enable_colors_)
    : StreamSink{streamToString(stream), nullptr}
    , enable_colors(enable_colors_)
{
}

void ConsoleSink::write_log(
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
    std::string_view log_statement)
{
#if defined(DEBUG_OR_SANITIZER_BUILD)
    if (enable_colors)
    {
        static constexpr std::string_view error_message = "Logging with color is only supported with OwnPatternFormatter";
        auto * formatter_ptr = Logger::getFormatter();
        if (!formatter_ptr)
        {
            std::cerr << error_message << std::endl;
            enable_colors = false;
        }

        auto & formatter = *formatter_ptr;
        if (typeid(formatter) != typeid(OwnPatternFormatter))
        {
            std::cerr << error_message << std::endl;
            enable_colors = false;
        }
    }
#endif

    if (!enable_colors)
    {
        // Write record to file
        StreamSink::write_log(
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

    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    std::string colored_log;
    /// reset color takes 7 bytes
    /// color can be at most 19 bytes
    colored_log.resize(log_statement.size() + 5 * 26);
    DB::WriteBufferFromString buf(colored_log);
    auto write_string = [&](std::string_view str)
    {
        buf.write(str.data(), str.size());
    };

    std::string_view reset_color = resetColor();

    size_t thread_id_start = log_statement.find('[');
    chassert(thread_id_start != std::string_view::npos && log_statement.size() - thread_id_start > 2);
    thread_id_start += 2;

    write_string(log_statement.substr(0, thread_id_start));
    log_statement.remove_prefix(thread_id_start);

    size_t thread_id_end = log_statement.find(' ');
    chassert(thread_id_end != std::string_view::npos);
    auto thread_id_str = log_statement.substr(0, thread_id_end);
    auto thread_id_color = setColor(std::hash<std::string_view>()(thread_id_str));
    write_string(thread_id_color);
    write_string(thread_id_str);
    write_string(reset_color);
    log_statement.remove_prefix(thread_id_str.size());


    auto query_id_start = log_statement.find('{');
    chassert(query_id_start != std::string_view::npos && log_statement.size() - query_id_start > 1);
    query_id_start += 1;
    write_string(log_statement.substr(0, query_id_start));
    log_statement.remove_prefix(query_id_start);

    auto query_id_end = log_statement.find('}');
    chassert(query_id_end != std::string_view::npos);
    auto query_id_str = log_statement.substr(0, query_id_end);
    if (!query_id_str.empty())
    {
        auto query_id_color = setColor(std::hash<std::string_view>()(query_id_str));
        write_string(query_id_color);
        write_string(query_id_str);
        write_string(reset_color);
        log_statement.remove_prefix(query_id_str.size());
    }

    auto priority_start = log_statement.find('<');
    chassert(priority_start != std::string_view::npos && log_statement.size() - priority_start > 1);
    priority_start += 1;
    write_string(log_statement.substr(0, priority_start));
    log_statement.remove_prefix(priority_start);

    auto level_end = log_statement.find('>');
    chassert(level_end != std::string_view::npos);
    auto level_str = log_statement.substr(0, level_end);
    auto level_color = getLogLevelColor(log_level_short_code);
    write_string(level_color);
    write_string(level_str);
    write_string(reset_color);
    write_string("> ");
    log_statement.remove_prefix(level_str.size() + 2);

    auto logger_name_end = log_statement.find(": ");
    chassert(logger_name_end != std::string_view::npos);
    auto logger_name_str = log_statement.substr(0, logger_name_end);
    auto logger_name_color = setColor(std::hash<std::string_view>()(logger_name_str));
    write_string(logger_name_color);
    write_string(logger_name_str);
    write_string(reset_color);
    log_statement.remove_prefix(logger_name_str.size());

    write_string(log_statement);

    buf.finalize();

    // Write record to file
    StreamSink::write_log(
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
        colored_log);
}

std::string_view ConsoleSink::getLogLevelColor(std::string_view log_level_short_code)
{
    if (log_level_short_code == "C")
        return "\033[1;41m";

    if (log_level_short_code == "E")
        return "\033[1;31m";

    if (log_level_short_code == "W")
        return "\033[0;31m";

    if (log_level_short_code == "I")
        return "\033[1m";

    if (log_level_short_code == "D")
        return "";

    if (log_level_short_code == "T1")
        return "\033[2m";

    return "";
}

namespace
{
bool renameFile(const std::filesystem::path & previous_file, const std::filesystem::path & new_file) noexcept
{
    std::error_code ec;
    std::filesystem::rename(previous_file, new_file, ec);
    return ec.value() == 0;
}

bool removeFile(const std::filesystem::path & filename) noexcept
{
    std::error_code ec;
    std::filesystem::remove(filename, ec);
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
        if (!deflater.good() || !ostr.good())
            throw Poco::WriteFileException(compressed_path);
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

}
RotatingFileSink::RotatingFileSink(const std::filesystem::path & filename, RotatingSinkConfiguration config_)
    : Base(filename, static_cast<const quill::FileSinkConfig &>(config_), quill::FileEventNotifier{}, /*do_fopen=*/false)
    , config(config_)
{
    recoverFiles();

    this->open_file(this->_filename, "a");
    created_files.emplace_front(this->_filename, 0);

    if (!this->is_null())
        current_log_size = std::filesystem::file_size(this->_filename);

    if (config.compress)
        pool = std::make_unique<FreeThreadPool>(
            CurrentMetrics::LogCompressionThreads,
            CurrentMetrics::LogCompressionActiveThreads,
            CurrentMetrics::LogCompressionScheduledThreads,
            /*max_threads_=*/1);

    if (config.rotate_on_open)
        rotateFiles();
}

void RotatingFileSink::write_log(
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
    std::string_view log_statement)
{
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

void RotatingFileSink::flush_sink()
{
    if (close_current_file.exchange(false, std::memory_order_relaxed))
        rotateFiles();

    Base::flush_sink();
}

void RotatingFileSink::closeFile()
{
    close_current_file.store(true, std::memory_order_relaxed);
}

void RotatingFileSink::recoverFiles()
{
    for (const auto & entry : std::filesystem::directory_iterator(std::filesystem::current_path() / this->_filename.parent_path()))
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
            std::filesystem::path current_file = entry.path().parent_path();
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

void RotatingFileSink::rotateFiles()
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

        std::filesystem::path existing_file = getFilename(it->base_filename, it->index);

        // increment the index if needed and rename the file
        uint32_t index_to_use = it->index + 1;
        std::filesystem::path renamed_file = getFilename(it->base_filename, index_to_use);
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
                    [current_path = std::move(renamed_file), new_path = std::move(compressed_path), compression_result_promise]() mutable
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
        const std::filesystem::path removed_file = getFilename(created_files.back().base_filename, created_files.back().index);
        removeFile(removed_file);
        created_files.pop_back();
    }

    // add the current file back to the list with index 0
    created_files.emplace_front(this->_filename, 0);

    // Open file for logging
    this->open_file(this->_filename, "w");
    current_log_size = 0;
}

std::filesystem::path RotatingFileSink::appendIndexToFilename(const std::filesystem::path & filename, size_t index)
{
    if (index == 0)
        return filename;

    auto const [stem, ext] = Base::extract_stem_and_extension(filename);
    return std::filesystem::path{stem + "." + std::to_string(index - 1) + ext};
}

std::filesystem::path RotatingFileSink::getFilename(std::filesystem::path base_filename, size_t index)
{
    if (index > 0)
        return appendIndexToFilename(base_filename, index);
    return base_filename;
}

}
