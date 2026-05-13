#include <Interpreters/NamedScalars/NamedScalarValueBackendLocal.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

#include <Core/Settings.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <chrono>
#include <filesystem>
#include <string_view>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
}

namespace
{

constexpr std::string_view value_suffix = ".bin";

String makeDirectoryPathCanonical(const String & directory_path)
{
    auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
    if (canonical_directory_path.has_filename())
        canonical_directory_path += std::filesystem::path::preferred_separator;
    return canonical_directory_path;
}

void createDirectory(const String & dir_path)
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(
            ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "Couldn't create directory {} reason: '{}'",
            dir_path,
            create_dir_error_code.message());
}

String getValueFilePath(const String & dir_path, const String & value_key)
{
    return dir_path + escapeForFileName(value_key) + String(value_suffix);
}

/// Rename to .bad.<ts>; fresh-eval will rewrite the file later.
void quarantineValueFile(const String & file_path, LoggerPtr log, const String & reason)
{
    const auto ts = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const String quarantine_path = fmt::format("{}.bad.{}", file_path, ts);
    LOG_ERROR(log, "Quarantining named scalar value file {} (reason: {}) -> {}; will re-evaluate", file_path, reason, quarantine_path);
    try { fs::rename(file_path, quarantine_path); }
    catch (...) { tryLogCurrentException(log, fmt::format("renaming {} to {}", file_path, quarantine_path)); }
}

std::optional<String> readValueBlobFromDisk(
    const String & dir_path, const String & value_key, LoggerPtr log)
{
    const String file_path = getValueFilePath(dir_path, value_key);
    LOG_DEBUG(log, "Loading raw named scalar value for {} from {}", value_key, file_path);

    if (!fs::exists(file_path))
        return std::nullopt;

    try
    {
        ReadBufferFromFile in(file_path);
        String contents;
        readStringUntilEOF(contents, in);
        return contents;
    }
    catch (...)
    {
        const String reason = fmt::format("raw read failed: {}", getCurrentExceptionMessage(false));
        quarantineValueFile(file_path, log, reason);
        return std::nullopt;
    }
}

/// Refresh-publish hot path: payload is already encoded by NamedScalar.
void writeValueFile(
    const String & dir_path,
    const String & value_key,
    const String & payload,
    const Settings & settings,
    LoggerPtr log)
{
    const String file_path = getValueFilePath(dir_path, value_key);
    const String temp_file_path = file_path + ".tmp." + getRandomASCIIString(8);
    LOG_DEBUG(log, "Storing named scalar value for {} to {}", value_key, file_path);

    try
    {
        WriteBufferFromFile out(temp_file_path, payload.size());
        writeString(payload, out);
        out.next();
        if (settings[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }
}

void removeValueFile(const String & dir_path, const String & value_key, LoggerPtr log)
{
    const String file_path = getValueFilePath(dir_path, value_key);
    LOG_DEBUG(log, "Removing named scalar value for {} from {}", value_key, file_path);
    fs::remove(file_path);
}

}

NamedScalarValueBackendLocal::NamedScalarValueBackendLocal(String dir_path_, LoggerPtr log_)
    : dir_path(makeDirectoryPathCanonical(dir_path_))
    , log(std::move(log_))
{
}

void NamedScalarValueBackendLocal::initialize()
{
    createDirectory(dir_path);
}

void NamedScalarValueBackendLocal::setGlobalContext(ContextPtr global_context_)
{
    global_context = std::move(global_context_);
}

std::optional<String> NamedScalarValueBackendLocal::readValueBlob(const String & value_key)
{
    return readValueBlobFromDisk(dir_path, value_key, log);
}

std::optional<String> NamedScalarValueBackendLocal::readValueBlobAndWatch(const String & value_key, std::function<void()>)
{
    return readValueBlob(value_key);
}

void NamedScalarValueBackendLocal::removeValue(const String & value_key)
{
    removeValueFile(dir_path, value_key, log);
}

std::optional<NamedScalarRefreshLease> NamedScalarValueBackendLocal::tryAcquireRefreshLease(
    const String &,
    const String & value_key)
{
    return NamedScalarRefreshLease(
        [this, value_key](const String & value_blob)
        {
            writeValue(value_key, value_blob);
            return RefreshPublishResult::Published;
        });
}

void NamedScalarValueBackendLocal::writeValue(const String & value_key, const String & value_blob)
{
    chassert(global_context);
    writeValueFile(dir_path, value_key, value_blob, global_context->getSettingsRef(), log);
}

}
