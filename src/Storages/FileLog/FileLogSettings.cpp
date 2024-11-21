#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/FileLog/FileLogSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int INVALID_SETTING_VALUE;
}

#define FILELOG_RELATED_SETTINGS(DECLARE, ALIAS) \
    /* default is stream_poll_timeout_ms */ \
    DECLARE(Milliseconds, poll_timeout_ms, 0, "Timeout for single poll from StorageFileLog.", 0) \
    DECLARE(UInt64, poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single StorageFileLog poll.", 0) \
    DECLARE(UInt64, max_block_size, 0, "Number of row collected by poll(s) for flushing data from StorageFileLog.", 0) \
    DECLARE(MaxThreads, max_threads, 0, "Number of max threads to parse files, default is 0, which means the number will be max(1, physical_cpu_cores / 4)", 0) \
    DECLARE(Milliseconds, poll_directory_watch_events_backoff_init, 500, "The initial sleep value for watch directory thread.", 0) \
    DECLARE(Milliseconds, poll_directory_watch_events_backoff_max, 32000, "The max sleep value for watch directory thread.", 0) \
    DECLARE(UInt64, poll_directory_watch_events_backoff_factor, 2, "The speed of backoff, exponential by default", 0) \
    DECLARE(StreamingHandleErrorMode, handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for FileLog engine. Possible values: default (throw an exception after nats_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \

#define LIST_OF_FILELOG_SETTINGS(M, ALIAS) \
    FILELOG_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(FileLogSettingsTraits, LIST_OF_FILELOG_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(FileLogSettingsTraits, LIST_OF_FILELOG_SETTINGS)

struct FileLogSettingsImpl : public BaseSettings<FileLogSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) FileLogSettings##TYPE NAME = &FileLogSettingsImpl ::NAME;

namespace FileLogSetting
{
LIST_OF_FILELOG_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

FileLogSettings::FileLogSettings() : impl(std::make_unique<FileLogSettingsImpl>())
{
}

FileLogSettings::FileLogSettings(const FileLogSettings & settings) : impl(std::make_unique<FileLogSettingsImpl>(*settings.impl))
{
}

FileLogSettings::FileLogSettings(FileLogSettings && settings) noexcept
    : impl(std::make_unique<FileLogSettingsImpl>(std::move(*settings.impl)))
{
}

FileLogSettings::~FileLogSettings() = default;

FILELOG_SETTINGS_SUPPORTED_TYPES(FileLogSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void FileLogSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    /// Check that batch size is not too high (the same as we check setting max_block_size).
    constexpr UInt64 max_sane_block_rows_size = 4294967296; // 2^32
    if (impl->poll_max_batch_size > max_sane_block_rows_size)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Sanity check: 'poll_max_batch_size' value is too high ({})", impl->poll_max_batch_size);
}

}
