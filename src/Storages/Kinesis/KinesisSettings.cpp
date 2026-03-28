#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Kinesis/KinesisSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define KINESIS_RELATED_SETTINGS(DECLARE, ALIAS) \
    /* Connection parameters */ \
    DECLARE(String, kinesis_stream_name, "", "Name of the Kinesis stream.", 0) \
    DECLARE(String, kinesis_aws_access_key_id, "", "AWS access key ID.", 0) \
    DECLARE(String, kinesis_aws_secret_access_key, "", "AWS secret access key.", 0) \
    DECLARE(String, kinesis_aws_region, "us-east-1", "AWS region where the Kinesis stream is located.", 0) \
    DECLARE(String, kinesis_endpoint, "", "Custom endpoint URL (e.g. for LocalStack). Derived from stream ARN if not set.", 0) \
    /* Format */ \
    DECLARE(String, kinesis_format, "", "The message format.", 0) \
    DECLARE(String, kinesis_schema, "", "Schema identifier (used by schema-based formats).", 0) \
    /* Consumer performance */ \
    DECLARE(UInt64, kinesis_num_consumers, 1, "The number of consumer threads per table.", 0) \
    DECLARE(UInt64, kinesis_max_block_size, 0, "Number of rows collected before flushing data from Kinesis.", 0) \
    DECLARE(Milliseconds, kinesis_flush_interval_ms, 0, "Timeout for flushing data from Kinesis.", 0) \
    DECLARE(UInt64, kinesis_poll_timeout_ms, 500, "Timeout between polling attempts when no records are available.", 0) \
    /* Record receiving */ \
    DECLARE(UInt64, kinesis_max_records_per_request, 10000, "Maximum number of records per GetRecords request (1-10000).", 0) \
    /* Starting position */ \
    DECLARE(String, kinesis_starting_position, "LATEST", "Starting position: LATEST, TRIM_HORIZON, or AT_TIMESTAMP.", 0) \
    DECLARE(UInt64, kinesis_at_timestamp, 0, "Unix timestamp for AT_TIMESTAMP starting position.", 0) \
    /* Checkpointing */ \
    DECLARE(Bool, kinesis_save_checkpoints, true, "Save sequence number checkpoints to survive restarts.", 0) \
    /* Error handling */ \
    DECLARE(UInt64, kinesis_skip_broken_messages, 0, "Skip at least this number of broken messages from Kinesis per block.", 0) \
    DECLARE(StreamingHandleErrorMode, kinesis_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for Kinesis engine.", 0) \
    /* Producer */ \
    DECLARE(UInt64, kinesis_max_rows_per_message, 1, "The maximum number of rows per PutRecord call for row-based formats.", 0) \
    /* SSL */ \
    DECLARE(Bool, kinesis_verify_ssl, true, "Verify SSL certificate when connecting to Kinesis.", 0) \

#define LIST_OF_KINESIS_SETTINGS(M, ALIAS) \
    KINESIS_RELATED_SETTINGS(M, ALIAS)     \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(KinesisSettingsTraits, LIST_OF_KINESIS_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(KinesisSettingsTraits, LIST_OF_KINESIS_SETTINGS)

struct KinesisSettingsImpl : public BaseSettings<KinesisSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) KinesisSettings##TYPE NAME = &KinesisSettingsImpl::NAME;

namespace KinesisSetting
{
LIST_OF_KINESIS_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

KinesisSettings::KinesisSettings() : impl(std::make_unique<KinesisSettingsImpl>())
{
}

KinesisSettings::KinesisSettings(const KinesisSettings & settings) : impl(std::make_unique<KinesisSettingsImpl>(*settings.impl))
{
}

KinesisSettings::KinesisSettings(KinesisSettings && settings) noexcept : impl(std::make_unique<KinesisSettingsImpl>(std::move(*settings.impl)))
{
}

KinesisSettings::~KinesisSettings() = default;

KINESIS_SETTINGS_SUPPORTED_TYPES(KinesisSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void KinesisSettings::loadFromQuery(ASTStorage & storage_def)
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
                e.addMessage("for storage Kinesis");
            throw;
        }
    }
}

SettingsChanges KinesisSettings::getFormatSettings() const
{
    SettingsChanges result;

    for (const auto & change : impl->changes())
    {
        /// Every setting starting with "kinesis_" is for kinesis enginee
        if (!change.name.starts_with("kinesis_"))
            result.push_back(change);
    }

    return result;
}

bool KinesisSettings::hasBuiltin(std::string_view name)
{
    return KinesisSettingsImpl::hasBuiltin(name);
}

}