#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/SQS/SQSSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define SQS_RELATED_SETTINGS(DECLARE, ALIAS) \
    /* Connection parameters */ \
    DECLARE(String, sqs_queue_url, "", "URL of the SQS queue (e.g. http://localhost:4566/000000000000/queue-name).", 0) \
    DECLARE(String, sqs_aws_access_key_id, "", "AWS access key ID.", 0) \
    DECLARE(String, sqs_aws_secret_access_key, "", "AWS secret access key.", 0) \
    DECLARE(String, sqs_aws_region, "us-east-1", "AWS region.", 0) \
    DECLARE(String, sqs_endpoint, "", "Custom endpoint URL override (derived from queue URL if not set).", 0) \
    /* Format */ \
    DECLARE(String, sqs_format, "", "The message format.", 0) \
    DECLARE(String, sqs_schema, "", "Schema identifier (used by schema-based formats) for SQS engine.", 0) \
    /* Consumer performance */ \
    DECLARE(UInt64, sqs_num_consumers, 1, "The number of consumer threads per table.", 0) \
    DECLARE(UInt64, sqs_max_block_size, 0, "Number of rows collected before flushing data from SQS.", 0) \
    DECLARE(Milliseconds, sqs_flush_interval_ms, 0, "Timeout for flushing data from SQS.", 0) \
    DECLARE(UInt64, sqs_poll_timeout_ms, 500, "Timeout between polling attempts when no messages are available.", 0) \
    /* Message receiving */ \
    DECLARE(UInt64, sqs_max_messages_per_receive, 10, "Maximum number of messages per ReceiveMessage request (1-10).", 0) \
    DECLARE(UInt64, sqs_visibility_timeout, 30, "Visibility timeout in seconds: how long a received message is hidden from other consumers.", 0) \
    DECLARE(UInt64, sqs_wait_time_seconds, 0, "Long-polling wait time in seconds (0 disables long polling, max 20).", 0) \
    /* Message lifecycle */ \
    DECLARE(Bool, sqs_auto_delete, false, "Automatically delete messages after successful processing.", 0) \
    DECLARE(String, sqs_dead_letter_queue_url, "", "URL of the Dead Letter Queue for messages that exceed the max receive count.", 0) \
    DECLARE(UInt64, sqs_max_receive_count, 3, "Maximum number of receive attempts before a message is moved to the DLQ.", 0) \
    /* Error handling */ \
    DECLARE(UInt64, sqs_skip_broken_messages, 0, "Skip at least this number of broken messages from SQS per block.", 0) \
    DECLARE(StreamingHandleErrorMode, sqs_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for SQS engine. Possible values: default (throw an exception after sqs_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \
    /* Producer */ \
    DECLARE(UInt64, sqs_max_rows_per_message, 1, "The maximum number of rows produced in one message for row-based formats.", 0) \
    /* SSL */ \
    DECLARE(Bool, sqs_verify_ssl, true, "Verify SSL certificate when connecting to SQS.", 0) \

#define LIST_OF_SQS_SETTINGS(M, ALIAS) \
    SQS_RELATED_SETTINGS(M, ALIAS)     \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(SQSSettingsTraits, LIST_OF_SQS_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(SQSSettingsTraits, LIST_OF_SQS_SETTINGS)

struct SQSSettingsImpl : public BaseSettings<SQSSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) SQSSettings##TYPE NAME = &SQSSettingsImpl::NAME;

namespace SQSSetting
{
LIST_OF_SQS_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

SQSSettings::SQSSettings() : impl(std::make_unique<SQSSettingsImpl>())
{
}

SQSSettings::SQSSettings(const SQSSettings & settings) : impl(std::make_unique<SQSSettingsImpl>(*settings.impl))
{
}

SQSSettings::SQSSettings(SQSSettings && settings) noexcept : impl(std::make_unique<SQSSettingsImpl>(std::move(*settings.impl)))
{
}

SQSSettings::~SQSSettings() = default;

SQS_SETTINGS_SUPPORTED_TYPES(SQSSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void SQSSettings::loadFromQuery(ASTStorage & storage_def)
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
                e.addMessage("for storage SQS");
            throw;
        }
    }
}

SettingsChanges SQSSettings::getFormatSettings() const
{
    SettingsChanges result;

    for (const auto & change : impl->changes())
    {
        /// Every setting starting with "sqs_" is for SQS engine itself.
        if (!change.name.starts_with("sqs_"))
            result.push_back(change);
    }

    return result;
}

bool SQSSettings::hasBuiltin(std::string_view name)
{
    auto it = SQSSettingsTraits::findIndex(name);
    return it != SQSSettingsTraits::npos;
}

}
