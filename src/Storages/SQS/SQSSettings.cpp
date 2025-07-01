#include <Storages/SQS/SQSSettings.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <unordered_set>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool SQSSettings::has(std::string_view name)
{
    static const std::unordered_set<std::string_view> settings = {
        // Main parameters and their aliases
        "queue_url", "sqs_queue_url",
        
        // AWS credentials and their aliases
        "aws_access_key_id", "sqs_aws_access_key_id",
        "aws_secret_access_key", "sqs_aws_secret_access_key",
        "aws_region", "sqs_aws_region",
        "endpoint_override", "sqs_endpoint",
        
        // Queue parameters
        "queue_type", "sqs_queue_type",
        "max_message_size", "sqs_max_message_size",
        "max_receive_count", "sqs_max_receive_count",
        "dead_letter_queue_url", "sqs_dead_letter_queue_url",
        
        // Message receiving parameters
        "wait_time_seconds", "sqs_wait_time_seconds",
        "receive_message_wait_time_seconds", "sqs_receive_message_wait_time_seconds",
        "max_messages_per_receive", "sqs_max_messages_per_receive",
        "visibility_timeout", "sqs_visibility_timeout",
        "auto_delete", "sqs_auto_delete",
        "skip_invalid_messages", "sqs_skip_invalid_messages",

        // Performance parameters
        "max_rows_per_message", "sqs_max_rows_per_message",
        "poll_timeout_ms", "sqs_poll_timeout_ms",
        "num_consumers", "sqs_num_consumers",
        "max_block_size", "sqs_max_block_size",
        "skip_broken_messages", "sqs_skip_broken_messages",
        "flush_interval_ms", "sqs_flush_interval_ms",
        "internal_queue_size", "sqs_internal_queue_size",
        "max_connections", "sqs_max_connections",

        // Formatting parameters
        "format_name", "sqs_format",
        "row_delimiter", "sqs_row_delimiter",
        "schema", "sqs_schema",
        
        // SSL/HTTP settings
        "verify_ssl", "sqs_verify_ssl",
        "use_http", "sqs_use_http",
        "request_timeout_ms", "sqs_request_timeout_ms",
        "connect_timeout_ms", "sqs_connect_timeout_ms",
        
        // Retry attempts settings
        "max_retries", "sqs_max_retries",
        "retry_initial_delay_ms", "sqs_retry_initial_delay_ms",
        
        // TCP settings
        "enable_tcp_keep_alive", "sqs_enable_tcp_keep_alive",
        "tcp_keep_alive_interval_ms", "sqs_tcp_keep_alive_interval_ms",
        
        // Proxy settings
        "proxy_host", "sqs_proxy_host",
        "proxy_port", "sqs_proxy_port",
        "proxy_username", "sqs_proxy_username",
        "proxy_password", "sqs_proxy_password"
    };
    
    return settings.contains(name);
}

void SQSSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (!storage_def.settings)
        return;
    
    for (const auto & setting : storage_def.settings->changes)
    {
        const String & name = setting.name;
        const Field & value = setting.value;
        
        // Required parameters
        if (name == "queue_url" || name == "sqs_queue_url")
            queue_url = value.safeGet<String>();
        
        // AWS credentials
        else if (name == "aws_access_key_id" || name == "sqs_aws_access_key_id")
            aws_access_key_id = value.safeGet<String>();
        else if (name == "aws_secret_access_key" || name == "sqs_aws_secret_access_key")
            aws_secret_access_key = value.safeGet<String>();
        else if (name == "aws_region" || name == "sqs_aws_region")
            aws_region = value.safeGet<String>();
        
        // Queue parameters
        else if (name == "queue_type" || name == "sqs_queue_type")
            queue_type = value.safeGet<String>();
        else if (name == "max_message_size" || name == "sqs_max_message_size")
            max_message_size = value.safeGet<UInt64>();
        else if (name == "max_receive_count" || name == "sqs_max_receive_count")
            max_receive_count = value.safeGet<UInt64>();
        else if (name == "dead_letter_queue_url" || name == "sqs_dead_letter_queue_url")
            dead_letter_queue_url = value.safeGet<String>();
        
        // Message receiving parameters
        else if (name == "wait_time_seconds" || name == "sqs_wait_time_seconds")
            wait_time_seconds = value.safeGet<UInt64>();
        else if (name == "receive_message_wait_time_seconds" || name == "sqs_receive_message_wait_time_seconds")
            receive_message_wait_time_seconds = value.safeGet<UInt64>();
        else if (name == "max_messages_per_receive" || name == "sqs_max_messages_per_receive")
            max_messages_per_receive = value.safeGet<UInt64>();
        else if (name == "visibility_timeout" || name == "sqs_visibility_timeout")
            visibility_timeout = value.safeGet<UInt64>();
        else if (name == "auto_delete" || name == "sqs_auto_delete")
            auto_delete = value.safeGet<bool>();
        else if (name == "skip_invalid_messages" || name == "sqs_skip_invalid_messages")
            skip_invalid_messages = value.safeGet<bool>();
        
        // Performance parameters
        else if (name == "max_rows_per_message" || name == "sqs_max_rows_per_message")
            max_rows_per_message = value.safeGet<UInt64>();
        else if (name == "poll_timeout_ms" || name == "sqs_poll_timeout_ms")
            poll_timeout_ms = value.safeGet<UInt64>();
        else if (name == "num_consumers" || name == "sqs_num_consumers")
            num_consumers = value.safeGet<UInt64>();
        else if (name == "max_block_size" || name == "sqs_max_block_size")
            max_block_size = value.safeGet<UInt64>();
        else if (name == "skip_broken_messages" || name == "sqs_skip_broken_messages")
            skip_broken_messages = value.safeGet<UInt64>();
        else if (name == "flush_interval_ms" || name == "sqs_flush_interval_ms")
            flush_interval_ms = value.safeGet<UInt64>();
        else if (name == "internal_queue_size" || name == "sqs_internal_queue_size")
            internal_queue_size = value.safeGet<size_t>();
        else if (name == "max_connections" || name == "sqs_max_connections")
            max_connections = static_cast<UInt32>(value.safeGet<UInt64>());
        
        // Formatting parameters
        else if (name == "format_name" || name == "sqs_format")
            format_name = value.safeGet<String>();
        else if (name == "row_delimiter" || name == "sqs_row_delimiter")
            row_delimiter = value.safeGet<String>();
        else if (name == "schema" || name == "sqs_schema")
            schema = value.safeGet<String>();

        else if (name == "max_retries" || name == "sqs_max_retries")
            max_retries = value.safeGet<UInt64>();
        else if (name == "retry_initial_delay_ms" || name == "sqs_retry_initial_delay_ms")
            retry_initial_delay_ms = value.safeGet<UInt64>();
        else if (name == "enable_tcp_keep_alive" || name == "sqs_enable_tcp_keep_alive")
            enable_tcp_keep_alive = value.safeGet<bool>();
        else if (name == "tcp_keep_alive_interval_ms" || name == "sqs_tcp_keep_alive_interval_ms")
            tcp_keep_alive_interval_ms = value.safeGet<UInt64>();

        else if (name == "proxy_host" || name == "sqs_proxy_host")
            proxy_host = value.safeGet<String>();
        else if (name == "proxy_port" || name == "sqs_proxy_port")
            proxy_port = static_cast<UInt32>(value.safeGet<UInt64>());
        else if (name == "proxy_username" || name == "sqs_proxy_username")
            proxy_username = value.safeGet<String>();
        else if (name == "proxy_password" || name == "sqs_proxy_password")
            proxy_password = value.safeGet<String>();
        else throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting '{}' for storage SQS", name);
    }

    const auto & config = getContext()->getConfigRef();
    if (aws_access_key_id.empty() && config.has("aws_access_key_id"))
        aws_access_key_id = config.getString("aws_access_key_id");
    if (aws_secret_access_key.empty() && config.has("aws_secret_access_key"))
        aws_secret_access_key = config.getString("aws_secret_access_key");
    if (aws_region.empty() && config.has("aws_region"))
        aws_region = config.getString("aws_region");
    if (queue_url.empty() && config.has("sqs.queue_url"))
        queue_url = config.getString("sqs.queue_url");
    if (format_name.empty() && config.has("sqs.format_name"))
        format_name = config.getString("sqs.format_name");

    // Check required parameters
    if (queue_url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required setting 'queue_url' for storage SQS is empty");
}

}
