#include <Storages/Kinesis/KinesisSettings.h>
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

bool KinesisSettings::has(std::string_view name)
{
    static const std::unordered_set<std::string_view> settings = {
        // Main parameters and their aliases
        "stream_name", "kinesis_stream_name",
        
        // AWS credentials and their aliases
        "aws_access_key_id", "kinesis_aws_access_key_id",
        "aws_secret_access_key", "kinesis_aws_secret_access_key",
        "aws_region", "kinesis_aws_region",
        "endpoint_override", "kinesis_endpoint",
        
        // Stream parameters
        "starting_position", "kinesis_starting_position",
        "at_timestamp", "kinesis_timestamp",
        "enhanced_fan_out", "kinesis_enhanced_fan_out",
        "consumer_name", "kinesis_consumer_name",
        
        // Record receiving parameters
        "max_records_per_request", "kinesis_max_records_per_request",
        "auto_reconnect", "kinesis_auto_reconnect",
        "retry_backoff_ms", "kinesis_retry_backoff_ms",
        "save_checkpoints", "kinesis_save_checkpoints",
        "max_execution_time_ms", "kinesis_max_execution_time_ms",
        // Performance parameters
        "max_rows_per_message", "kinesis_max_rows_per_message",
        "poll_timeout_ms", "kinesis_poll_timeout_ms",
        "num_consumers", "kinesis_num_consumers",
        "max_block_size", "kinesis_max_block_size",
        "skip_broken_messages", "kinesis_skip_broken_messages",
        "flush_interval_ms", "kinesis_flush_interval_ms",
        "internal_queue_size", "kinesis_internal_queue_size",
        "thread_per_consumer", "kinesis_thread_per_consumer",
        "max_connections", "kinesis_max_connections",

        // Formatting parameters
        "format_name", "kinesis_format",
        "row_delimiter", "kinesis_row_delimiter",
        "schema", "kinesis_schema",
        
        // Sharding parameters
        "balance_consumers", "kinesis_balance_consumers",
        "checkpoint_store", "kinesis_checkpoint_store",
        "checkpoint_table", "kinesis_checkpoint_table",
        
        // SSL/HTTP settings
        "verify_ssl", "kinesis_verify_ssl",
        "use_http", "kinesis_use_http",
        "request_timeout_ms", "kinesis_request_timeout_ms",
        "connect_timeout_ms", "kinesis_connect_timeout_ms",
        
        // Retry attempts settings
        "max_retries", "kinesis_max_retries",
        "retry_initial_delay_ms", "kinesis_retry_initial_delay_ms",
        
        // TCP settings
        "enable_tcp_keep_alive", "kinesis_enable_tcp_keep_alive",
        "tcp_keep_alive_interval_ms", "kinesis_tcp_keep_alive_interval_ms",
        
        // Proxy settings
        "proxy_host", "kinesis_proxy_host",
        "proxy_port", "kinesis_proxy_port",
        "proxy_username", "kinesis_proxy_username",
        "proxy_password", "kinesis_proxy_password"
    };
    
    return settings.contains(name);
}

void KinesisSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (!storage_def.settings)
        return;
    
    for (const auto & setting : storage_def.settings->changes)
    {
        const String & name = setting.name;
        const Field & value = setting.value;
        
        // Required parameters
        if (name == "stream_name" || name == "kinesis_stream_name")
            stream_name = value.safeGet<String>();
        
        // AWS credentials
        else if (name == "aws_access_key_id" || name == "kinesis_aws_access_key_id")
            aws_access_key_id = value.safeGet<String>();
        else if (name == "aws_secret_access_key" || name == "kinesis_aws_secret_access_key")
            aws_secret_access_key = value.safeGet<String>();
        else if (name == "aws_region" || name == "kinesis_aws_region")
            aws_region = value.safeGet<String>();
        else if (name == "endpoint_override" || name == "kinesis_endpoint")
            endpoint_override = value.safeGet<String>();
        
        // Stream parameters
        else if (name == "starting_position" || name == "kinesis_starting_position")
            starting_position = value.safeGet<String>();
        else if (name == "at_timestamp" || name == "kinesis_timestamp")
            at_timestamp = value.safeGet<UInt64>();
        else if (name == "enhanced_fan_out" || name == "kinesis_enhanced_fan_out")
            enhanced_fan_out = value.safeGet<bool>();
        else if (name == "consumer_name" || name == "kinesis_consumer_name")
            consumer_name = value.safeGet<String>();
        
        // Record receiving parameters
        else if (name == "max_records_per_request" || name == "kinesis_max_records_per_request")
            max_records_per_request = value.safeGet<UInt64>();
        else if (name == "auto_reconnect" || name == "kinesis_auto_reconnect")
            auto_reconnect = value.safeGet<bool>();
        else if (name == "retry_backoff_ms" || name == "kinesis_retry_backoff_ms")
            retry_backoff_ms = value.safeGet<UInt64>();
        else if (name == "save_checkpoints" || name == "kinesis_save_checkpoints")
            save_checkpoints = value.safeGet<bool>();
        else if (name == "max_execution_time_ms" || name == "kinesis_max_execution_time_ms")
            max_execution_time_ms = value.safeGet<UInt64>();
        // Performance parameters
        else if (name == "max_rows_per_message" || name == "kinesis_max_rows_per_message")
            max_rows_per_message = value.safeGet<UInt64>();
        else if (name == "poll_timeout_ms" || name == "kinesis_poll_timeout_ms")
            poll_timeout_ms = value.safeGet<UInt64>();
        else if (name == "num_consumers" || name == "kinesis_num_consumers")
            num_consumers = value.safeGet<UInt64>();
        else if (name == "max_block_size" || name == "kinesis_max_block_size")
            max_block_size = value.safeGet<UInt64>();
        else if (name == "skip_broken_messages" || name == "kinesis_skip_broken_messages")
            skip_broken_messages = value.safeGet<UInt64>();
        else if (name == "flush_interval_ms" || name == "kinesis_flush_interval_ms")
            flush_interval_ms = value.safeGet<UInt64>();
        else if (name == "internal_queue_size" || name == "kinesis_internal_queue_size")
            internal_queue_size = value.safeGet<size_t>();
        else if (name == "max_connections" || name == "kinesis_max_connections")
            max_connections = static_cast<UInt32>(value.safeGet<UInt64>());
        
        // Formatting parameters
        else if (name == "format_name" || name == "kinesis_format")
            format_name = value.safeGet<String>();
        else if (name == "row_delimiter" || name == "kinesis_row_delimiter")
            row_delimiter = value.safeGet<String>();
        else if (name == "schema" || name == "kinesis_schema")
            schema = value.safeGet<String>();

        // SSL/HTTP parameters
        else if (name == "verify_ssl" || name == "kinesis_verify_ssl")
            verify_ssl = value.safeGet<bool>();
        else if (name == "use_http" || name == "kinesis_use_http")
            use_http = value.safeGet<bool>();
        else if (name == "request_timeout_ms" || name == "kinesis_request_timeout_ms")
            request_timeout_ms = value.safeGet<UInt64>();
        else if (name == "connect_timeout_ms" || name == "kinesis_connect_timeout_ms")
            connect_timeout_ms = value.safeGet<UInt64>();
            
        // Retry attempts settings
        else if (name == "max_retries" || name == "kinesis_max_retries")
            max_retries = value.safeGet<UInt64>();
        else if (name == "retry_initial_delay_ms" || name == "kinesis_retry_initial_delay_ms")
            retry_initial_delay_ms = value.safeGet<UInt64>();
        
        // TCP settings
        else if (name == "enable_tcp_keep_alive" || name == "kinesis_enable_tcp_keep_alive")
            enable_tcp_keep_alive = value.safeGet<bool>();
        else if (name == "tcp_keep_alive_interval_ms" || name == "kinesis_tcp_keep_alive_interval_ms")
            tcp_keep_alive_interval_ms = value.safeGet<UInt64>();

        // Proxy settings
        else if (name == "proxy_host" || name == "kinesis_proxy_host")
            proxy_host = value.safeGet<String>();
        else if (name == "proxy_port" || name == "kinesis_proxy_port")
            proxy_port = static_cast<UInt32>(value.safeGet<UInt64>());
        else if (name == "proxy_username" || name == "kinesis_proxy_username")
            proxy_username = value.safeGet<String>();
        else if (name == "proxy_password" || name == "kinesis_proxy_password")
            proxy_password = value.safeGet<String>();
        else throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting '{}' for storage Kinesis", name);
    }

    const auto & config = getContext()->getConfigRef();
    if (aws_access_key_id.empty() && config.has("aws_access_key_id"))
        aws_access_key_id = config.getString("aws_access_key_id");
    if (aws_secret_access_key.empty() && config.has("aws_secret_access_key"))
        aws_secret_access_key = config.getString("aws_secret_access_key");
    if (aws_region.empty() && config.has("aws_region"))
        aws_region = config.getString("aws_region");
    if (stream_name.empty() && config.has("kinesis.stream_name"))
        stream_name = config.getString("kinesis.stream_name");
    if (format_name.empty() && config.has("kinesis.format_name"))
        format_name = config.getString("kinesis.format_name");
    if (endpoint_override.empty() && config.has("kinesis.endpoint"))
        endpoint_override = config.getString("kinesis.endpoint");

    // Check required settings
    if (stream_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required setting 'stream_name' for storage Kinesis is empty");
        
    // Check dependent settings
    if (enhanced_fan_out && consumer_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required setting 'consumer_name' when using enhanced_fan_out for storage Kinesis");
        
    // Validate starting position
    if (starting_position != "LATEST" && starting_position != "TRIM_HORIZON" && starting_position != "AT_TIMESTAMP")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, 
            "Invalid value for starting_position: '{}'. Valid values are: LATEST, TRIM_HORIZON, AT_TIMESTAMP", 
            starting_position);
            
    // Check if at_timestamp is set when starting_position is AT_TIMESTAMP
    if (starting_position == "AT_TIMESTAMP" && at_timestamp == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, 
            "Required setting 'at_timestamp' when starting_position is AT_TIMESTAMP");
}

}
