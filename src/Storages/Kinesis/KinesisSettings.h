#pragma once

#include <string_view>

#include <Core/Settings.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

struct KinesisSettings : public WithContext
{
    // Required parameters
    String stream_name;                            // Kinesis stream name

    // AWS authorization parameters (can be taken from global configuration)
    String aws_access_key_id;                      // AWS access key ID
    String aws_secret_access_key;                  // AWS secret access key
    String aws_region = "us-east-1";               // AWS region where Kinesis stream is located
    String endpoint_override;                      // Kinesis endpoint override

    // SSL/HTTP parameters
    bool verify_ssl = false;                       // Flag to verify SSL certificate
    bool use_http = false;                         // Flag to use HTTP instead of HTTPS (for local development)
    UInt64 request_timeout_ms = 5000;              // Request timeout in milliseconds
    UInt64 connect_timeout_ms = 5000;              // Connection timeout in milliseconds
    
    // Kinesis stream parameters
    String starting_position = "TRIM_HORIZON";     // Starting position for reading: LATEST, TRIM_HORIZON, AT_TIMESTAMP
    UInt64 at_timestamp = 0;                       // Timestamp for AT_TIMESTAMP position (unix timestamp)
    bool enhanced_fan_out = false;                 // Use Enhanced Fan-Out mode with dedicated capacity
    String consumer_name;                          // Consumer name for Enhanced Fan-Out mode
    
    // Parameters for record receiving behavior
    UInt64 max_records_per_request = 10000;        // Maximum number of records per request GetRecords
    bool auto_reconnect = true;                    // Automatically reconnect on failures
    UInt64 retry_backoff_ms = 1000;                // Interval between retry attempts in milliseconds
    bool save_checkpoints = true;                  // Flag to save checkpoints
    UInt64 max_execution_time_ms = 0;              // Maximum execution time of consumer on one iteration

    // Performance settings
    UInt64 max_rows_per_message = 1;               // Maximum number of rows per message when sending
    UInt64 poll_timeout_ms = 500;                  // Timeout for GetRecords request in milliseconds
    UInt64 num_consumers = 3;                      // Number of consumers for reading from shards
    UInt64 max_block_size = 500000;                // Maximum block size for processing
    UInt64 skip_broken_messages = 0;               // Maximum number of broken messages to skip
    UInt64 flush_interval_ms = 0;                  // Timeout for flushing data from engine
    UInt32 max_connections = 25;                   // Maximum number of HTTP connections
    size_t internal_queue_size = 1000;             // Size of the queue for storing received messages
    
    // Formatting settings
    String format_name = "JSONEachRow";            // Message format (JSONEachRow, CSV, TSV, etc.)
    String row_delimiter;                          // Row delimiter in the message
    String schema;                                 // Optional schema for complex formats
    
    // Retry parameters
    UInt64 max_retries = 10;                       // Maximum number of retry attempts
    UInt64 retry_initial_delay_ms = 50;            // Initial delay between retry attempts
    
    // TCP settings
    bool enable_tcp_keep_alive = true;             // Use TCP keep-alive
    UInt64 tcp_keep_alive_interval_ms = 30000;     // TCP keep-alive interval
    
    // Proxy settings
    String proxy_host;                             // Proxy host
    UInt32 proxy_port = 0;                         // Proxy port
    String proxy_username;                         // Proxy username
    String proxy_password;                         // Proxy password

    explicit KinesisSettings(ContextPtr context_) : WithContext(context_) {}

    void loadFromQuery(const ASTStorage & storage_def);
    static bool has(std::string_view name);
};

}
