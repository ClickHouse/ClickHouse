#pragma once

#include <string_view>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct SQSSettings : public WithContext
{
    // Required parameters
    String queue_url;                              // URL SQS queue (for example, http://localstack:4566/000000000000/queue-name)
    
    // AWS authorization parameters (can be taken from global configuration)
    String aws_access_key_id;                      // AWS access key ID
    String aws_secret_access_key;                  // AWS secret access key
    String aws_region = "us-east-1";               // AWS region

    // SSL/HTTP parameters
    bool verify_ssl = false;                       // Flag to verify SSL certificate
    bool use_http = true;                          // Flag to use HTTP instead of HTTPS
    UInt64 request_timeout_ms = 5000;              // Request timeout in milliseconds
    UInt64 connect_timeout_ms = 5000;              // Connection timeout in milliseconds
    
    // SQS queue parameters
    String queue_type = "standard";                // Queue type: standard or fifo
    UInt64 max_message_size = 262144;              // Maximum message size (256 KB by default)
    UInt64 max_receive_count = 3;                  // Number of attempts to process before moving to Dead Letter Queue
    String dead_letter_queue_url;                  // URL for Dead Letter Queue
    
    // Parameters for message receiving behavior
    UInt64 wait_time_seconds = 0;                  // Waiting time for messages (long polling) in seconds
    UInt64 receive_message_wait_time_seconds = 0;  // Waiting time for a separate ReceiveMessage request (short polling)
    UInt64 max_messages_per_receive = 10;          // Maximum number of messages per receive request
    bool auto_delete = false;                      // Automatically delete messages after reading
    UInt64 visibility_timeout = 30;                // Visibility timeout for messages in seconds
    bool skip_invalid_messages = true;             // Skip invalid messages

    // Performance settings
    UInt64 max_rows_per_message = 1;               // Maximum number of rows per message when sending
    UInt64 poll_timeout_ms = 500;                  // Timeout for polling in milliseconds
    UInt64 num_consumers = 16;                     // Number of parallel consumers
    UInt64 max_block_size = 500000;                // Maximum block size
    UInt64 skip_broken_messages = 0;               // Maximum number of skipped broken messages
    UInt64 flush_interval_ms = 0;                  // Timeout for flushing data from the engine
    UInt32 max_connections = 25;                   // Maximum number of HTTP connections
    size_t internal_queue_size = 100;              // Size of the queue for storing received messages
    
    // Formatting settings
    String format_name = "JSONEachRow";            // Message format (JSONEachRow, CSV, TSV and etc.)
    String row_delimiter;                          // Row delimiter in the message
    String schema;                                 // Optional schema for complex formats
    
    // Parameters for retry attempts
    UInt64 max_retries = 10;                       // Maximum number of retries
    UInt64 retry_initial_delay_ms = 50;            // Initial delay between retries
    
    // TCP settings
    bool enable_tcp_keep_alive = true;             // Use TCP keep-alive
    UInt64 tcp_keep_alive_interval_ms = 30000;     // TCP keep-alive interval
    
    // Proxy settings (optional)
    String proxy_host;                              // Proxy host
    UInt32 proxy_port = 0;                          // Proxy port
    String proxy_username;                          // Proxy username
    String proxy_password;                          // Proxy password

    explicit SQSSettings(ContextPtr context_) : WithContext(context_) {}

    void loadFromQuery(const ASTStorage & storage_def);
    static bool has(std::string_view name);
};

}
