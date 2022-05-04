#pragma once
#include <string>

namespace Poco
{
class Message;
}

namespace DB
{
struct log_key_names
{
    std::string key_date_time;
    std::string key_thread_name;
    std::string key_thread_id;
    std::string key_level;
    std::string key_query_id;
    std::string key_logger_name;
    std::string key_message;
    std::string key_source_file;
    std::string key_source_line;
};
/// Poco::Message with more ClickHouse-specific info
/// NOTE: Poco::Message is not polymorphic class, so we can't use inheritance in couple with dynamic_cast<>()
class ExtendedLogMessage
{
public:
    explicit ExtendedLogMessage(const Poco::Message & base_) : base(base_) { }

    /// Attach additional data to the message
    static ExtendedLogMessage getFrom(const Poco::Message & base);

    // Do not copy for efficiency reasons
    const Poco::Message & base;

    uint32_t time_seconds = 0;
    uint32_t time_microseconds = 0;
    uint64_t time_in_microseconds = 0;

    uint64_t thread_id = 0;
    std::string query_id;
    static bool log_format_json;
    static log_key_names log_keys;
    //static log_key_names log_keys;
    static std::string key_thread_name;
    static std::string key_thread_id;
    static std::string key_level;
    static std::string key_query_id;
    static std::string key_logger_name;
    static std::string key_message;
    static std::string key_source_file;
    static std::string key_source_line;
};


/// Interface extension of Poco::Channel
class ExtendedLogChannel
{
public:
    virtual void logExtended(const ExtendedLogMessage & msg) = 0;
    virtual ~ExtendedLogChannel() = default;
};

}
