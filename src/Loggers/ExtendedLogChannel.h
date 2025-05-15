#pragma once
#include <string>

namespace Poco
{
class Message;
}

namespace DB
{
/// Poco::Message with more ClickHouse-specific info
/// NOTE: Poco::Message is not polymorphic class, so we can't use inheritance in couple with dynamic_cast<>()
class ExtendedLogMessage
{
public:
    explicit ExtendedLogMessage(const Poco::Message & base_)
        : base(&base_)
    {
    }
    explicit ExtendedLogMessage(const Poco::Message & new_base_, const ExtendedLogMessage & other)
        : base(&new_base_)
        , time_seconds(other.time_seconds)
        , time_microseconds(other.time_microseconds)
        , time_in_microseconds(other.time_in_microseconds)
        , thread_id(other.thread_id)
        , query_id(other.query_id)
    {
    }

    /// Attach additional data to the message
    static ExtendedLogMessage getFrom(const Poco::Message & base);

    // Do not copy for efficiency reasons
    const Poco::Message * base;

    uint32_t time_seconds = 0;
    uint32_t time_microseconds = 0;
    uint64_t time_in_microseconds = 0;

    uint64_t thread_id = 0;
    std::string query_id;
};


/// Interface extension of Poco::Channel
class ExtendedLogChannel
{
public:
    virtual void logExtended(const ExtendedLogMessage & msg) = 0;
    virtual ~ExtendedLogChannel() = default;
};

}
