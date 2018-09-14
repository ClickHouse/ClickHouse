#pragma once
#include <Core/Types.h>
#include <Poco/Message.h>


namespace DB
{

/// Poco::Message with more ClickHouse-specific info
/// NOTE: Poco::Message is not polymorphic class, so we can't use inheritance in couple with dynamic_cast<>()
class ExtendedLogMessage
{
public:
    explicit ExtendedLogMessage(const Poco::Message & base) : base(base) {}

    /// Attach additional data to the message
    static ExtendedLogMessage getFrom(const Poco::Message & base);

    // Do not copy for efficiency reasons
    const Poco::Message & base;

    UInt32 time_seconds = 0;
    UInt32 time_microseconds = 0;

    UInt32 thread_number = 0;
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
