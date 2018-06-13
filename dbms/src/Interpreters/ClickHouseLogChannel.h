#pragma once
#include <Poco/Channel.h>


namespace DB
{

/// Poco::Channel used to implement passing of query logs to Client via TCP interface
class ClickHouseLogChannel : public Poco::Channel
{
public:
    ClickHouseLogChannel() = default;

    void log(const Poco::Message & msg) override;
};

}
