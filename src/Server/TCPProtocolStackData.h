#pragma once

#include <string>
#include <Poco/Net/StreamSocket.h>

namespace DB
{

struct TCPProtocolStackData
{
    Poco::Net::StreamSocket socket;
    std::string forwarded_for;
};

}
