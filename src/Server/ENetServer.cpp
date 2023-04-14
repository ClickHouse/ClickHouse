#include <Server/ENetServer.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Thread.h>

namespace DB
{

ENetServer::ENetServer()
{
    thread = new Poco::Thread("ENetServer");
}

}
