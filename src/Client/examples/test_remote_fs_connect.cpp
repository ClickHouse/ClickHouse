#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <thread>
#include <atomic>

#include <Client/RemoteFSConnection.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>

#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>


int main()
try
{
    using namespace DB;

    Poco::Logger::setLevel(Poco::Logger::root().name(), Poco::Message::Priority::PRIO_TRACE);
    Poco::AutoPtr<Poco::ConsoleChannel> log = new Poco::ConsoleChannel;
    Poco::Logger::setChannel(Poco::Logger::root().name(), log.get());

    ConnectionTimeouts timeouts(
        Poco::Timespan(1000000), /// Connection timeout.
        Poco::Timespan(1000000), /// Send timeout.
        Poco::Timespan(1000000)  /// Receive timeout.
    );
    RemoteFSConnection conn("localhost", 9012, "test_disk");
    conn.forceConnected(timeouts);
    conn.forceConnected(timeouts);
    std::cout << "Should ping" << std::endl;
    
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
