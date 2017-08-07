#define BOOST_COROUTINE_NO_DEPRECATION_WARNING 1
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING 1

#include <IO/ReadBufferFromAsioSocket.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <common/logger_useful.h>
#include <daemon/OwnPatternFormatter.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/AutoPtr.h>

#include <boost/asio.hpp>

#include <iostream>

using namespace DB;

static void setupLogging(const std::string & log_level)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new OwnPatternFormatter(nullptr));
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(log_level);
}

int main(int argc, char ** argv)
try
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " PORT [ PORT ]..." << std::endl;
        return 1;
    }

    std::vector<std::string> ports;
    for (int i = 1; i < argc; ++i)
        ports.emplace_back(argv[i]);

    setupLogging("trace");
    auto * log = &Logger::get("main");

    using Coro = boost::coroutines::coroutine<std::string>;

    Coro::pull_type coro([&](Coro::push_type & yield)
    {
        boost::asio::io_service io_service;

        auto work = [&](size_t num, const std::string & port, boost::asio::yield_context asio_yield)
        {
            try
            {
                LOG_TRACE(log, "Coro " << num << " starting.");

                boost::system::error_code ec;

                boost::asio::ip::tcp::endpoint endpoint;
                {
                    boost::asio::ip::tcp::resolver resolver(io_service);
                    boost::asio::ip::tcp::resolver::query query("localhost", port);
                    auto iter = resolver.async_resolve(query, asio_yield[ec]);
                    if (ec)
                        throw Exception("Could not resolve.");

                    ++iter; /// skip ipv6
                    endpoint = *iter;
                }

                LOG_TRACE(log, "Coro " << num << " resolved address to " << endpoint);

                boost::asio::ip::tcp::socket socket(io_service);
                socket.async_connect(endpoint, asio_yield[ec]);
                if (ec)
                    throw Exception("Could not connect.");

                LOG_TRACE(log, "Coro " << num << " connected.");

                ReadBufferFromAsioSocket read_buf(socket, asio_yield);

                while (!read_buf.eof())
                {
                    String str;
                    readString(str, read_buf);
                    yield(str);

                    char delim;
                    readChar(delim, read_buf);
                }

                LOG_TRACE(log, "Coro " << num << " ended.");
            }
            catch (Exception & e)
            {
                LOG_ERROR(log, "Coro " << num << ": " << e.displayText());
            }
        };

        for (size_t i = 0; i < ports.size(); ++i)
        {
            const std::string & port = ports[i];
            boost::asio::spawn(io_service, [=](boost::asio::yield_context yield) { work(i, port, yield); });
        }

        io_service.run();
    });

    for (const std::string & str : coro)
    {
        std::cout << "Got " << str << std::endl;
    }

    return 0;
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
