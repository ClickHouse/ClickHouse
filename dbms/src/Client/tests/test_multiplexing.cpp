#include <Client/Connection.cpp>
#include <Client/ConnectionPoolWithFailover.cpp>
#include <Common/Exception.h>
#include <Interpreters/Settings.h>
#include <Core/FieldVisitors.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromAsioSocket.h>

#include <daemon/OwnPatternFormatter.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/AutoPtr.h>

#include <boost/fiber/all.hpp>
#include <boost/asio.hpp>

#include <iostream>
#include <thread>

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
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " NUM_SHARDS MAX_PARALLEL_REPLICAS" << std::endl;
        return 1;
    }

    setupLogging("trace");
    auto * log = &Logger::get("main");

    size_t num_shards = std::stoul(argv[1]);
    size_t num_replicas = std::stoul(argv[2]);

    boost::asio::io_service io_service;
    boost::fibers::use_scheduling_algorithm<boost::fibers::asio::round_robin>(io_service);

    auto work = [&](size_t num, const std::string & port)
    {
        try
        {
            LOG_TRACE(log, "Fiber " << num << " starting.");

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver resolver(io_service);
            boost::asio::ip::tcp::resolver::query query("localhost", port);
            auto iter = resolver.async_resolve(query, boost::fibers::asio::yield[ec]);
            if (ec)
                throw Exception("Could not resolve.");

            ++iter; /// skip ipv6
            LOG_TRACE(log, "Fiber " << num << " resolved address to " << iter->endpoint());

            boost::asio::ip::tcp::socket socket(io_service);
            socket.async_connect(iter->endpoint(), boost::fibers::asio::yield[ec]);
            if (ec)
                throw Exception("Could not connect.");

            LOG_TRACE(log, "Fiber " << num << " connected.");

            ReadBufferFromAsioSocket read_buf(socket);

            while (!read_buf.eof())
            {
                String str;
                readString(str, read_buf);
                LOG_INFO(log, "Fiber " << num << " read string: " << str);

                char delim;
                readChar(delim, read_buf);
            }

            LOG_TRACE(log, "Fiber " << num << " ended.");
        }
        catch (Exception & e)
        {
            LOG_ERROR(log, "Fiber " << num << ": " << e.displayText());
        }
    };

    boost::fibers::fiber([&]
    {
        boost::fibers::fiber f1(work, 1, "1234");
        boost::fibers::fiber f2(work, 2, "1235");

        f1.join();
        f2.join();

        io_service.stop();
    }).detach();

    io_service.run();

    return 1;

    ConnectionPoolWithFailoverPtrs shard_pools;
    for (size_t i = 0; i < num_shards; ++i)
    {
        ConnectionPoolPtrs replica_pools;
        for (std::string replica : {"mtlog01-01-2t.yandex.ru", "mtlog01-01-2t.yandex.ru"})
            replica_pools.emplace_back(std::make_shared<ConnectionPool>(3, replica, 9000, "", "default", ""));

        shard_pools.emplace_back(std::make_shared<ConnectionPoolWithFailover>(
            std::move(replica_pools), LoadBalancing::RANDOM));
    }

    for (size_t round = 0; round < 3; ++round)
    {

    Stopwatch stopwatch;

    Settings settings;
    settings.max_parallel_replicas = num_replicas;

    LOG_TRACE(log, "Starting...");

    std::vector<std::thread> shard_threads;

    for (size_t i = 0; i < num_shards; ++i)
    {
        shard_threads.emplace_back([&](size_t i)
        {

        auto connections = shard_pools[i]->getMany(&settings, PoolMode::GET_MANY);

        std::vector<std::thread> replica_threads;

        for (size_t r = 0; r < connections.size(); ++r)
        {
            replica_threads.emplace_back([&](size_t r)
            {

            auto & entry = connections[r];

            LOG_TRACE(log, "SHARD " << i << " REPLICA " << r << " sending query...");
            entry->sendQuery("SELECT 12 union all SELECT 34", "", QueryProcessingStage::Complete, &settings);

            bool eof = false;
            while (!eof)
            {
                auto packet = entry->receivePacket();
                switch (packet.type)
                {
                case Protocol::Server::Data:
                    LOG_TRACE(log, "SHARD " << i << " REPLICA " << r << " DATA rows: " << packet.block.rows());
                    for (size_t r = 0; r < packet.block.rows(); ++r)
                    {
                        LOG_TRACE(log,
                            "SHARD " << i << " REPLICA " << r << " ROW " << r << " "
                            << applyVisitor(FieldVisitorToString(), (*packet.block.safeGetByPosition(0).column)[r]));
                    }
                    break;
                case Protocol::Server::Exception:
                    LOG_ERROR(log, packet.exception->displayText());
                    break;
                case Protocol::Server::EndOfStream:
                    LOG_TRACE(log, "SHARD " << i << " REPLICA " << r << " EOF");
                    eof = true;
                    break;
                default:
                    LOG_TRACE(log, "SHARD " << i << " REPLICA " << r << " PACKET TYPE " << packet.type);
                    break;
                }
            }

            }, r);
        }

        for (auto & thread : replica_threads)
            thread.join();

        }, i);
    }

    for (auto & thread : shard_threads)
        thread.join();

    LOG_INFO(log, "ELAPSED " << stopwatch.elapsedSeconds());

    }
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
