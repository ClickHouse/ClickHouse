#include <Client/Connection.cpp>
#include <Client/ConnectionPoolWithFailover.cpp>
#include <Common/Exception.h>
#include <Interpreters/Settings.h>
#include <Core/FieldVisitors.h>
#include <Common/Stopwatch.h>

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

    size_t num_shards = std::stoul(argv[1]);
    size_t num_replicas = std::stoul(argv[2]);

    boost::fibers::fiber fiber([] { std::cerr << "ololo!" << std::endl; });
    fiber.join();

    boost::asio::io_service io;
    boost::asio::deadline_timer t(io, boost::posix_time::seconds(5));
    t.wait();
    std::cout << "Hello, world!" << std::endl;

    setupLogging("trace");
    auto * log = &Logger::get("main");

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
