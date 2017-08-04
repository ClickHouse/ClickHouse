#include <Client/Connection.cpp>
#include <Client/ConnectionPoolWithFailover.cpp>
#include <Common/Exception.h>
#include <Interpreters/Settings.h>
#include <Core/FieldVisitors.h>
#include <Common/Stopwatch.h>

#include <iostream>

using namespace DB;

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

    ConnectionPoolWithFailoverPtrs shard_pools;
    for (size_t i = 0; i < num_shards; ++i)
    {
        ConnectionPoolPtrs replica_pools;
        for (std::string replica : {"mtlog01-01-2t.yandex.ru", "mtlog01-01-2t.yandex.ru"})
            replica_pools.emplace_back(std::make_shared<ConnectionPool>(3, replica, 9000, "", "default", ""));

        shard_pools.emplace_back(std::make_shared<ConnectionPoolWithFailover>(
            std::move(replica_pools), LoadBalancing::RANDOM));
    }

    Stopwatch stopwatch;

    Settings settings;
    settings.max_parallel_replicas = num_replicas;

    for (size_t i = 0; i < num_shards; ++i)
    {

        auto connections = shard_pools[i]->getMany(&settings, PoolMode::GET_MANY);
        for (size_t r = 0; r < connections.size(); ++r)
        {
            auto & entry = connections[r];

            entry->sendQuery("SELECT 12 union all SELECT 34", "test_ztlpn", QueryProcessingStage::Complete, &settings);

            bool eof = false;
            while (!eof)
            {
                auto packet = entry->receivePacket();
                switch (packet.type)
                {
                case Protocol::Server::Data:
                    std::cerr << "SHARD " << i << " REPLICA " << r << " DATA" << std::endl;
                    if (packet.block)
                    {
                        for (size_t r = 0; r < packet.block.rows(); ++r)
                        {
                            std::cerr << "ROW " << r << " "
                                      << applyVisitor(FieldVisitorToString(), (*packet.block.safeGetByPosition(0).column)[r])
                                      << std::endl;
                        }
                    }
                    break;
                case Protocol::Server::Exception:
                    packet.exception->rethrow();
                case Protocol::Server::EndOfStream:
                    std::cerr << "SHARD " << i << " REPLICA " << r << " EOF" << std::endl;
                    eof = true;
                    break;
                default:
                    std::cerr << "SHARD " << i << " REPLICA " << r << " PACKET TYPE " << packet.type << std::endl;
                    break;
                }
            }
        }
    }

    std::cerr << "ELAPSED " << stopwatch.elapsedSeconds() << std::endl;
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
