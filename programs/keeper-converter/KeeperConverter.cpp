#include <iostream>
#include <boost/program_options.hpp>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/ZooKeeperDataReader.h>
#include <Coordination/KeeperContext.h>
#include <Common/TerminalSize.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <Disks/DiskLocal.h>


int mainEntryClickHouseKeeperConverter(int argc, char ** argv)
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help,h", "produce help message")
        ("zookeeper-logs-dir", po::value<std::string>(), "Path to directory with ZooKeeper logs")
        ("zookeeper-snapshots-dir", po::value<std::string>(), "Path to directory with ZooKeeper snapshots")
        ("output-dir", po::value<std::string>(), "Directory to place output clickhouse-keeper snapshot")
    ;
    po::variables_map options;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), options);
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    LoggerPtr logger = getLogger("KeeperConverter");
    logger->setChannel(console_channel);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " --zookeeper-logs-dir /var/lib/zookeeper/data/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/data/version-2 --output-dir /var/lib/clickhouse/coordination/snapshots" << std::endl;
        std::cout << desc << std::endl;
        return 0;
    }

    try
    {
        auto keeper_context = std::make_shared<KeeperContext>(true, std::make_shared<CoordinationSettings>());
        keeper_context->setDigestEnabled(true);
        keeper_context->setSnapshotDisk(std::make_shared<DiskLocal>("Keeper-snapshots", options["output-dir"].as<std::string>()));

        /// TODO(hanfei): support rocksdb here
        DB::KeeperMemoryStorage storage(/* tick_time_ms */ 500, /* superdigest */ "", keeper_context, /* initialize_system_nodes */ false);

        DB::deserializeKeeperStorageFromSnapshotsDir(storage, options["zookeeper-snapshots-dir"].as<std::string>(), logger);
        storage.initializeSystemNodes();

        DB::deserializeLogsAndApplyToStorage(storage, options["zookeeper-logs-dir"].as<std::string>(), logger);
        DB::SnapshotMetadataPtr snapshot_meta = std::make_shared<DB::SnapshotMetadata>(storage.getZXID(), 1, std::make_shared<nuraft::cluster_config>());
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snapshot(&storage, snapshot_meta);

        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> manager(1, keeper_context);
        auto snp = manager.serializeSnapshotToBuffer(snapshot);
        auto file_info = manager.serializeSnapshotBufferToDisk(*snp, storage.getZXID());
        std::cout << "Snapshot serialized to path:" << fs::path(file_info->disk->getPath()) / file_info->path << std::endl;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}
