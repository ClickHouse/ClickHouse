// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorageImpl.h>
#include <Common/assert_cast.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Exception.h>
#include <libnuraft/nuraft.hxx>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/Changelog.h>
#include <Common/logger_useful.h>
#include <Disks/DiskLocal.h>

using namespace Coordination;
using namespace DB;

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool compress_logs;
}

namespace
{

void dumpMachine(std::shared_ptr<KeeperStateMachine> machine)
{
    auto & storage = machine->getStorageUnsafe();
    std::queue<std::string> keys;
    keys.push("/");

    while (!keys.empty())
    {
        auto key = keys.front();
        keys.pop();
        std::cout << key << "\n";
        KeeperNodeStats stats;
        std::string data;
        if (!storage.nodes_storage->getCommittedNodeSimple(key, &stats, &data))
            continue;
        std::cout << "\tStat: {version: " << stats.version <<
            ", mtime: " << stats.mtime <<
            ", emphemeralOwner: " << stats.getEphemeralOwner() <<
            ", czxid: " << stats.czxid <<
            ", mzxid: " << stats.mzxid <<
            ", numChildren: " << stats.getNumChildren() <<
            ", dataLength: " << stats.data_size <<
            "}" << std::endl;
        std::cout << "\tData: " << data << std::endl;

        for (const auto & child : storage.nodes_storage->listCommittedChildrenNames(key))
        {
            if (key == "/")
                keys.push(key + child);
            else
                keys.push(key + "/" + child);
        }
    }
    std::cout << std::flush;
}

}

int mainEntryClickHouseKeeperDataDumper(int argc, char ** argv);
int mainEntryClickHouseKeeperDataDumper(int argc, char ** argv)
{
    if (argc != 3)
    {
        std::cerr << "usage: " << argv[0] << " snapshotpath logpath" << std::endl;
        return 3;
    }

    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    auto logger = getLogger("keeper-dumper");
    SnapshotsQueue snapshots_queue{1};
    CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
    KeeperContextPtr keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", argv[2]));
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", argv[1]));

    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, keeper_context, nullptr);
    state_machine->init();
    size_t last_commited_index = state_machine->last_commit_index();

    LOG_INFO(logger, "Last committed index: {}", last_commited_index);

    DB::KeeperLogStore changelog(
        LogFileSettings{.force_sync = true, .compress_logs = (*settings)[DB::CoordinationSetting::compress_logs], .rotate_interval = 10000000},
        FlushSettings(),
        keeper_context);
    changelog.init(last_commited_index, 10000000000UL); /// collect all logs
    if (changelog.size() == 0)
        LOG_INFO(logger, "Changelog empty");
    else
        LOG_INFO(logger, "Last changelog entry {}", changelog.next_slot() - 1);

    for (size_t i = last_commited_index + 1; i < changelog.next_slot(); ++i)
    {
        if (changelog.entry_at(i)->get_val_type() == nuraft::log_val_type::app_log)
        {
            state_machine->pre_commit(i, changelog.entry_at(i)->get_buf());
            state_machine->commit(i, changelog.entry_at(i)->get_buf());
        }
    }

    dumpMachine(state_machine);

    return 0;
}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
