#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>

#include <libnuraft/buffer.hxx>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <unistd.h>


/// The first byte selects the input and the nodes storage:
/// - a snapshot after decompression, which Keeper reads from its disk or receives from the leader;
/// - a log entry, which Keeper reads from its changelog or receives from the leader in `append_entries`.

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool storage_memory_only;
    extern const CoordinationSettingsBool use_lsmt_storage;
}

using namespace DB;

namespace
{

struct Keeper
{
    KeeperContextPtr keeper_context;
    SnapshotsQueue snapshots_queue{1};
    std::shared_ptr<KeeperStateMachine> state_machine;

    Keeper(bool use_lsmt_storage, const std::string & path)
    {
        auto settings = std::make_shared<CoordinationSettings>();
        (*settings)[CoordinationSetting::use_lsmt_storage] = use_lsmt_storage;
        (*settings)[CoordinationSetting::storage_memory_only] = true;

        keeper_context = std::make_shared<KeeperContext>(true, settings);
        std::filesystem::create_directories(path);
        keeper_context->setSnapshotDisk(std::make_shared<DiskLocal>("SnapshotDisk", path));
        keeper_context->setLogDisk(std::make_shared<DiskLocal>("LogDisk", path));
        keeper_context->setStateFileDisk(std::make_shared<DiskLocal>("StateFile", path));
        keeper_context->setLocalLogsPreprocessed();

        state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, keeper_context, nullptr);
    }
};

std::unique_ptr<Keeper> keepers[2];

}

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    MainThreadStatus::getInstance();

    /// The disks stay empty: the snapshots and log entries come from the input.
    static const auto path = std::filesystem::temp_directory_path() / ("keeper_snapshot_changelog_fuzzer_" + std::to_string(getpid()));
    keepers[0] = std::make_unique<Keeper>(false, path / "mem");
    keepers[1] = std::make_unique<Keeper>(true, path / "lsmt");

    /// libFuzzer starts many processes for a merge, each with its own directory.
    std::atexit([]
    {
        for (auto & keeper : keepers)
            keeper.reset();
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    });
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    if (size < 1)
        return 0;

    const bool snapshot = data[0] & 1;
    Keeper & keeper = *keepers[(data[0] >> 1) & 1];
    const bool allow_orphaned_nodes_removal = data[0] & 4;

    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        if (snapshot)
        {
            auto in = std::make_unique<ReadBufferFromMemory>(data + 1, size - 1);
            KeeperSnapshotReader reader(std::move(in), keeper.keeper_context);
            reader.allow_orphaned_nodes_removal = allow_orphaned_nodes_removal;
            auto storage = KeeperStorage::create(500, "", keeper.keeper_context, /*initialize_system_nodes=*/false);
            storage->loadFromSnapshot(reader);
        }
        else
        {
            auto entry = nuraft::buffer::alloc(size - 1);
            memcpy(entry->data_begin(), data + 1, size - 1);
            std::ignore = keeper.state_machine->parseRequestBatch(*entry, /*final=*/true);
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed snapshots and log entries are rejected with exceptions.
    }

    return 0;
}
