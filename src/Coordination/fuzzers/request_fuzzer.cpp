#include <iostream>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperLogStore.h>
#include <memory>


using namespace DB;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    try {
        ResponsesQueue queue;
        SnapshotsQueue snapshots_queue{1};

        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        auto state_machine = std::make_shared<KeeperStateMachine>(queue, snapshots_queue, "./snapshots", settings);
        state_machine->init();
        DB::KeeperLogStore changelog("./logs", settings->rotate_log_storage_interval, true);
        changelog.init(state_machine->last_commit_index() + 1, settings->reserved_log_items);

        nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(size));
        nuraft::buffer_serializer bs(ret);
        bs.put_raw(data, size);

        state_machine->commit(1, *ret);

    } catch (...) {
        return 0;
    }
    return 0;
}
catch (...)
{
    return 1;
}
