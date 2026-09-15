#include <Coordination/tests/gtest_coordination_common.h>

#if USE_NURAFT

#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>

LogEntryPtr getLogEntry(const std::string & s, size_t term)
{
    DB::WriteBufferFromNuraftBuffer bufwriter;
    writeText(s, bufwriter);
    return nuraft::cs_new<nuraft::log_entry>(term, bufwriter.getBuffer());
}

void waitDurableLogs(nuraft::log_store & log_store)
{
    while (log_store.last_durable_index() != log_store.next_slot() - 1)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

void assertFileDeleted(std::string path)
{
    for (size_t i = 0; i < 100; ++i)
    {
        if (!fs::exists(path))
            return;

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    FAIL() << "File " << path << " was not removed";
}

nuraft::ptr<nuraft::log_entry>
getLogEntryFromZKRequest(size_t term, int64_t session_id, int64_t zxid, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::KeeperRequestForSession request_for_session;
    request_for_session.session_id = session_id;
    request_for_session.zxid = zxid;
    request_for_session.request = request;
    auto buffer = DB::IKeeperStateMachine::getZooKeeperLogEntry(request_for_session);
    return nuraft::cs_new<nuraft::log_entry>(term, buffer);
}

#endif
