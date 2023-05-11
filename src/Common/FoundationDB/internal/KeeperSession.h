#pragma once

#include <shared_mutex>
#include <Core/BackgroundSchedulePool.h>
#include <Common/FoundationDB/fdb_error_definitions.h>

#include "AsyncTrx.h"
#include "KeeperCommon.h"

namespace DB::FoundationDB
{
class KeeperSession
{
public:
    /// KeeperSession will take the ownership of tr
    KeeperSession(BackgroundSchedulePool & pool, KeeperKeys & keys_, FDBTransaction * tr);
    ~KeeperSession();

    void currentSession(AsyncTrxBuilder & trxb, AsyncTrxVar<SessionID> var_session);
    SessionID currentSessionSync() { return curSessionID(); }

    bool isExpired() const { return expired; }

    /// onExpired will be invoked on session expired.
    /// It should be quick.
    std::function<void()> onExpired;

private:
    KeeperKeys & keys;
    Poco::Logger * log;

    AsyncTrxCancelSource heartbeat_trx_cancel;
    AsyncTrx::Ptr heartbeat_trx;
    BackgroundSchedulePool::TaskHolder heartbeat_task;

    void buildHeartbeatTrx(FDBTransaction * tr);
    void heartbeat();

    /// Session expired
    std::atomic<bool> expired = false;

    /// Session key without vs index
    String cur_session_key;
    std::shared_mutex cur_session_key_mutex;
    SessionID curSessionID()
    {
        std::shared_lock lock(cur_session_key_mutex);
        if (cur_session_key.empty())
            return 0;
        else
            return KeeperKeys::extractSessionFromSessionKey(cur_session_key, false);
    }
    BigEndianTimestamp curExpire()
    {
        std::shared_lock lock(cur_session_key_mutex);
        if (cur_session_key.empty())
            return BigEndianTimestamp{};
        else
            return KeeperKeys::getTSFromSessionKey(cur_session_key, false);
    }

    /// Session key with vs index (last 4 bytes)
    String new_session_key_template;
    BigEndianTimestamp & newTimestamp() { return KeeperKeys::getTSFromSessionKey(new_session_key_template, true); }
    FDBVersionstamp & newVersionstamp() { return KeeperKeys::getVSFromSessionKey(new_session_key_template, true); }
    void applyNewSessionKey()
    {
        std::unique_lock lock(cur_session_key_mutex);
        cur_session_key.assign(new_session_key_template.data(), new_session_key_template.size() - 4);
    }
};
}
