#pragma once

#include <future>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>

#include "KeeperCommon.h"
#include "AsyncTrx.h"

namespace DB::FoundationDB
{
class KeeperCleaner
{
public:
    KeeperCleaner(BackgroundSchedulePool & pool, const KeeperKeys & keys_, FDBTransaction * tr);
    ~KeeperCleaner();

    // Create a cleaner without cleaner thread. Useful for testing.
    explicit KeeperCleaner(const KeeperKeys & keys_);

    // clean specfic session. clean() will take the ownership of tr.
    // clean() is inefficient. You should only use it in test.
    std::future<void> clean(SessionID session, FDBTransaction * tr);

private:
    Poco::Logger * log;
    const KeeperKeys & keys;

    AsyncTrxCancelSource clear_trx_cancel;
    BackgroundSchedulePoolTaskHolder clear_task;
    AsyncTrx::Ptr clear_trx;

    AsyncTrx::Ptr buildClearTrx(FDBTransaction * tr);
    void tryGetRound(AsyncTrxBuilder & trxb, AsyncTrxVar<BigEndianTimestamp> var_round_start_ts);
    void clearSessions(AsyncTrxBuilder & trxb, AsyncTrxVar<std::deque<String>> var_session_keys);

    class OtherCleanerRunningException : public std::exception
    {
    public:
        const char * what() const noexcept override { return "Other cleaner is running"; }
    };
};
}
