#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>

#include "Coroutine.h"
#include "KeeperCommon.h"

namespace DB::FoundationDB
{
class KeeperCleaner
{
public:
    KeeperCleaner(const KeeperKeys & keys_, FDBTransaction * tr);
    ~KeeperCleaner() = default;

    // clean specific session. clean() will take the ownership of tr.
    // clean() is inefficient. You should only use it in test.
    Coroutine::Task<void> clean(FDBTransaction & tr, SessionID session);

private:
    Poco::Logger * log;
    const KeeperKeys & keys;

    AsyncTrxTracker clear_trx_cancel;

    Coroutine::Task<void> cleaner(std::shared_ptr<FDBTransaction> trx);
    Coroutine::Task<void> doCleanRound(FDBTransaction & trx);
    Coroutine::Task<BigEndianTimestamp> tryGetRound(FDBTransaction & trx);
    Coroutine::Task<void> clearSession(FDBTransaction & trx, const String & session_key);

    class OtherCleanerRunningException : public std::exception
    {
    public:
        const char * what() const noexcept override { return "Other cleaner is running"; }
    };
};
}
