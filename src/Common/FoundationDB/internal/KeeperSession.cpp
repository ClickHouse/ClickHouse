#include <foundationdb/fdb_c.h>

#include "FDBExtended.h"
#include "KeeperConst.h"
#include "KeeperSession.h"

namespace DB::FoundationDB
{
KeeperSession::KeeperSession(KeeperKeys & keys_, FDBTransaction * tr) : keys(keys_), log(getLogger("FDBKeeperSession"))
{
    new_session_key_template = keys.getSessionKey(BigEndianTimestamp{}, FDBVersionstamp{}, true);
    heartbeatLoop(fdb_manage_object(tr))
        .start(
            [this](std::exception_ptr eptr)
            {
                if (eptr)
                    tryLogException(eptr, log, "Unexpected error during heartbeat");
            },
            heartbeat_trx_cancel.newToken());
}

Coroutine::Task<void> KeeperSession::heartbeatLoop(std::shared_ptr<FDBTransaction> trx)
{
    try
    {
        while (true)
        {
            fdb_transaction_reset(trx.get());

            newTimestamp() = BigEndianTimestamp::fromNow(KEEPER_SESSION_EXPIRE_AT_MS);
            if (cur_session_key.empty())
            {
                fdb_transaction_atomic_op(
                    trx.get(), FDB_KEY_FROM_STRING(new_session_key_template), nullptr, 0, FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);

                auto future_versionstamp = fdb_manage_object(fdb_transaction_get_versionstamp(trx.get()));
                co_await fdb_transaction_commit(trx.get());
                co_await *future_versionstamp;

                const uint8_t * vs_bytes;
                int vs_len;

                throwIfFDBError(fdb_future_get_key(future_versionstamp.get(), &vs_bytes, &vs_len));
                assert(vs_len == 10);

                auto & vs = newVersionstamp();
                memcpy(&vs.bytes, vs_bytes, 10);
            }
            else
            {
                auto future = co_await fdb_transaction_get(trx.get(), FDB_KEY_FROM_STRING(cur_session_key), false);

                fdb_bool_t exists;
                const uint8_t * data;
                int data_len;
                throwIfFDBError(fdb_future_get_value(future.get(), &exists, &data, &data_len));

                if (!exists)
                    throw KeeperException(Coordination::Error::ZSESSIONEXPIRED);

                fdb_transaction_clear(trx.get(), FDB_KEY_FROM_STRING(cur_session_key));
                fdb_transaction_set(trx.get(), FDB_KEY_FROM_STRING(new_session_key_template) - 4, nullptr, 0);

                co_await fdb_transaction_commit(trx.get());
            }

            applyNewSessionKey();
            co_await fdb_delay(KEEPER_HEARTBEAT_INTERVAL_S);
        }
    }
    catch (const FoundationDBException & e)
    {
        if (e.code != FDBErrorCode::operation_cancelled)
            tryLogCurrentException(log, "Unexpected error during heartbeat");
        expired.store(true);
        on_expired();
    }
    catch (...)
    {
        expired.store(true);
        tryLogCurrentException(log, "Unexpected error during heartbeat");
        on_expired();
    }
}

Coroutine::Task<SessionID> KeeperSession::currentSession(FDBTransaction & trx)
{
    if (expired)
        throw KeeperException(Coordination::Error::ZSESSIONEXPIRED);

    while (true)
    {
        if (cur_session_key.empty())
        {
            LOG_TRACE(log, "Session not inited, retry...");
        }
        else
        {
            auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(cur_session_key), false);

            fdb_bool_t exists;
            const uint8_t * data;
            int data_len;
            throwIfFDBError(fdb_future_get_value(future.get(), &exists, &data, &data_len));

            if (exists)
                co_return curSessionID();
            else
                LOG_TRACE(log, "Session expired, retry...");
        }

        co_await fdb_delay(KEEPER_SESSION_RETRY_INTERVAL_S);
    }
}
}
