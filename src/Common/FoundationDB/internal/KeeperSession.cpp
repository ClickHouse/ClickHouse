#include <foundationdb/fdb_c.h>

#include "KeeperConst.h"
#include "KeeperSession.h"

namespace DB::FoundationDB
{
KeeperSession::KeeperSession(BackgroundSchedulePool & pool, KeeperKeys & keys_, FDBTransaction * tr)
    : keys(keys_), log(&Poco::Logger::get("FDBKeeperSession"))
{
    new_session_key_template = keys.getSessionKey(BigEndianTimestamp{}, FDBVersionstamp{}, true);
    buildHeartbeatTrx(tr);

    heartbeat_task = pool.createTask("FDBKeeper Session Heartbeat", [this]() { this->heartbeat(); });
    heartbeat_task->schedule();
}

KeeperSession::~KeeperSession()
{
    heartbeat_task->deactivate();
}

void KeeperSession::heartbeat()
{
    LOG_TRACE(log, "FDBKeeper Session Heartbeat");

    if (!heartbeat_trx)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Heartbeat trx is released");

    heartbeat_trx->ctx.reset();
    AsyncTrx::startInBackground(heartbeat_trx);
}

void KeeperSession::buildHeartbeatTrx(FDBTransaction * tr)
{
    AsyncTrxBuilder trxb;

    auto var_new_vs = trxb.var<bool>();
    trxb.then(TRX_STEP(this, var_new_vs)
        {
            auto & new_vs = *ctx.getVar(var_new_vs);

            if (cur_session_key.empty())
            {
                new_vs = true;
                ctx.gotoCur(2);
            }

            return fdb_transaction_get(ctx.getTrx(), FDB_KEY_FROM_STRING(cur_session_key), false);
        })
        .then(TRX_STEP(this, var_new_vs)
        {
            fdb_bool_t exists;
            const uint8_t * data;
            int data_len;
            throwIfFDBError(fdb_future_get_value(f, &exists, &data, &data_len));

            auto & new_vs = *ctx.getVar(var_new_vs);
            new_vs = !exists;

            if (exists)
                fdb_transaction_clear(ctx.getTrx(), FDB_KEY_FROM_STRING(cur_session_key));
            else
                throw KeeperException(Coordination::Error::ZSESSIONEXPIRED);
            return nullptr;
        })
        .then(TRX_STEP(this, var_new_vs)
        {
            newTimestamp() = BigEndianTimestamp::fromNow(KEEPER_SESSION_EXPIRE_AT_MS);
            auto & new_vs = *ctx.getVar(var_new_vs);

            if (new_vs)
                fdb_transaction_atomic_op(
                    ctx.getTrx(), FDB_KEY_FROM_STRING(new_session_key_template), nullptr, 0, FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
            else
                fdb_transaction_set(ctx.getTrx(), FDB_KEY_FROM_STRING(new_session_key_template) - 4, nullptr, 0);

            return nullptr;
        });
    auto var_vs = trxb.var<FDBVersionstamp>();
    trxb.commit(var_vs);

    heartbeat_trx = trxb.build(
        tr,
        [this, log = log, var_vs, var_new_vs](AsyncTrx::Context & ctx, std::exception_ptr eptr)
        {
            if (eptr)
            {
                expired.store(true);
                tryLogException(eptr, log, "Unexpected error during heartbeat");
                onExpired();
            }
            else
            {
                auto & new_vs = *ctx.getVar(var_new_vs);
                if (new_vs)
                    newVersionstamp() = *ctx.getVar(var_vs);
                applyNewSessionKey();

                heartbeat_task->scheduleAfter(KEEPER_HEARTBEAT_INTERVAL_MS);
                LOG_TRACE(log, "{} session: {}, expire: {}", new_vs ? "New" : "Renew", curSessionID(), curExpire().format());
            }
        },
        heartbeat_trx_cancel.getToken());
}

void KeeperSession::currentSession(AsyncTrxBuilder & trxb, AsyncTrxVar<SessionID> var_session)
{
    trxb.then(TRX_STEP(this, log = log)
        {
            if (expired)
                throw KeeperException(Coordination::Error::ZSESSIONEXPIRED);

            if (cur_session_key.empty())
            {
                LOG_TRACE(log, "Session not inited, retry...");
                ctx.reset();
                return fdb_delay(KEEPER_SESSION_RETRY_INTERVAL_S);
            }

            return fdb_transaction_get(ctx.getTrx(), FDB_KEY_FROM_STRING(cur_session_key), false);
        })
        .then(TRX_STEP(this, log = log, var_session)
        {
            fdb_bool_t exists;
            const uint8_t * data;
            int data_len;
            throwIfFDBError(fdb_future_get_value(f, &exists, &data, &data_len));

            if (!exists)
            {
                LOG_TRACE(log, "Session expired, retry...");
                ctx.reset();
                return fdb_delay(KEEPER_SESSION_RETRY_INTERVAL_S);
            }

            *ctx.getVar(var_session) = curSessionID();
            return nullptr;
        });
}
}
