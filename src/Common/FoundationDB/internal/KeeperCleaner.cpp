#include <Common/FoundationDB/fdb_error_definitions.h>

#include "KeeperCleaner.h"
#include "KeeperConst.h"
#include "ZNodeLayer.h"

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
}

namespace DB::FoundationDB
{
/// Timestamp in milliseconds
struct LittleEndianTimestamp
{
    static_assert(std::endian::native == std::endian::little);

    Int64 t;

    BigEndianTimestamp toBigEndian() const { return BigEndianTimestamp{boost::endian::native_to_big(t)}; }

    LittleEndianTimestamp addMS(Int64 millisecond) const { return LittleEndianTimestamp{t + millisecond}; }

    bool operator<(const LittleEndianTimestamp & other) const { return t < other.t; }
    bool operator==(const LittleEndianTimestamp & other) const { return t == other.t; }
    bool operator>(const LittleEndianTimestamp & other) const { return t > other.t; }

    static LittleEndianTimestamp now() { return LittleEndianTimestamp{Poco::Timestamp().epochMicroseconds() / 1000}; }
};

KeeperCleaner::KeeperCleaner(BackgroundSchedulePool & pool, const KeeperKeys & keys_, FDBTransaction * tr)
    : log(&Poco::Logger::get("FDBKeeperCleaner")), keys(keys_)
{
    clear_task = pool.createTask(
        "FDBKeeperCleaner",
        [this]()
        {
            clear_trx->ctx.reset();
            AsyncTrx::startInBackground(clear_trx);
        });

    clear_trx = buildClearTrx(tr);

    clear_task->schedule();
}

KeeperCleaner::~KeeperCleaner()
{
    if (clear_task)
        clear_task->deactivate();
}

AsyncTrx::Ptr KeeperCleaner::buildClearTrx(FDBTransaction * tr)
{
    AsyncTrxBuilder trxb;

    auto var_expire_before_ts = trxb.varDefault<BigEndianTimestamp>(BigEndianTimestamp{});

    tryGetRound(trxb, var_expire_before_ts);

    auto var_session_keys = trxb.var<std::deque<String>>();
    auto var_session_key_begin = trxb.var<String>();
    auto var_session_key_end = trxb.var<String>();

    /// Fetch all expired session keys
    trxb.then(TRX_STEP(local_keys = keys, var_expire_before_ts, var_session_keys, var_session_key_begin, var_session_key_end)
    {
        auto & session_key_begin = *ctx.getVar(var_session_key_begin);
        auto & session_key_end = *ctx.getVar(var_session_key_end);

        /// First iterator
        if (!f)
        {
            auto & expire_before_ts = *ctx.getVar(var_expire_before_ts);
            session_key_begin = local_keys.getSessionKey(BigEndianTimestamp{}, FDBVersionstamp{}, false);
            session_key_end = local_keys.getSessionKey(expire_before_ts, FDBVersionstamp{}, false);
        }
        else
        {
            const FDBKeyValue * kvs;
            int kvs_len;
            fdb_bool_t more;
            throwIfFDBError(fdb_future_get_keyvalue_array(f, &kvs, &kvs_len, &more));

            auto & session_keys = *ctx.getVar(var_session_keys);

            for (int i = 0; i < kvs_len; i++)
                session_keys.emplace_back(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length);

            if (!more)
                return nullptr;

            if (kvs_len > 0)
                session_key_begin.assign(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length);
        }

        ctx.gotoCur(0);
        return fdb_transaction_get_range(
            ctx.getTrx(),
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(session_key_begin),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(session_key_end),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            0,
            true,
            false);
    });

    clearSessions(trxb, var_session_keys);

    return trxb.build(
        tr,
        [lambda_clear_task = clear_task->shared_from_this(), lambda_log = this->log](AsyncTrxContext &, std::exception_ptr eptr)
        {
            if (eptr)
            {
                try
                {
                    std::rethrow_exception(eptr);
                }
                catch (OtherCleanerRunningException &)
                {
                    LOG_TRACE(lambda_log, "Other cleaner is running");
                }
                catch (...)
                {
                    tryLogException(eptr, lambda_log, "Unexpected error in cleaner trx");
                }
            }
            else
            {
                LOG_TRACE(lambda_log, "Current clean round done");
            }

            lambda_clear_task->scheduleAfter(KEEPER_CLEANER_ROUND_DURATION_MS);
        },
        clear_trx_cancel.getToken());
}

void KeeperCleaner::tryGetRound(AsyncTrxBuilder & trxb, AsyncTrxVar<BigEndianTimestamp> var_round_start_ts)
{
    auto cleaner_key = keys.getCleaner();

    trxb.then(TRX_STEP(cleaner_key) { return fdb_transaction_get(ctx.getTrx(), FDB_KEY_FROM_STRING(cleaner_key), false); })
        .then(TRX_STEP(var_round_start_ts, cleaner_key)
        {
            fdb_bool_t exists_cleaner;
            const uint8_t * cleaner_bytes;
            int cleaner_bytes_len;
            throwIfFDBError(fdb_future_get_value(f, &exists_cleaner, &cleaner_bytes, &cleaner_bytes_len));

            assert(!exists_cleaner || cleaner_bytes_len == sizeof(LittleEndianTimestamp));
            const auto & cur_round_expire_ts = *reinterpret_cast<const LittleEndianTimestamp *>(cleaner_bytes);
            auto cur_ts = LittleEndianTimestamp::now();

            if (exists_cleaner && cur_round_expire_ts > cur_ts)
                throw OtherCleanerRunningException();

            auto & expire_session_ts = *ctx.getVar(var_round_start_ts);
            expire_session_ts = cur_ts.toBigEndian();

            LittleEndianTimestamp round_expire_ts = cur_ts.addMS(KEEPER_CLEANER_ROUND_DURATION_MS);
            fdb_transaction_set(ctx.getTrx(), FDB_KEY_FROM_STRING(cleaner_key), FDB_VALUE_FROM_POD(round_expire_ts));

            return fdb_transaction_commit(ctx.getTrx());
        })
        .then(TRX_STEP(local_log = this->log)
        {
            auto error = fdb_future_get_error(f);
            if (error == FDBErrorCode::not_committed)
                throw OtherCleanerRunningException();
            throwIfFDBError(error);
            fdb_transaction_reset(ctx.getTrx());

            LOG_TRACE(local_log, "I am cleaner now");
            return nullptr;
        });
}

void KeeperCleaner::clearSessions(AsyncTrxBuilder & trxb, AsyncTrxVar<std::deque<String>> var_session_keys)
{
    auto var_paths = trxb.var<std::vector<String>>();
    auto var_ephe_range_begin = trxb.var<String>();
    auto var_ephe_range_end = trxb.var<String>();
    auto var_ephe_iterator = trxb.var<int>();

    auto init_step_tick = trxb.nextStepTick();
    trxb.then(TRX_STEP(local_keys = keys, var_session_keys, var_ephe_iterator, var_paths)
        {
            auto & session_keys = *ctx.getVar(var_session_keys);
            if (session_keys.empty())
            {
                /// Skip following steps
                ctx.gotoCur(3);
            }

            /// Init variables
            auto & ephe_iterator = *ctx.getVar(var_ephe_iterator);
            auto & paths = *ctx.getVar(var_paths);
            fdb_transaction_reset(ctx.getTrx());
            ephe_iterator = 1;
            paths.clear();

            return nullptr;
        })
        /// Get all paths, then clear ephemeral keys, session key and nodes keys
        .then(TRX_STEP(local_keys = keys, var_session_keys, var_ephe_iterator, var_ephe_range_begin, var_ephe_range_end, var_paths)
        {
            auto & session_keys = *ctx.getVar(var_session_keys);
            if (session_keys.empty())
                return nullptr;

            auto & session_key = session_keys.front();
            auto & ephe_range_begin = *ctx.getVar(var_ephe_range_begin);
            auto & ephe_range_end = *ctx.getVar(var_ephe_range_end);
            auto & ephe_iterator = *ctx.getVar(var_ephe_iterator);
            auto & paths = *ctx.getVar(var_paths);

            fdb_bool_t more = true;
            if (ephe_iterator == 1)
            {
                ephe_range_begin = local_keys.getEphemeral(KeeperKeys::extractSessionFromSessionKey(session_key, false), "");
                ephe_range_end = ephe_range_begin + '\xff';
            }
            else
            {
                const FDBKeyValue * kvs;
                int kvs_len;
                throwIfFDBError(fdb_future_get_keyvalue_array(f, &kvs, &kvs_len, &more));

                const int prefix_size = static_cast<int>(local_keys.getEphemeralPrefixSize());

                for (int i = 0; i < kvs_len; i++)
                {
                    const auto & kv = kvs[i];
                    if (kv.key_length < prefix_size)
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA, "Key length of ephemeral key is too short ({} < {})", kv.key_length, prefix_size);
                    paths.emplace_back(reinterpret_cast<const char *>(kv.key) + prefix_size, kv.key_length - prefix_size);
                }

                if (kvs_len > 0)
                    ephe_range_begin
                        = StringRef(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length).toString();
            }

            if (more)
            {
                ctx.gotoCur(0);
                return fdb_transaction_get_range(
                    ctx.getTrx(),
                    FDB_KEYSEL_FIRST_GREATER_THAN_STRING(ephe_range_begin),
                    FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(ephe_range_end),
                    0,
                    0,
                    FDB_STREAMING_MODE_ITERATOR,
                    ephe_iterator++,
                    false,
                    false);
            }
            else
            {
                ephe_range_begin = local_keys.getEphemeral(KeeperKeys::extractSessionFromSessionKey(session_key, false), "");
                fdb_transaction_clear_range(ctx.getTrx(), FDB_KEY_FROM_STRING(ephe_range_begin), FDB_KEY_FROM_STRING(ephe_range_end));
                fdb_transaction_clear(ctx.getTrx(), FDB_KEY_FROM_STRING(session_key));

                for (auto & path : paths)
                    ZNodeLayer::removeUnsafeTrx(ctx.getTrx(), local_keys, path);
                return fdb_transaction_commit(ctx.getTrx());
            }
        })
        /// Check error
        .then(TRX_STEP(var_session_keys, var_paths, local_log = this->log, init_step_tick)
        {
            auto error = fdb_future_get_error(f);
            if (error != 0)
                throwIfFDBError(error);

            auto & session_keys = *ctx.getVar(var_session_keys);
            auto & session_key = session_keys.front();
            auto & paths = *ctx.getVar(var_paths);
            LOG_DEBUG(
                local_log,
                "Clean {} nodes owned by session {} expired at {}.",
                paths.size(),
                KeeperKeys::extractSessionFromSessionKey(session_key, false),
                KeeperKeys::getTSFromSessionKey(session_key, false).format());

            session_keys.pop_front();
            if (!session_keys.empty())
                ctx.gotoSet(init_step_tick);

            return nullptr;
        });
}

KeeperCleaner::KeeperCleaner(const KeeperKeys & keys_) : log(&Poco::Logger::get("FDBKeeperCleaner")), keys(keys_)
{
}

std::future<void> KeeperCleaner::clean(SessionID session, FDBTransaction * tr)
{
    AsyncTrxBuilder trxb;
    auto var_session_keys = trxb.var<std::deque<String>>();
    auto var_session_key_begin = trxb.var<String>();
    auto var_session_key_end = trxb.var<String>();

    // Find session key of session
    trxb.then(
        TRX_STEP(session_key_prefix = keys.getSessionPrefix(), var_session_keys, var_session_key_begin, var_session_key_end, session)
        {
            auto & session_keys = *ctx.getVar(var_session_keys);
            auto & session_key_begin = *ctx.getVar(var_session_key_begin);
            auto & session_key_end = *ctx.getVar(var_session_key_end);

            // First loop
            if (!f)
            {
                session_key_begin = session_key_prefix;
                session_key_end = session_key_prefix + '\xff';
            }
            else
            {
                const FDBKeyValue * kvs;
                int kvs_len;
                fdb_bool_t more;
                throwIfFDBError(fdb_future_get_keyvalue_array(f, &kvs, &kvs_len, &more));

                for (int i = 0; i < kvs_len; i++)
                {
                    std::string session_key(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length);
                    if (KeeperKeys::extractSessionFromSessionKey(session_key, false) == session)
                    {
                        session_keys.emplace_back(session_key);
                        return nullptr;
                    }
                }

                if (!more)
                    return nullptr;

                if (kvs_len > 0)
                    session_key_begin.assign(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length);
            }

            ctx.gotoCur(0);
            return fdb_transaction_get_range(
                ctx.getTrx(),
                FDB_KEYSEL_FIRST_GREATER_THAN_STRING(session_key_begin),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(session_key_end),
                0,
                0,
                FDB_STREAMING_MODE_WANT_ALL,
                0,
                true,
                false);
        })
        .then(TRX_STEP(var_session_keys, session)
        {
            auto & session_keys = *ctx.getVar(var_session_keys);
            if (session_keys.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such session {}", session);
            return nullptr;
        });

    // Clear session
    clearSessions(trxb, var_session_keys);

    auto promise = std::make_shared<std::promise<void>>();
    trxb.exec(
        tr,
        [promise](AsyncTrx::Context &, std::exception_ptr eptr)
        {
            if (eptr)
                promise->set_exception(eptr);
            else
                promise->set_value();
        });
    return promise->get_future();
}
}
