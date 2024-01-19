#include <Common/FoundationDB/fdb_error_definitions.h>

#include "FDBExtended.h"
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

KeeperCleaner::KeeperCleaner(const KeeperKeys & keys_, FDBTransaction * tr) : log(&Poco::Logger::get("FDBKeeperCleaner")), keys(keys_)
{
    cleaner(fdb_manage_object(tr))
        .start(
            [this](std::exception_ptr eptr)
            {
                if (eptr)
                    tryLogException(eptr, log, "Cleaner stopped unexpectedly");
            },
            clear_trx_cancel.newToken());
}

Coroutine::Task<void> KeeperCleaner::cleaner(std::shared_ptr<FDBTransaction> trx)
{
    try
    {
        while (true)
        {
            try
            {
                fdb_transaction_reset(trx.get());
                co_await doCleanRound(*trx);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Unexpected error in clean round");
            }
            co_await fdb_delay(KEEPER_CLEANER_ROUND_DURATION_S);
        }
    }
    catch (const FoundationDBException & e)
    {
        if (e.code != FDBErrorCode::operation_cancelled)
            throw;
    }
}

Coroutine::Task<void> KeeperCleaner::doCleanRound(FDBTransaction & trx)
{
    try
    {
        auto expire_before_ts = co_await tryGetRound(trx);
        fdb_transaction_reset(&trx);

        /// Fetch all expired session keys
        std::vector<String> session_keys;
        String session_key_begin = keys.getSessionKey(BigEndianTimestamp{}, FDBVersionstamp{}, false);
        String session_key_end = keys.getSessionKey(expire_before_ts, FDBVersionstamp{}, false);

        fdb_bool_t more = true;
        while (more)
        {
            auto future = co_await fdb_transaction_get_range(
                &trx,
                FDB_KEYSEL_FIRST_GREATER_THAN_STRING(session_key_begin),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(session_key_end),
                0,
                0,
                FDB_STREAMING_MODE_WANT_ALL,
                0,
                true,
                false);

            const FDBKeyValue * kvs;
            int kvs_len;
            throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_len, &more));

            for (int i = 0; i < kvs_len; i++)
                session_keys.emplace_back(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length);

            if (kvs_len > 0)
                session_key_begin.assign(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length);
        }
        fdb_transaction_reset(&trx);

        for (auto & session_key : session_keys)
            co_await clearSession(trx, session_key);

        LOG_TRACE(log, "Current clean round done");
    }
    catch (OtherCleanerRunningException &)
    {
        LOG_TRACE(log, "Other cleaner is running");
    }
}

Coroutine::Task<BigEndianTimestamp> KeeperCleaner::tryGetRound(FDBTransaction & trx)
{
    auto cleaner_key = keys.getCleaner();

    auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(cleaner_key), false);

    fdb_bool_t exists_cleaner;
    const uint8_t * cleaner_bytes;
    int cleaner_bytes_len;
    throwIfFDBError(fdb_future_get_value(future.get(), &exists_cleaner, &cleaner_bytes, &cleaner_bytes_len));

    assert(!exists_cleaner || cleaner_bytes_len == sizeof(LittleEndianTimestamp));
    const auto & cur_round_expire_ts = *reinterpret_cast<const LittleEndianTimestamp *>(cleaner_bytes);
    auto cur_ts = LittleEndianTimestamp::now();

    if (exists_cleaner && cur_round_expire_ts > cur_ts)
        throw OtherCleanerRunningException();

    auto expire_session_ts = cur_ts.toBigEndian();

    LittleEndianTimestamp round_expire_ts = cur_ts.addMS(KEEPER_CLEANER_ROUND_DURATION_MS);
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(cleaner_key), FDB_VALUE_FROM_POD(round_expire_ts));

    try
    {
        co_await fdb_transaction_commit(&trx);
    }
    catch (const FoundationDBException & e)
    {
        if (e.code == FDBErrorCode::not_committed)
            throw OtherCleanerRunningException();
    }

    LOG_TRACE(log, "I am cleaner now");
    co_return expire_session_ts;
}

Coroutine::Task<void> KeeperCleaner::clearSession(FDBTransaction & trx, const String & session_key)
{
    String ephe_range_begin = keys.getEphemeral(KeeperKeys::extractSessionFromSessionKey(session_key, false), "");
    String ephe_range_end = ephe_range_begin + '\xff';
    int ephe_iterator = 1;

    fdb_transaction_reset(&trx);

    /// Get session paths
    std::vector<String> paths;
    for (fdb_bool_t more = true; more;)
    {
        auto future = co_await fdb_transaction_get_range(
            &trx,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(ephe_range_begin),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(ephe_range_end),
            0,
            0,
            FDB_STREAMING_MODE_ITERATOR,
            ephe_iterator++,
            false,
            false);

        const FDBKeyValue * kvs;
        int kvs_len;
        throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_len, &more));

        const int prefix_size = static_cast<int>(keys.getEphemeralPrefixSize());

        for (int i = 0; i < kvs_len; i++)
        {
            const auto & kv = kvs[i];
            if (kv.key_length < prefix_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Key length of ephemeral key is too short ({} < {})", kv.key_length, prefix_size);
            paths.emplace_back(reinterpret_cast<const char *>(kv.key) + prefix_size, kv.key_length - prefix_size);
        }

        if (kvs_len > 0)
            ephe_range_begin = StringRef(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length).toString();
    }

    /// Clear session keys and path keys
    fdb_transaction_clear_range(&trx, FDB_KEY_FROM_STRING(ephe_range_begin), FDB_KEY_FROM_STRING(ephe_range_end));
    fdb_transaction_clear(&trx, FDB_KEY_FROM_STRING(session_key));
    for (auto & path : paths)
        ZNodeLayer::removeUnsafeTrx(&trx, keys, path);

    co_await fdb_transaction_commit(&trx);

    LOG_DEBUG(log, "Clean {} nodes owned by session {}.", paths.size(), KeeperKeys::extractSessionFromSessionKey(session_key, false));
}

Coroutine::Task<void> KeeperCleaner::clean(FDBTransaction & tr, SessionID session)
{
    String target_session_key;

    /// Find session_key of session id
    fdb_bool_t more = true;
    auto session_key_prefix = keys.getSessionPrefix();
    auto session_key_begin = session_key_prefix;
    auto session_key_end = session_key_prefix + '\xff';
    while (target_session_key.empty() && more)
    {
        auto future = co_await fdb_transaction_get_range(
            &tr,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(session_key_begin),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(session_key_end),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            0,
            true,
            false);

        const FDBKeyValue * kvs;
        int kvs_len;
        throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_len, &more));

        for (int i = 0; i < kvs_len; i++)
        {
            std::string session_key(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length);
            if (KeeperKeys::extractSessionFromSessionKey(session_key, false) == session)
            {
                target_session_key = session_key;
                break;
            }
        }

        if (kvs_len > 0)
            session_key_begin.assign(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length);
    }
    if (target_session_key.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such session {}", session);

    /// Clean session
    LOG_DEBUG(log, "Clear session {} with key {}", session, fdb_print_key(target_session_key));
    co_await clearSession(tr, target_session_key);

    co_return;
}
}
