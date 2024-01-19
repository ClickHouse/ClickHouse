#include "ZNodeLayer.h"
#include <chrono>
#include <Common/FoundationDB/fdb_error_definitions.h>

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace ProfileEvents
{
extern const Event ZooKeeperWatchResponse;
}

namespace DB::FoundationDB
{
using namespace Coordination;

static const uint8_t fdb_atomic_set_versionstamped_value[14] = {0};
static const uint8_t fdb_atomic_plus_one_32[4] = {1, 0, 0, 0};
static const uint8_t fdb_atomic_minus_one_32[4] = {255, 255, 255, 255};
#define FDB_ATOMIC_PLUS_ONE_32 fdb_atomic_plus_one_32, sizeof(fdb_atomic_plus_one_32), FDB_MUTATION_TYPE_ADD
#define FDB_ATOMIC_MINUS_ONE_32 fdb_atomic_minus_one_32, sizeof(fdb_atomic_minus_one_32), FDB_MUTATION_TYPE_ADD
#define FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE \
    fdb_atomic_set_versionstamped_value, sizeof(fdb_atomic_set_versionstamped_value), FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE

constexpr size_t fdb_max_value_size = 100'000;
constexpr size_t zk_max_value_size = 1'048'576;
constexpr char fdb_max_value_splits = zk_max_value_size / fdb_max_value_size + 1;
static_assert(zk_max_value_size / fdb_max_value_size + 1 < std::numeric_limits<char>::max());

Coroutine::Task<void> ZNodeLayer::assertExists()
{
    return assertExists(path, true, Error::ZNONODE);
}

Coroutine::Task<void> ZNodeLayer::assertExists(const String & target_path, bool should_exists, Coordination::Error error)
{
    /// Root node always exists
    if (isRootPath(target_path))
        co_return;

    auto child_key = keys.getChild(target_path);
    auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(child_key), 0);

    fdb_bool_t exists;
    const uint8_t * data;
    int len;

    throwIfFDBError(fdb_future_get_value(future.get(), &exists, &data, &len));

    if (should_exists != exists)
        throw KeeperException(error);
}

Coroutine::Task<void>
ZNodeLayer::create(const String & data, bool is_sequential, Coordination::CreateResponse & resp, bool ignore_exists)
{
    if (isRootPath(path))
    {
        if (!ignore_exists)
            throw KeeperException(Error::ZNODEEXISTS);
        else
            resp.path_created = "/";
        co_return;
    }

    const auto parent_path = getParentPath(path).toString();
    co_await assertExists(parent_path, true, Error::ZNONODE);

    /// Parent node should not be ephemeral
    {
        const auto parent_owner_key = keys.getMetaPrefix(parent_path) + static_cast<char>(KeeperKeys::ephemeralOwner);
        auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(parent_owner_key), false);

        fdb_bool_t exists;
        const uint8_t * data_bytes;
        int data_len;
        throwIfFDBError(fdb_future_get_value(future.get(), &exists, &data_bytes, &data_len));
        if (exists)
            throw KeeperException(Error::ZNOCHILDRENFOREPHEMERALS);
    }

    /// Find actual path to create
    if (is_sequential)
    {
        const auto parent_numcreate_key = keys.getMetaPrefix(parent_path) + static_cast<char>(KeeperKeys::numCreate);
        auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(parent_numcreate_key), 0);

        fdb_bool_t exists_seq;
        const uint8_t * seq_bytes;
        int seq_len;
        UInt32 seq;

        throwIfFDBError(fdb_future_get_value(future.get(), &exists_seq, &seq_bytes, &seq_len));
        if (!exists_seq)
            seq = 0;
        else
            seq = *reinterpret_cast<const UInt32 *>(seq_bytes);

        resp.path_created = fmt::format("{}{:010}", path, seq);
    }
    else
    {
        resp.path_created = path;

        auto child_key = keys.getChild(path);
        auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(child_key), 0);
        fdb_bool_t exists;
        const uint8_t * value;
        int len;

        throwIfFDBError(fdb_future_get_value(future.get(), &exists, &value, &len));

        if (exists)
        {
            if (!ignore_exists)
                throw KeeperException(Error::ZNODEEXISTS);
            else
                co_return;
        }
    }

    auto data_key = keys.getData(resp.path_created);
    auto child_key = keys.getChild(resp.path_created);
    auto meta_key_prefix = keys.getMetaPrefix(resp.path_created);
    auto parent_meta_key_prefix = keys.getMetaPrefix(parent_path);

    /// Set child key
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(child_key), nullptr, 0);

    /// Set data
    if (data.size() > zk_max_value_size)
        throw KeeperException(Error::ZBADARGUMENTS);
    auto & data_split = data_key.back();
    size_t data_offset = 0;
    for (; data_split < fdb_max_value_splits && data_offset < data.size(); ++data_split)
    {
        size_t split_size = std::min(data.size() - data_offset, fdb_max_value_size);
        fdb_transaction_set(
            &trx,
            FDB_KEY_FROM_STRING(data_key),
            reinterpret_cast<const uint8_t *>(data.data() + data_offset),
            static_cast<int>(split_size));
        data_offset += split_size;
    }

    /// Set meta
    auto meta_key = meta_key_prefix + '_';
    auto & meta_field = meta_key.back();

    auto parent_meta_key = parent_meta_key_prefix + '_';
    auto & parent_meta_field = parent_meta_key.back();

    const UInt32 zero = static_cast<UInt32>(0);
    const UInt32 data_length = static_cast<UInt32>(data.length());
    const UInt64 timestamp_now
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

#define SET_META(NAME, OP, ...) \
    meta_field = KeeperKeys::NAME; \
    fdb_transaction_##OP(&trx, FDB_KEY_FROM_STRING(meta_key), __VA_ARGS__)
    SET_META(czxid, atomic_op, FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE);
    SET_META(mzxid, atomic_op, FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE);
    SET_META(pzxid, atomic_op, FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE);
    SET_META(ctime, set, FDB_VALUE_FROM_POD(timestamp_now));
    SET_META(mtime, set, FDB_VALUE_FROM_POD(timestamp_now));
    SET_META(dataLength, set, reinterpret_cast<const uint8_t *>(&data_length), sizeof(data_length));
    SET_META(version, set, FDB_VALUE_FROM_POD(zero));
    SET_META(cversion, set, FDB_VALUE_FROM_POD(zero));
#undef SET_META

#define SET_META_PARENT(NAME, OP, ...) \
    parent_meta_field = KeeperKeys::NAME; \
    fdb_transaction_##OP(&trx, FDB_KEY_FROM_STRING(parent_meta_key), __VA_ARGS__)
    SET_META_PARENT(pzxid, atomic_op, FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE);
    SET_META_PARENT(numChildren, atomic_op, FDB_ATOMIC_PLUS_ONE_32);
    SET_META_PARENT(cversion, atomic_op, FDB_ATOMIC_PLUS_ONE_32);
    SET_META_PARENT(numCreate, atomic_op, FDB_ATOMIC_PLUS_ONE_32);
#undef SET_META_PARENT
}

void ZNodeLayer::registerEphemeralUnsafe(int64_t session, const Coordination::CreateResponse & resp)
{
    auto ephemeral_key = keys.getEphemeral(session, resp.path_created);
    auto meta_owner_key = keys.getMetaPrefix(resp.path_created) + static_cast<char>(KeeperKeys::ephemeralOwner);
    auto child_key = keys.getChild(resp.path_created);
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(ephemeral_key), nullptr, 0);
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(meta_owner_key), FDB_VALUE_FROM_POD(session));
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(child_key), FDB_VALUE_FROM_POD(KeeperKeys::ListFilterEphemeral));
}

void ZNodeLayer::setUnsafe(const String & data)
{
    auto data_key = keys.getData(path);
    if (data.size() > zk_max_value_size)
        throw KeeperException(Error::ZBADARGUMENTS);
    auto & data_split = data_key.back();
    size_t data_offset = 0;
    for (data_split = 0; data_split < fdb_max_value_splits && data_offset < data.size(); ++data_split)
    {
        size_t split_size = std::min(data.size() - data_offset, fdb_max_value_size);
        fdb_transaction_set(
            &trx,
            FDB_KEY_FROM_STRING(data_key),
            reinterpret_cast<const uint8_t *>(data.data() + data_offset),
            static_cast<int>(split_size));
        data_offset += split_size;
    }

    auto meta_key_prefix = keys.getMetaPrefix(path);
    auto meta_key = meta_key_prefix + static_cast<char>(KeeperKeys::version);
    fdb_transaction_atomic_op(&trx, FDB_KEY_FROM_STRING(meta_key), FDB_ATOMIC_PLUS_ONE_32);

    auto data_length = data.size();
    meta_key.back() = static_cast<char>(KeeperKeys::dataLength);
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(meta_key), FDB_VALUE_FROM_POD(data_length));

    meta_key.back() = static_cast<char>(KeeperKeys::mzxid);
    fdb_transaction_atomic_op(&trx, FDB_KEY_FROM_STRING(meta_key), FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE);

    const UInt64 timestamp_now
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    meta_key.back() = static_cast<char>(KeeperKeys::mtime);
    fdb_transaction_set(&trx, FDB_KEY_FROM_STRING(meta_key), FDB_VALUE_FROM_POD(timestamp_now));
}

template <typename StatResponse>
Coroutine::Task<void> ZNodeLayer::stat(StatResponse & resp, bool throw_on_non_exists, StatSkip skip)
{
    auto meta_key_prefix = keys.getMetaPrefix(path);
    auto begin = meta_key_prefix + static_cast<char>(skip);
    auto end = meta_key_prefix + '\xff';
    auto f = co_await fdb_transaction_get_range(
        &trx,
        FDB_KEYSEL_FIRST_GREATER_THAN_STRING(begin),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(end),
        znode_stats_total_size,
        0,
        FDB_STREAMING_MODE_EXACT,
        0,
        0,
        0);

    auto & stat = resp.stat;
    memset(&stat, 0, sizeof(stat)); /// Clear stat

    const FDBKeyValue * kvs;
    int kvs_len;
    fdb_bool_t more;

    throwIfFDBError(fdb_future_get_keyvalue_array(f.get(), &kvs, &kvs_len, &more));

    if (more)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Found more meta keys than expected (expect < {})", znode_stats_total_size);

    /// Non-root znode must have meta keys.
    /// Root znode is always exists. Root's stat is zero by default.
    if (kvs_len == 0 && !isRootPath(path))
    {
        if (throw_on_non_exists)
        {
            throw KeeperException(Error::ZNONODE);
        }
        else
        {
            resp.error = Error::ZNONODE;
            co_return;
        }
    }

    const int expect_key_lens = static_cast<int>(meta_key_prefix.size() + 1);
    for (int i = 0; i < kvs_len; i++)
    {
        const auto & kv = kvs[i];

        if (kv.key_length != expect_key_lens)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid meta key {}", fdb_print_key(kv.key, kv.key_length));

        switch (reinterpret_cast<const char *>(kv.key)[kv.key_length - 1])
        {
#define M(K, V) \
    case V: \
        stat.K = *reinterpret_cast<const decltype(stat.K) *>(kv.value); \
        break;
            APPLY_FOR_ZNODE_STATS_WITHOUT_HIDDEN(M)
#undef M
#define M(K, V) \
    case V: \
        break;
            APPLY_FOR_ZNODE_STATS_HIDDEN(M)
#undef M
            default:
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Invalid meta key {} due to unknown meta field", fdb_print_key(kv.key, kv.key_length));
        }
    }
}

template Coroutine::Task<void> ZNodeLayer::stat(SetResponse & resp, bool throw_on_non_exists, StatSkip skip);
template Coroutine::Task<void> ZNodeLayer::stat(ExistsResponse & resp, bool throw_on_non_exists, StatSkip skip);

Coroutine::Task<void> ZNodeLayer::get(Coordination::GetResponse & resp, bool throw_on_non_exists)
{
    co_await stat(resp, throw_on_non_exists);
    if (!throw_on_non_exists)
    {
        if (resp.error != Error::ZOK)
            co_return;
    }

    auto data_key = keys.getData(path);
    auto & data_split_idx = data_key.back();
    data_split_idx = 0;
    resp.data.clear();

    while (data_split_idx < fdb_max_value_splits && static_cast<int>(data_split_idx * fdb_max_value_size) < resp.stat.dataLength)
    {
        auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(data_key), 0);
        data_split_idx++;

        fdb_bool_t exists = false;
        const uint8_t * data_split = nullptr;
        int len = 0;
        throwIfFDBError(fdb_future_get_value(future.get(), &exists, &data_split, &len));

        if (!exists)
            co_return;
        resp.data.append(reinterpret_cast<const char *>(data_split), len);
    }
}

Coroutine::Task<void> ZNodeLayer::check(int32_t version)
{
    co_await assertExists(path, true, Error::ZNONODE);
    if (version == -1)
        co_return;

    auto version_key = keys.getMetaPrefix(path) + static_cast<char>(KeeperKeys::version);
    auto f = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(version_key), 0);

    fdb_bool_t exists;
    const uint8_t * version_bytes;
    int len;

    throwIfFDBError(fdb_future_get_value(f.get(), &exists, &version_bytes, &len));
    if (exists)
    {
        if (*(reinterpret_cast<const int32_t *>(version_bytes)) != version)
            throw KeeperException(Error::ZBADVERSION);
    }
    else if (version != 0)
    {
        throw KeeperException(Error::ZBADVERSION);
    }
}

Coroutine::Task<void> ZNodeLayer::checkNotExists(int32_t version)
{
    if (isRootPath(path))
        throw KeeperException(Error::ZNODEEXISTS);

    auto version_key = keys.getMetaPrefix(path) + static_cast<char>(KeeperKeys::version);
    auto f = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(version_key), 0);

    fdb_bool_t exists;
    const uint8_t * version_bytes;
    int len;

    throwIfFDBError(fdb_future_get_value(f.get(), &exists, &version_bytes, &len));
    if (exists && (version == -1 || *(reinterpret_cast<const int32_t *>(version_bytes)) == version))
        throw KeeperException(Error::ZNODEEXISTS);
}

Coroutine::Task<void> ZNodeLayer::remove(int32_t version)
{
    if (version != -1)
        co_await check(version); /// check() implied assertExists()
    else
        co_await assertExists(path, true, Error::ZNONODE);

    auto meta_prefix = keys.getMetaPrefix(path);
    auto children_key = meta_prefix + static_cast<char>(KeeperKeys::numChildren);
    auto owner_key = meta_prefix + static_cast<char>(KeeperKeys::ephemeralOwner);

    /// Remove ephemeral key if exists
    {
        auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(owner_key), false);
        fdb_bool_t exists;
        const uint8_t * session_bytes;
        int len;
        throwIfFDBError(fdb_future_get_value(future.get(), &exists, &session_bytes, &len));

        if (exists)
        {
            assert(len == sizeof(SessionID));
            auto ephemeral_key = keys.getEphemeral(*reinterpret_cast<const SessionID *>(session_bytes), path);
            fdb_transaction_clear(&trx, FDB_KEY_FROM_STRING(ephemeral_key));
        }
    }

    auto future = co_await fdb_transaction_get(&trx, FDB_KEY_FROM_STRING(children_key), 0);

    fdb_bool_t children_key_exists;
    const uint8_t * children_bytes;
    int children_bytes_len;
    throwIfFDBError(fdb_future_get_value(future.get(), &children_key_exists, &children_bytes, &children_bytes_len));

    if (children_key_exists && *(reinterpret_cast<const int32_t *>(children_bytes)) > 0)
        throw KeeperException(Error::ZNOTEMPTY);

    removeUnsafeTrx(&trx, keys, path);
}

void ZNodeLayer::removeUnsafeTrx(FDBTransaction * tr, const KeeperKeys & keys, const String & path)
{
    auto child_key = keys.getChild(path);
    auto data_key = keys.getData(path);
    auto meta_key_begin = keys.getMetaPrefix(path);
    auto meta_key_end = meta_key_begin + '\xff';

    fdb_transaction_clear(tr, FDB_KEY_FROM_STRING(child_key));
    fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(meta_key_begin), FDB_KEY_FROM_STRING(meta_key_end));

    char & split_idx = data_key.back();
    for (; split_idx < fdb_max_value_splits; split_idx++)
        fdb_transaction_clear(tr, FDB_KEY_FROM_STRING(data_key));

    auto parent_meta_prefix = keys.getMetaPrefix(getParentPath(path).toString());
    auto parent_meta_key = parent_meta_prefix + '_';

#define SET_META_PARENT(NAME, OP, ...) \
    parent_meta_key.back() = KeeperKeys::NAME; \
    fdb_transaction_##OP(tr, FDB_KEY_FROM_STRING(parent_meta_key), __VA_ARGS__)

    SET_META_PARENT(cversion, atomic_op, FDB_ATOMIC_PLUS_ONE_32);
    SET_META_PARENT(numChildren, atomic_op, FDB_ATOMIC_MINUS_ONE_32);
    SET_META_PARENT(pzxid, atomic_op, FDB_ATOMIC_SET_VERSIONSTAMPED_VALUE);
#undef SET_META_PARENT
}

Coroutine::Task<void> ZNodeLayer::list(Coordination::ListResponse & resp, Coordination::ListRequestType list_request_type)
{
    auto children_key_prefix = keys.getChildrenPrefix(path);
    co_await stat(resp);

    int iterator = 0;
    auto & list = resp.names;

    auto children_key_begin = children_key_prefix;
    fdb_bool_t more = true;
    while (more)
    {
        auto children_key_end = children_key_prefix + '\xff';
        auto f = co_await fdb_transaction_get_range(
            &trx,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(children_key_begin),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(children_key_end),
            0,
            0,
            FDB_STREAMING_MODE_ITERATOR,
            ++iterator,
            0,
            0);

        const FDBKeyValue * kvs;
        int kvs_len;
        throwIfFDBError(fdb_future_get_keyvalue_array(f.get(), &kvs, &kvs_len, &more));

        if (kvs_len == 0)
            break;

        for (int i = 0; i < kvs_len; i++)
        {
            const auto & kv = kvs[i];

            bool is_ephemeral = kv.value_length > 0 && (*kv.value & KeeperKeys::ListFilterEphemeral) == KeeperKeys::ListFilterEphemeral;

            if (list_request_type == Coordination::ListRequestType::ALL
                || (is_ephemeral && list_request_type == Coordination::ListRequestType::EPHEMERAL_ONLY)
                || (!is_ephemeral && list_request_type == Coordination::ListRequestType::PERSISTENT_ONLY))
                list.emplace_back(
                    reinterpret_cast<const char *>(kv.key) + children_key_prefix.size(), kv.key_length - children_key_prefix.size());
        }

        children_key_begin.assign(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length);
    }
}

Coroutine::Task<void> ZNodeLayer::watch(Coordination::WatchCallbackPtr cb, const String & request_path)
{
    auto version_key = keys.getMetaPrefix(path) + static_cast<char>(KeeperKeys::version);

    auto * watch_future = fdb_transaction_watch(&trx, FDB_KEY_FROM_STRING(version_key));

    auto * payload = new WatchPayload{request_path, cb, nullptr};
    payload->tracker_token = co_await Coroutine::ForkCancelTokenTag{};
    if (payload->tracker_token)
        payload->tracker_token->setCancelPoint(watch_future);

    throwIfFDBError(fdb_future_set_callback(watch_future, ZNodeLayer::onWatch<Event::CHANGED>, payload));
}

Coroutine::Task<void> ZNodeLayer::watchChildren(Coordination::WatchCallbackPtr cb, const String & request_path)
{
    auto cversion_key = keys.getMetaPrefix(path) + static_cast<char>(KeeperKeys::cversion);

    auto * watch_future = fdb_transaction_watch(&trx, FDB_KEY_FROM_STRING(cversion_key));

    auto * payload = new WatchPayload{request_path, cb, nullptr};
    payload->tracker_token = co_await Coroutine::ForkCancelTokenTag{};
    if (payload->tracker_token)
        payload->tracker_token->setCancelPoint(watch_future);

    throwIfFDBError(fdb_future_set_callback(watch_future, ZNodeLayer::onWatch<Event::CHILD>, payload));
}

Coroutine::Task<void> ZNodeLayer::watchExists(
    const Coordination::ExistsResponse & exists_resp, Coordination::WatchCallbackPtr cb, const String & request_path)
{
    auto child_key = keys.getChild(path);

    auto * watch_future = fdb_transaction_watch(&trx, FDB_KEY_FROM_STRING(child_key));

    auto * payload = new WatchPayload{request_path, cb, nullptr};
    payload->tracker_token = co_await Coroutine::ForkCancelTokenTag{};
    if (payload->tracker_token)
        payload->tracker_token->setCancelPoint(watch_future);

    throwIfFDBError(fdb_future_set_callback(
        watch_future,
        exists_resp.error == Error::ZOK ? ZNodeLayer::onWatch<Event::DELETED> : ZNodeLayer::onWatch<Event::CREATED>,
        payload));
}

template <Event resp_event>
void ZNodeLayer::onWatch(FDBFuture * f, void * payload) noexcept
{
    WatchResponse resp;
    auto * watch_payload = reinterpret_cast<WatchPayload *>(payload);

    auto fdb_error = fdb_future_get_error(f);
    if (fdb_error == FDBErrorCode::operation_cancelled)
        resp.error = Coordination::Error::ZSESSIONEXPIRED;
    else
        resp.error = getKeeperErrorFromFDBError(fdb_error);

    resp.type = resp_event;
    resp.state = State::CONNECTED;
    resp.path = watch_payload->path;

    try
    {
        auto & cb = watch_payload->callback;
        (*cb)(resp);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "WatchCallback failed");
    }

    delete watch_payload;
    fdb_future_destroy(f);

    ProfileEvents::increment(ProfileEvents::ZooKeeperWatchResponse);
}
}
