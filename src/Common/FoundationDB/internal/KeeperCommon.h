#pragma once

#include <exception>
#include <base/defines.h>
#include <base/types.h>
#include <boost/endian/conversion.hpp>
#include <Poco/DateTimeFormatter.h>
#include <Poco/Logger.h>
#include <Poco/Timestamp.h>

#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/ZooKeeper/IKeeper.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

/// ZNode stat <=> Meta key in fdb
///
/// mzxid, pzxid, czxid must be the first 3 by order, because they may be
/// skipped in the range lookup.

/// Meta keys in ZNode stat
#define APPLY_FOR_ZNODE_STATS_WITHOUT_HIDDEN(M) \
    M(mzxid, 1) \
    M(pzxid, 2) \
    M(czxid, 3) \
    M(ctime, 4) \
    M(mtime, 5) \
    M(version, 6) \
    M(cversion, 7) \
    M(dataLength, 8) \
    M(numChildren, 9) \
    M(ephemeralOwner, 10)

/// Additional meta keys
#define APPLY_FOR_ZNODE_STATS_HIDDEN(M) M(numCreate, 11)

/// All meta keys
#define APPLY_FOR_ZNODE_STATS(M) \
    APPLY_FOR_ZNODE_STATS_WITHOUT_HIDDEN(M) \
    APPLY_FOR_ZNODE_STATS_HIDDEN(M)

constexpr int znode_stats_total_size = []()
{
    int i = 0;
#define M(k, v) ++i;
    APPLY_FOR_ZNODE_STATS(M)
#undef M
    return i;
}();

namespace DB::FoundationDB
{
using SessionID = int64_t;

/// Convert POD to char *. Assuming little endian architecture.
template <typename T>
inline std::string_view toBinary(const T & value)
{
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::endian::native == std::endian::little);
    return std::string_view(reinterpret_cast<const char *>(&value), sizeof(value));
}

/// Timestamp in milliseconds
struct BigEndianTimestamp
{
    Int64 t;

    String format() const
    {
        Poco::Timestamp pt(boost::endian::big_to_native(t) * 1000);
        return Poco::DateTimeFormatter::format(pt, "%H:%M:%s UTC");
    }

    static BigEndianTimestamp fromNow(Int64 offset_millisecond)
    {
        BigEndianTimestamp ts;

        ts.t = Poco::Timestamp().epochMicroseconds() / 1000 + offset_millisecond;
        boost::endian::native_to_big_inplace(ts.t);

        return ts;
    }
};

/// Keys of FDBKeeper
///
/// Children key:
///   {$path} + 'child' + {$child_name} = list_filter_flags (1byte)
/// Stat key:
///   {$path} + 'stat' + {$stat_name} = {$stat_value}
/// Data key:
///   {$path} + 'data' = {$data}
class KeeperKeys
{
public:
    enum SubSpace : char
    {
        children = 0,
        meta = 1,
        data = 2,
        expire = 3,
        ephemeral = 4,
        cleaner = 5,
    };

    enum Stat : char
    {
#define M(K, V) K = (V),
        APPLY_FOR_ZNODE_STATS(M)
#undef M
    };

    const static UInt8 ListFilterEphemeral;

    explicit KeeperKeys(const String & prefix_) : prefix(prefix_) { }

    String getChildrenPrefix(const String & path) const noexcept
    {
        auto key = prefix;
        if (unlikely(!path.starts_with('/')))
            key.push_back('/');
        key.append(path);
        key.push_back(SubSpace::children);
        return key;
    }

    String getChild(const String & path) const noexcept
    {
        auto key = prefix;
        auto dir_pos = path.rfind('/');
        if (unlikely(dir_pos == 0))
        {
            key.push_back('/');
            key.push_back(SubSpace::children);
            key.insert(key.end(), path.begin() + 1, path.end());
        }
        else if (unlikely(dir_pos == String::npos))
        {
            key.push_back('/');
            key.push_back(SubSpace::children);
            key.append(path);
        }
        else
        {
            dir_pos += key.size();

            key.append(path);
            key[dir_pos] = SubSpace::children;
        }

        return key;
    }

    String getMetaPrefix(const String & path) const noexcept
    {
        auto key = prefix;
        if (unlikely(!path.starts_with('/')))
            key.push_back('/');
        key.append(path);
        key.push_back(SubSpace::meta);
        return key;
    }

    String getData(const String & path) const noexcept
    {
        auto key = prefix;
        if (unlikely(!path.starts_with('/')))
            key.push_back('/');
        key.append(path);
        key.push_back(SubSpace::data);
        key.push_back(0);
        return key;
    }

    String getSessionPrefix() const
    {
        String key;
        key.reserve(prefix.size() + sizeof(SubSpace::expire));
        key.append(prefix);
        key.push_back(SubSpace::expire);
        return key;
    }

    String getSessionKey(BigEndianTimestamp expire_timestamp, FDBVersionstamp stamp, bool include_vs_index) const
    {
        String key;
        key.reserve(prefix.size() + sizeof(SubSpace::expire) + sizeof(expire_timestamp) + (include_vs_index ? 4 : 0));
        key.append(prefix);
        key.push_back(SubSpace::expire);
        key.append(toBinary(expire_timestamp));
        UInt32 vs_index = static_cast<int32_t>(key.size());
        key.append(toBinary(stamp));
        if (include_vs_index)
            key.append(toBinary(vs_index));
        return key;
    }

    static FDBVersionstamp & getVSFromSessionKey(String & key, bool include_vs_index)
    {
        auto * vs_bytes = key.data() + key.size() - sizeof(FDBVersionstamp);

        if (include_vs_index)
            vs_bytes -= 4;

        return *reinterpret_cast<FDBVersionstamp *>(vs_bytes);
    }

    static BigEndianTimestamp & getTSFromSessionKey(String & key, bool include_vs_index)
    {
        auto * ts_bytes = key.data() + key.size() - sizeof(FDBVersionstamp) - sizeof(BigEndianTimestamp);

        if (include_vs_index)
            ts_bytes -= 4;

        return *reinterpret_cast<BigEndianTimestamp *>(ts_bytes);
    }

    /// Extract session id from session key.
    ///
    /// NOTE: You are supposed to check again if the session id is already occupied.
    /// This method take the tailing 8 bytes from versionstamp as the session id.
    /// A versionstamp is a 10 byte, unique, monotonically (but not sequentially)
    /// increasing value for each committed transaction. The first 8 bytes are
    /// the committed version of the database (serialized in big-endian order).
    /// The last 2 bytes are monotonic in the serialization order for transactions
    /// (serialized in big-endian order).
    /// As a result, session id may be duplicated, even they are is obtained from
    /// different transactions.
    static SessionID extractSessionFromSessionKey(const String & key, bool include_vs_index)
    {
        const auto * vs_bytes = key.data() + key.size() - sizeof(FDBVersionstamp) + 2;

        if (include_vs_index)
            vs_bytes -= 4;

        return *reinterpret_cast<const SessionID *>(vs_bytes);
    }

    String getEphemeral(SessionID session, const String & path) const
    {
        auto key = prefix;
        key.push_back(SubSpace::ephemeral);
        key.append(toBinary(session));
        if (!path.starts_with('/'))
            key.push_back('/');
        key.append(path);
        return key;
    }

    size_t getEphemeralPrefixSize() const { return prefix.size() + sizeof(SubSpace::ephemeral) + sizeof(SessionID); }

    String getCleaner() const { return prefix + static_cast<char>(KeeperKeys::cleaner); }

    const String prefix;
};

/// Exception of ZooKeeper error
class KeeperException : public Exception
{
public:
    explicit KeeperException(Coordination::Error error_);
    const Coordination::Error error;
};

/// Convert FDB error code to Keeper error code. The error will output to the
/// log if there is no match keeper error code.
inline Coordination::Error getKeeperErrorFromFDBError(fdb_error_t fdb_error, const String & message = "") noexcept
{
    switch (fdb_error)
    {
        case 0:
            return Coordination::Error::ZOK;
        case 1004:
        case 1031:
            return Coordination::Error::ZOPERATIONTIMEOUT;
        default: {
            auto * log = &Poco::Logger::get(__PRETTY_FUNCTION__);
            LOG_WARNING(log, "Unexpected fdb error {} ({}).{}", fdb_error, fdb_get_error(fdb_error), message);
            return Coordination::Error::ZSYSTEMERROR;
        }
    }
}

/// Convert any exception to Keeper error code. The exception will output to the
/// log if there is no match keeper error code.
inline Coordination::Error getKeeperErrorFromException(std::exception_ptr eptr, const String & message = "") noexcept
{
    try
    {
        std::rethrow_exception(eptr);
    }
    catch (KeeperException & e)
    {
        return e.error;
    }
    catch (FoundationDBException & e)
    {
        return getKeeperErrorFromFDBError(
            e.code, " Stack trace (when copying this message, always include the lines below):\n\n" + e.getStackTraceString());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Unexpected exception" + message);
        return Coordination::Error::ZSYSTEMERROR;
    }
}

inline StringRef getParentPath(const String & path)
{
    assert(!path.empty() && path != "/");
    auto pos = path.rfind('/');
    if (pos == String::npos)
        pos = 0;
    return StringRef(path.data(), pos);
}


inline void removeRootPath(String & path, const String & root_path)
{
    if (path.size() < root_path.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path is not longer than root_path");
    else if (path.size() == root_path.size())
        path = '/';
    else
        path.erase(0, root_path.size());
}
}
