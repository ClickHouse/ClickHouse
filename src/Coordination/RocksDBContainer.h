#pragma once
#include <base/StringRef.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include "Common/Exception.h"
#include <Common/SipHash.h>
#include <Disks/DiskLocal.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include "config.h"
#if USE_ROCKSDB
#include <rocksdb/convenience.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/snapshot.h>
#endif

namespace DB
{

template<typename S>
inline UInt16 getKeyDepth(const S & key)
{
    UInt16 depth = 0;
    for (size_t i = 0; i < key.size(); i++)
    {
        if (key[i] == '/' && i + 1 != key.size())
            depth ++;
    }
    return depth;
}

template<bool throw_exception = true, typename S>
bool checkKeyEncoded(const S &key)
{
    if (key.length() <= 2)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Key {} is not encoded", key);
        else
            return false;
    }
    ReadBufferFromOwnString key_buffer(key);
    UInt16 depth;
    readIntBinary(depth, key_buffer);
    UInt16 real_depth = getKeyDepth(std::string_view(key.data() + 2, key.size() - 2));
    if (real_depth != depth)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Key {} is not encoded", key);
        else
            return false;
    }
    return true;
}

template<bool use_rocksdb = true, bool child_prefix = false, typename S>
inline std::string getEncodedKey(const S & key)
{
    if constexpr (!use_rocksdb)
        return key;
    WriteBufferFromOwnString key_buffer;
    UInt16 depth = getKeyDepth(key) + (child_prefix ? 1 : 0);
    writeIntBinary(depth, key_buffer);
    writeString(key, key_buffer);
    std::string & value = key_buffer.str();
    if constexpr (child_prefix)
    {
        if (!value.ends_with('/'))
            value += '/';
    }
    return value;
}

template<bool use_rocksdb = true, typename S>
static S getDecodedKey(const S & key)
{
    if constexpr (!use_rocksdb)
        return key;
    if (!checkKeyEncoded<false>(key))
        return key;
    return {key.begin() + 2, key.end()};
}

#if USE_ROCKSDB
namespace ErrorCodes
{
extern const int ROCKSDB_ERROR;
extern const int LOGICAL_ERROR;
}

/// The key-value format of rocks db will be
/// - key: Int8 (depth of the path) + String (path)
/// - value: SizeOf(keeperRocksNodeInfo) (meta of the node) + String (data)

template <class Node_>
struct RocksDBContainer
{
    using Node = Node_;

private:
/// MockNode is only use in test to mock `getChildren()` and `getData()`
    struct MockNode
    {
        std::vector<int> children;
        std::string data;
        MockNode(size_t children_num, std::string_view data_)
            : children(std::vector<int>(children_num)),
                data(data_)
        {
        }

        std::vector<int> getChildren() { return children; }
        std::string getData() { return data; }
    };

    struct KVPair
    {
        StringRef key;
        Node value;
    };

    using ValueUpdater = std::function<void(Node & node)>;

public:

    /// This is an iterator wrapping rocksdb iterator and the kv result.
    struct const_iterator
    {
        std::shared_ptr<rocksdb::Iterator> iter;

        std::shared_ptr<const KVPair> pair;

        const_iterator() = default;

        explicit const_iterator(std::shared_ptr<KVPair> pair_) : pair(std::move(pair_)) {}

        explicit const_iterator(rocksdb::Iterator * iter_) : iter(iter_)
        {
            updatePairFromIter();
        }

        const KVPair & operator * () const
        {
            return *pair;
        }

        const KVPair * operator->() const
        {
            return pair.get();
        }

        bool operator != (const const_iterator & other) const
        {
            return !(*this == other);
        }

        bool operator == (const const_iterator & other) const
        {
            if (pair == nullptr && other == nullptr)
                return true;
            if (pair == nullptr || other == nullptr)
                return false;
            return pair->key.toView() == other->key.toView() && iter == other.iter;
        }

        bool operator == (std::nullptr_t) const
        {
            return iter == nullptr;
        }

        bool operator != (std::nullptr_t) const
        {
            return iter != nullptr;
        }

        explicit operator bool() const
        {
            return iter != nullptr;
        }

        const_iterator & operator ++()
        {
            iter->Next();
            updatePairFromIter();
            return *this;
        }

    private:
        void updatePairFromIter()
        {
            if (iter && iter->Valid())
            {
                auto new_pair = std::make_shared<KVPair>();
                #ifdef NDEBUG
                    checkKeyEncoded(iter->key().ToStringView());
                #endif
                new_pair->key = StringRef(getDecodedKey(iter->key().ToStringView()));
                ReadBufferFromOwnString buffer(iter->value().ToStringView());
                typename Node::Meta & meta = new_pair->value;
                readPODBinary(meta, buffer);
                readVarUInt(new_pair->value.data_size, buffer);
                if (new_pair->value.data_size)
                {
                    new_pair->value.data = std::unique_ptr<char[]>(new char[new_pair->value.data_size]);
                    buffer.readStrict(new_pair->value.data.get(), new_pair->value.data_size);
                }
                pair = new_pair;
            }
            else
            {
                pair = nullptr;
                iter = nullptr;
            }
        }
    };

    bool initialized = false;

    const const_iterator end_ptr;

    void initialize(const KeeperContextPtr & context)
    {
        DiskPtr disk = context->getTemporaryRocksDBDisk();
        if (disk == nullptr)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get rocksdb disk");
        }
        auto options = context->getRocksDBOptions();
        if (options == nullptr)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get rocksdb options");
        }
        rocksdb_dir = disk->getPath();
        rocksdb::DB * db;
        auto status = rocksdb::DB::Open(*options, rocksdb_dir, &db);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}",
                rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
        write_options.disableWAL = true;
        initialized = true;
    }

    ~RocksDBContainer()
    {
        if (initialized)
        {
            rocksdb_ptr->Close();
            rocksdb_ptr = nullptr;

            std::filesystem::remove_all(rocksdb_dir);
        }
    }

    std::vector<std::pair<std::string, Node>> getChildren(const std::string & key)
    {
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;

        size_t len = key.size();
        auto iter = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(read_options));
        rocksdb::Slice prefix(key);
        std::vector<std::pair<std::string, Node>> result;
        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next())
        {
            Node node;
            ReadBufferFromOwnString buffer(iter->value().ToStringView());
            typename Node::Meta & meta = node;
            /// We do not read data here
            readPODBinary(meta, buffer);
            std::string real_key(iter->key().data() + len, iter->key().size() - len);
            result.emplace_back(std::move(real_key), std::move(node));
        }

        return result;
    }

    bool contains(const std::string & key)
    {
    #ifdef NDEBUG
        checkKeyEncoded(key);
    #endif
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &buffer_str);
        if (status.IsNotFound())
            return false;
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during executing contains. The error message is {}.", status.ToString());
        return true;
    }

    const_iterator find(StringRef key_)
    {
    #ifdef NDEBUG
        checkKeyEncoded(key_.toView());
    #endif
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key_.toString(), &buffer_str);
        if (status.IsNotFound())
            return end();
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during executing find. The error message is {}.", status.ToString());
        ReadBufferFromOwnString buffer(buffer_str);
        auto kv = std::make_shared<KVPair>();
        kv->key = key_;
        typename Node::Meta & meta = kv->value;
        readPODBinary(meta, buffer);
        /// TODO: Sometimes we don't need to load data.
        readVarUInt(kv->value.data_size, buffer);
        if (kv->value.data_size)
        {
            kv->value.data = std::unique_ptr<char[]>(new char[kv->value.data_size]);
            buffer.readStrict(kv->value.data.get(), kv->value.data_size);
        }
        return const_iterator(kv);
    }

    MockNode getValue(StringRef key)
    {
        String encoded_key = getEncodedKey(key.toString());
        auto it = find(StringRef(encoded_key));
        chassert(it != end());
        return MockNode(it->value.numChildren(), it->value.getData());
    }

    const_iterator updateValue(StringRef key_, ValueUpdater updater)
    {
    #ifdef NDEBUG
        checkKeyEncoded(key_.toView());
    #endif
        const std::string & key = key_.toString();
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &buffer_str);
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during find. The error message is {}.", status.ToString());
        auto kv = std::make_shared<KVPair>();
        kv->key = key_;
        kv->value.decodeFromString(buffer_str);
        updater(kv->value);
        insertOrReplace<false>(key, kv->value);
        return const_iterator(kv);
    }

    bool insert(const std::string & key, Node & value)
    {
    #ifdef NDEBUG
        checkKeyEncoded(key);
    #endif
        std::string value_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value_str);
        if (status.ok())
        {
            return false;
        }
        else if (status.IsNotFound())
        {
            status = rocksdb_ptr->Put(write_options, key, value.getEncodedString());
            if (status.ok())
            {
                counter++;
                return true;
            }
        }

        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during insert. The error message is {}.", status.ToString());
    }

    template<bool need_to_get = true>
    void insertOrReplace(const std::string & key, Node & value)
    {
    #ifdef NDEBUG
        checkKeyEncoded(key);
    #endif
        bool increase_counter = false;
        if constexpr (need_to_get)
        {
            std::string value_str;
            rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value_str);
            if (status.IsNotFound())
                increase_counter = true;
            else if (!status.ok())
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during get. The error message is {}.", status.ToString());
        }

        rocksdb::Status status = rocksdb_ptr->Put(write_options, key, value.getEncodedString());
        if (status.ok())
            counter += increase_counter;
        else
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during insert. The error message is {}.", status.ToString());
    }

    using KeyPtr = std::unique_ptr<char[]>;

    /// To be compatible with SnapshotableHashTable, will remove later;
    KeyPtr allocateKey(size_t size)
    {
        return KeyPtr{new char[size]};
    }

    void insertOrReplace(KeyPtr key_data, size_t key_size, Node value)
    {
        std::string key(key_data.get(), key_size);
        std::string encoded_key = getEncodedKey<true>(key);
        insertOrReplace<true>(encoded_key, value);
    }

    bool erase(const std::string & key)
    {
    #ifdef NDEBUG
        checkKeyEncoded(key);
    #endif
        auto status = rocksdb_ptr->Delete(write_options, key);
        if (status.IsNotFound())
            return false;
        if (status.ok())
        {
            counter--;
            return true;
        }
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during erase. The error message is {}.", status.ToString());
    }

    void recalculateDataSize() {}
    void reverse(size_t size_) {(void)size_;}

    uint64_t getApproximateDataSize() const
    {
        /// use statistics from rocksdb
        return counter * sizeof(Node);
    }

    void enableSnapshotMode(size_t version)
    {
        chassert(!snapshot_mode);
        snapshot_mode = true;
        snapshot_up_to_version = version;
        snapshot_size = counter;
        ++current_version;

        snapshot = rocksdb_ptr->GetSnapshot();
    }

    void disableSnapshotMode()
    {
        chassert(snapshot_mode);
        snapshot_mode = false;
        rocksdb_ptr->ReleaseSnapshot(snapshot);
    }

    void clearOutdatedNodes() {}

    std::pair<size_t, size_t> snapshotSizeWithVersion() const
    {
        if (!snapshot_mode)
            return std::make_pair(counter, current_version);
        else
            return std::make_pair(snapshot_size, current_version);
    }

    const_iterator begin() const
    {
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;
        if (snapshot_mode)
            read_options.snapshot = snapshot;
        auto * iter = rocksdb_ptr->NewIterator(read_options);
        iter->SeekToFirst();
        return const_iterator(iter);
    }

    const_iterator end() const
    {
        return end_ptr;
    }

    size_t size() const
    {
        return counter;
    }

    uint64_t getArenaDataSize() const
    {
        return 0;
    }

    uint64_t keyArenaSize() const
    {
        return 0;
    }

private:
    String rocksdb_dir;

    std::unique_ptr<rocksdb::DB> rocksdb_ptr;
    rocksdb::WriteOptions write_options;

    const rocksdb::Snapshot * snapshot;

    bool snapshot_mode{false};
    size_t current_version{0};
    size_t snapshot_up_to_version{0};
    size_t snapshot_size{0};
    size_t counter{0};

};
#endif

}
