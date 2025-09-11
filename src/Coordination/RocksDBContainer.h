#pragma once
#include <base/StringRef.h>
#include <Coordination/KeeperContext.h>
#include <Disks/DiskLocal.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <rocksdb/convenience.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/snapshot.h>

namespace DB
{

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
    /// MockNode is only used in test
    struct MockNode
    {
        uint64_t acl_id = 0;
        std::vector<int> children;
        std::string data;
        MockNode(size_t children_num, std::string_view data_, uint64_t acl_id_)
            : acl_id(acl_id_)
            , children(std::vector<int>(children_num))
            , data(data_)
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
                new_pair->key = StringRef(iter->key().ToStringView());
                ReadBufferFromOwnString buffer(iter->value().ToStringView());
                typename Node::Meta & meta = new_pair->value;
                readPODBinary(meta, buffer);
                readVarUInt(new_pair->value.stats.data_size, buffer);
                if (new_pair->value.stats.data_size)
                {
                    new_pair->value.data = std::unique_ptr<char[]>(new char[new_pair->value.stats.data_size]);
                    buffer.readStrict(new_pair->value.data.get(), new_pair->value.stats.data_size);
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

    std::vector<std::pair<std::string, Node>> getChildren(const std::string & key_prefix, bool read_meta = true, bool read_data = false)
    {
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;

        std::string key = key_prefix;
        if (!key.ends_with('/'))
            key += '/';
        size_t len = key.size();

        auto iter = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(read_options));
        rocksdb::Slice prefix(key);
        std::vector<std::pair<std::string, Node>> result;
        auto is_direct_child = [](rocksdb::Slice iter_key, rocksdb::Slice seek_key)
        {
            if (iter_key.size() <= 1)
                return false;
            size_t rslash_pos = 0;
            for (size_t i = iter_key.size() - 1; i > 0; --i)
            {
                if (iter_key[i] == '/')
                {
                    rslash_pos = i;
                    break;
                }
            }
            if (rslash_pos == 0 && seek_key.size() == 1)
                return true;
            return seek_key.compare(rocksdb::Slice(iter_key.data(), rslash_pos)) == 0;
        };
        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next())
        {
            if (!is_direct_child(iter->key(), rocksdb::Slice(key_prefix)))
                continue;
            Node node;
            if (read_meta)
            {
                ReadBufferFromOwnString buffer(iter->value().ToStringView());
                typename Node::Meta & meta = node;
                /// We do not read data here
                readPODBinary(meta, buffer);
                if (read_data)
                {
                    readVarUInt(meta.stats.data_size, buffer);
                    if (meta.stats.data_size)
                    {
                        node.data = std::unique_ptr<char[]>(new char[meta.stats.data_size]);
                        buffer.readStrict(node.data.get(), meta.stats.data_size);
                    }
                }
            }
            std::string real_key(iter->key().data() + len, iter->key().size() - len);
            result.emplace_back(std::move(real_key), std::move(node));
        }

        return result;
    }

    bool contains(const std::string & path)
    {
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), path, &buffer_str);
        if (status.IsNotFound())
            return false;
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during executing contains. The error message is {}.", status.ToString());
        return true;
    }

    const_iterator find(StringRef key)
    {
        /// rocksdb::PinnableSlice slice;
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key.toView(), &buffer_str);
        if (status.IsNotFound())
            return end();
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during executing find. The error message is {}.", status.ToString());
        ReadBufferFromOwnString buffer(buffer_str);
        auto kv = std::make_shared<KVPair>();
        kv->key = key;
        typename Node::Meta & meta = kv->value;
        readPODBinary(meta, buffer);
        /// TODO: Sometimes we don't need to load data.
        readVarUInt(kv->value.stats.data_size, buffer);
        if (kv->value.stats.data_size)
        {
            kv->value.data = std::unique_ptr<char[]>(new char[kv->value.stats.data_size]);
            buffer.readStrict(kv->value.data.get(), kv->value.stats.data_size);
        }
        return const_iterator(kv);
    }

    MockNode getValue(StringRef key)
    {
        auto it = find(key);
        chassert(it != end());
        return MockNode(it->value.stats.numChildren(), it->value.getData(), it->value.acl_id);
    }

    const_iterator updateValue(StringRef key, ValueUpdater updater)
    {
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key.toView(), &buffer_str);
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during find. The error message is {}.", status.ToString());
        auto kv = std::make_shared<KVPair>();
        kv->key = key;
        kv->value.decodeFromString(buffer_str);
        updater(kv->value);
        insertOrReplace<false>(key.toString(), kv->value);
        return const_iterator(kv);
    }

    bool insert(const std::string & key, Node & value)
    {
        std::string value_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value_str);
        if (status.ok())
        {
            return false;
        }
        if (status.IsNotFound())
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

    template<bool need_get = true>
    void insertOrReplace(const std::string & key, Node & value)
    {
        bool increase_counter = false;
        rocksdb::Status status;
        if constexpr (need_get)
        {
            std::string value_str;
            status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value_str);
            if (status.IsNotFound())
                increase_counter = true;
            else if (!status.ok())
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during get. The error message is {}.", status.ToString());
        }

        status = rocksdb_ptr->Put(write_options, key, value.getEncodedString());
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
        insertOrReplace(key, value);
    }

    bool erase(const std::string & key)
    {
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

}
