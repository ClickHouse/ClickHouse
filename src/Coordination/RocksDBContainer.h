#pragma once
#include <base/StringRef.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/SipHash.h>
#include <Disks/DiskLocal.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <rocksdb/convenience.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/snapshot.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

namespace fs = std::filesystem;

/// The key-value format of rocks db will be
/// - key: Int8 (depth of the path) + String (path)
/// - value: SizeOf(keeperRocksNodeInfo) (meta of the node) + String (data)

template <class Node_>
struct RocksDBContainer
{
    using Node = Node_;

private:
    UInt8 getKeyDepth(const std::string & key)
    {
        UInt8 depth = 0;
        for (size_t i = 0; i < key.size(); i++)
        {
            if (key[i] == '/' && i + 1 != key.size())
                depth ++;
        }
        return depth;
    }

    std::string getEncodedKey(const std::string & key, bool child_prefix = false)
    {
        WriteBufferFromOwnString key_buffer;
        UInt16 depth = getKeyDepth(key) + child_prefix;
        writeIntBinary(depth, key_buffer);
        writeStringBinary(key, key_buffer);
        return key_buffer.str();
    }


    struct KVPair
    {
        StringRef key;
        Node value;
    };

    /// using KVPointer = std::shared_ptr<KVPair>;
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
            iter->SeekToFirst();
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
            return pair->key.toView() != other->key.toView();
        }

        bool operator == (const const_iterator & other) const
        {
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
            if (iter && iter->Valid())
            {
                auto new_pair = std::make_shared<KVPair>();
                new_pair->key = iter->key().ToString();
                new_pair->value.reset();
                ReadBufferFromOwnString buffer(iter->value().ToStringView());
                typename Node::Meta & meta = new_pair->value;
                readPODBinary(meta, buffer);
                readVarUInt(new_pair->value.data_size, buffer);
                if (new_pair->value.data_size)
                    buffer.readStrict(new_pair->value.data.get(), new_pair->value.data_size);
                pair = new_pair;
            }
            else
            {
                pair = nullptr;
                iter = nullptr;
            }
            return *this;
        }
    };

    const const_iterator end_ptr;

    void initialize(const KeeperContextPtr & context)
    {

        DiskPtr disk = context->getTemporaryRocksDBDisk();
        if (disk == nullptr)
        {
            return;
        }
        auto options = context->getRocksDBOptions();
        if (options == nullptr)
        {
            return;
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
        /// storage_ptr = storage_;
    }

    ~RocksDBContainer()
    {
        rocksdb_ptr->Close();
        rocksdb_ptr = nullptr;

        fs::remove_all(rocksdb_dir);
    }

    std::vector<KVPair> getChildren(const std::string & key)
    {
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;

        auto iter = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(read_options));
        rocksdb::Slice prefix = getEncodedKey(key, true);
        std::vector<KVPair> result;
        for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next())
        {
            Node node;
            ReadBufferFromOwnString buffer(iter->value().ToStringView());
            typename Node::Meta & meta = node;
            /// We do not read data here
            readPODBinary(meta, buffer);
            result.emplace_back(iter->key().ToString(), std::move(node));
        }
        return result;
    }

    bool contains(const std::string & path)
    {
        const std::string & encoded_key = getEncodedKey(path);
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), encoded_key, &buffer_str);
        if (status.IsNotFound())
            return false;
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during find. The error message is {}.", status.ToString());
        return true;
    }

    const_iterator find(StringRef key_)
    {
        /// rocksdb::PinnableSlice slice;
        const std::string & encoded_key = getEncodedKey(key_.toString());
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), encoded_key, &buffer_str);
        if (status.IsNotFound())
            return end();
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during find. The error message is {}.", status.ToString());
        ReadBufferFromOwnString buffer(buffer_str);
        auto kv = std::make_shared<KVPair>();
        kv->key = key_;
        typename Node::Meta & meta = kv->value;
        readPODBinary(meta, buffer);
        /// TODO: Sometimes we don't need to load data.
        readVarUInt(kv->value.data_size, buffer);
        if (kv->value.data_size)
            buffer.readStrict(kv->value.data.get(), kv->value.data_size);
        return const_iterator(kv);
    }

    const_iterator updateValue(StringRef key_, ValueUpdater updater)
    {
        /// rocksdb::PinnableSlice slice;
        const std::string & key = key_.toString();
        const std::string & encoded_key = getEncodedKey(key);
        std::string buffer_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), encoded_key, &buffer_str);
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during find. The error message is {}.", status.ToString());
        auto kv = std::make_shared<KVPair>();
        kv->key = key_;
        kv->value.decodeFromString(buffer_str);
        /// storage->removeDigest(node, key);
        updater(kv->value);
        insertOrReplace(key, kv->value);
        return const_iterator(kv);
    }

    bool insert(const std::string & key, Node & value)
    {
        std::string value_str;
        const std::string & encoded_key = getEncodedKey(key);
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), encoded_key, &value_str);
        if (status.ok())
        {
            return false;
        }
        else if (status.IsNotFound())
        {
            // storage->addDigest(value, key);
            status = rocksdb_ptr->Put(rocksdb::WriteOptions(), encoded_key, value.getEncodedString());
            if (status.ok())
            {
                counter++;
                return true;
            }
        }

        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during insert. The error message is {}.", status.ToString());
    }

    void insertOrReplace(const std::string & key, Node & value)
    {
        const std::string & encoded_key = getEncodedKey(key);
        /// storage->addDigest(value, key);
        std::string value_str;
        rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), encoded_key, &value_str);
        bool increase_counter = false;
        if (status.IsNotFound())
            increase_counter = true;
        else if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Got rocksdb error during get. The error message is {}.", status.ToString());

        status = rocksdb_ptr->Put(rocksdb::WriteOptions(), encoded_key, value.getEncodedString());
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
        std::string key(key_data.release(), key_size);
        insertOrReplace(key, value);
    }

    bool erase(const std::string & key)
    {
        /// storage->removeDigest(value, key);
        auto status = rocksdb_ptr->Delete(rocksdb::WriteOptions(), key);
        if (status.IsNotFound())
            return false;
        if (status.ok())
        {
            counter --;
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
        return std::make_pair(counter, current_version);
    }

    const_iterator begin() const
    {
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;
        if (snapshot_mode)
            read_options.snapshot = snapshot;
        return const_iterator(rocksdb_ptr->NewIterator(read_options));
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

    /// Storage* storage_ptr;

    const rocksdb::Snapshot * snapshot;

    bool snapshot_mode{false};
    size_t current_version{0};
    size_t snapshot_up_to_version{0};
    size_t counter{0};

};

}
