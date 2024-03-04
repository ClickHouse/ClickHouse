#include <algorithm>

#include <Disks/ObjectStorages/VFSLogItem.h>
#include <Disks/ObjectStorages/VFSSnapshotStorage.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include "gtest/gtest.h"

using namespace DB;
using namespace std::string_view_literals;

namespace DB
{
constexpr bool operator==(const DB::StoredObject & left, const DB::StoredObject & right)
{
    return left.remote_path == right.remote_path;
}

std::ostream & operator<<(std::ostream & stream, const DB::StoredObject & obj)
{
    return stream << fmt::format("{}", obj);
}
}


class VFSSnapshotReadStreamFromString : public IVFSSnapshotReadStream
{
public:
    using IVFSSnapshotReadStream::entry_type;

    VFSSnapshotReadStreamFromString(String data) : read_buffer(data) { }

private:
    entry_type nextImpl() override
    {
        if (read_buffer->eof())
            return {};
        return VFSSnapshotEntryStringSerializer::deserialize(*read_buffer);
    }

    std::optional<ReadBufferFromString> read_buffer;
};


class VFSSnapshotWriteStreamFromString : public IVFSSnapshotWriteStream
{
public:
    VFSSnapshotWriteStreamFromString() { write_buffer.emplace(); }

    std::string & str() { return write_buffer->str(); }

private:
    void writeImpl(VFSSnapshotEntry && entry) override { VFSSnapshotEntryStringSerializer::serialize(std::move(entry), *write_buffer); }

    void finalizeImpl() override { write_buffer->finalize(); }

    std::optional<WriteBufferFromOwnString> write_buffer;
};


TEST(DiskObjectStorageVFS, VFSLogItem)
{
    Poco::Logger * log = &Poco::Logger::root();
    EXPECT_EQ(VFSLogItem::parse(""), VFSLogItem{});

    VFSLogItem item{{{"link", 1}, {"unlink", -1}}};
    EXPECT_EQ(item, VFSLogItem::parse(fmt::format("{}", fmt::join(item.serialize(), ""))));
    EXPECT_EQ(item.serialize().size(), 1);

    item.merge({{{"link", 5}, {"unlink", 1}, {"delta", -2}}});
    EXPECT_EQ(item, (VFSLogItem{{{"link", 6}, {"unlink", 0}, {"delta", -2}}}));

    {
        auto read_snapshot = VFSSnapshotReadStreamFromString("invalid");
        auto write_snapshot = VFSSnapshotWriteStreamFromString();
        EXPECT_THROW(VFSLogItem{}.mergeWithSnapshot(read_snapshot, write_snapshot, log), Exception) << "Invalid input buffer";
    }

    {
        auto read_snapshot = VFSSnapshotReadStreamFromString("");
        auto write_snapshot = VFSSnapshotWriteStreamFromString();
        auto res = VFSLogItem{item}.mergeWithSnapshot(read_snapshot, write_snapshot, log);
        EXPECT_EQ(res.obsolete, StoredObjects{StoredObject{"unlink"}});
        EXPECT_EQ(res.invalid, (VFSLogItemStorage{{"delta", -2}}));
    }

    String serialized_snapshot;
    {
        auto read_snapshot = VFSSnapshotReadStreamFromString("delta 2\n");
        auto write_snapshot = VFSSnapshotWriteStreamFromString();
        auto res = std::move(item).mergeWithSnapshot(read_snapshot, write_snapshot, log);
        EXPECT_EQ(res.obsolete, (StoredObjects{StoredObject{"delta"}, StoredObject{"unlink"}}));
        EXPECT_EQ(res.invalid, VFSLogItemStorage{});
        serialized_snapshot = write_snapshot.str();
    }

    {
        auto read_snapshot = VFSSnapshotReadStreamFromString(serialized_snapshot);
        auto write_snapshot = VFSSnapshotWriteStreamFromString();
        auto res_4 = VFSLogItem{}.mergeWithSnapshot(read_snapshot, write_snapshot, log);
        EXPECT_EQ(res_4.obsolete, StoredObjects{});
        EXPECT_EQ(res_4.invalid, VFSLogItemStorage{});
        EXPECT_EQ(serialized_snapshot, write_snapshot.str());
    }
}

TEST(DiskObjectStorageVFS, VFSSnapshotSortingWriteStream)
{
    {
        auto snapshot = std::make_shared<VFSSnapshotWriteStreamFromString>();
        VFSSnapshotSortingWriteStream sorting_snapshot(snapshot);

        sorting_snapshot.write(VFSSnapshotEntry{"/b", 1});
        sorting_snapshot.write(VFSSnapshotEntry{"/c", 1});
        sorting_snapshot.write(VFSSnapshotEntry{"/a", 1});

        sorting_snapshot.finalize();

        auto read_snapshot = VFSSnapshotReadStreamFromString(snapshot->str());

        std::vector<std::string> remote_paths;
        while (auto entry = read_snapshot.next())
            remote_paths.push_back(entry->remote_path);

        ASSERT_EQ(std::ranges::is_sorted(remote_paths), true);
        EXPECT_EQ(remote_paths.size(), 3);
    }
    {
        auto snapshot = std::make_shared<VFSSnapshotWriteStreamFromString>();
        VFSSnapshotSortingWriteStream sorting_snapshot(snapshot);

        sorting_snapshot.write(VFSSnapshotEntry{"/a", 1});
        sorting_snapshot.write(VFSSnapshotEntry{"/a", 1});

        sorting_snapshot.finalize();

        auto read_snapshot = VFSSnapshotReadStreamFromString(snapshot->str());
        auto entry = read_snapshot.next();

        EXPECT_EQ(read_snapshot.next(), std::nullopt);
        EXPECT_EQ(entry->link_count, 2);
    }
}
