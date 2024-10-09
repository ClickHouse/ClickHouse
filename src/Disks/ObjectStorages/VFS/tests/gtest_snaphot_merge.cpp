#include <algorithm>
#include <iostream>
#include <Disks/ObjectStorages/VFS/VFSShapshotMetadata.h>
#include <Disks/ObjectStorages/VFS/VFSSnapshot.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>


using namespace DB;

class VFSSnapshotDataFromString : public DB::VFSSnapshotDataBase
{
public:
    VFSSnapshotDataFromString() = default;

    void setSnaphotData(String str) { old_snapshot_data = str; }
    String getSnapshotdata() const { return new_snaphot_data; }
    VFSSnapshotEntries getEnriesToRemoveAfterLastMerge() const { return latest_entires_to_remove; }

protected:
    std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata &) const override
    {
        return std::unique_ptr<ReadBuffer>(new ReadBufferFromString(old_snapshot_data));
    }
    std::pair<std::unique_ptr<WriteBuffer>, String> getShapshotWriteBufferAndSnaphotObject(const SnapshotMetadata &) const override
    {
        String key_placeholed = "test";
        auto write_buffer = std::unique_ptr<WriteBuffer>(new WriteBufferFromString(new_snaphot_data));
        return {std::move(write_buffer), key_placeholed};
    }

    void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) override { latest_entires_to_remove = entires_to_remove; }

private:
    String old_snapshot_data;
    mutable String new_snaphot_data;
    VFSSnapshotEntries latest_entires_to_remove;
};


String convertSnaphotEntiesToString(const VFSSnapshotEntries & entries)
{
    String serialized;
    {
        WriteBufferFromString wb(serialized);
        for (const auto & entry : entries)
        {
            entry.serialize(wb);
        }
    }
    return serialized;
}

TEST(DiskObjectStorageVFS, SnaphotEntriesSerialization)
{
    VFSSnapshotEntries entries = {{"/b", 1}, {"/a", 1}};

    String serialized = convertSnaphotEntiesToString(entries);

    String expected_serialized = "/b 1\n/a 1\n";
    EXPECT_EQ(expected_serialized, serialized);

    VFSSnapshotEntries deserialized;
    {
        ReadBufferFromString rb(serialized);

        while (auto entry = VFSSnapshotEntry::deserialize(rb))
        {
            deserialized.emplace_back(*entry);
        }
    }
    EXPECT_EQ(deserialized.size(), entries.size());
    EXPECT_EQ(deserialized, entries);
}

TEST(DiskObjectStorageVFS, ExceptionOnUnsortedSnapshot)
{
    VFSSnapshotEntries entries = {{"/b", 1}, {"/a", 1}, {"/c", 2}};
    VFSSnapshotDataFromString snapshot_data;
    snapshot_data.setSnaphotData(convertSnaphotEntiesToString(entries));

    SnapshotMetadata placeholder_metadata;

    EXPECT_THROW(auto new_snapshot = snapshot_data.mergeWithWals({}, placeholder_metadata), Exception) << "Snaphot is unsorted";
}

// FIXME: Restore the test
// TEST(DiskObjectStorageVFS, MergeWalWithSnaphot)
// {
//     VFSLogItems wals = {{"/c", 1}, {"/d", 2}, {"/b", 1}};

//     String expected_snaphot_state;
//     VFSSnapshotDataFromString snapshot_data;
//     SnapshotMetadata placeholder_metadata;
//     {
//         snapshot_data.setSnaphotData("");
//         snapshot_data.mergeWithWals(wals, placeholder_metadata);

//         auto to_remove = snapshot_data.getEnriesToRemoveAfterLastMerge();
//         VFSSnapshotEntries expected_entries_to_remove = {};

//         EXPECT_EQ(expected_entries_to_remove, to_remove);
//     }
//     expected_snaphot_state = "/b 1\n/c 1\n/d 2\n";
//     EXPECT_EQ(expected_snaphot_state, snapshot_data.getSnapshotdata());

//     wals = {{"/c", -1}, {"/d", -1}, {"/d", -1}, {"/e", -1}, {"/e", 1}, {"/f", 1}, {"/f", 1}, {"/f", -1}, {"/f", 1}};
//     {
//         snapshot_data.setSnaphotData(expected_snaphot_state);
//         snapshot_data.mergeWithWals(wals, placeholder_metadata);

//         auto to_remove = snapshot_data.getEnriesToRemoveAfterLastMerge();
//         VFSSnapshotEntries expected_entries_to_remove = {{"/c", 0}, {"/d", 0}, {"/e", 0}};

//         EXPECT_EQ(expected_entries_to_remove, to_remove);
//     }
//     expected_snaphot_state = "/b 1\n/f 2\n";
//     EXPECT_EQ(expected_snaphot_state, snapshot_data.getSnapshotdata());

//     wals = {
//         {"/a", 1},
//         {"/b", 1},
//         {"/c", 1},
//     };

//     {
//         snapshot_data.setSnaphotData(expected_snaphot_state);

//         snapshot_data.mergeWithWals(wals, placeholder_metadata);
//         auto to_remove = snapshot_data.getEnriesToRemoveAfterLastMerge();
//         VFSSnapshotEntries expected_entries_to_remove;

//         EXPECT_EQ(expected_entries_to_remove, to_remove);
//     }
//     expected_snaphot_state = "/a 1\n/b 2\n/c 1\n/f 2\n";
//     EXPECT_EQ(expected_snaphot_state, snapshot_data.getSnapshotdata());

//     wals = {{"/a", -1}, {"/b", -2}, {"/c", -1}, {"/f", -2}};
//     {
//         snapshot_data.setSnaphotData(expected_snaphot_state);
//         snapshot_data.mergeWithWals(wals, placeholder_metadata);

//         auto to_remove = snapshot_data.getEnriesToRemoveAfterLastMerge();
//         VFSSnapshotEntries expected_entries_to_remove = {{"/a", 0}, {"/b", 0}, {"/c", 0}, {"/f", 0}};

//         EXPECT_EQ(expected_entries_to_remove, to_remove);
//     }
//     expected_snaphot_state = "";
//     EXPECT_EQ(expected_snaphot_state, snapshot_data.getSnapshotdata());
// }
