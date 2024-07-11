#include <algorithm>
#include <iostream>
#include <Disks/ObjectStorages/VFS/VFSSnapshot.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>


using namespace DB;

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

TEST(DiskObjectStorageVFS, ExceptionOnUnsortedSnahot)
{
    VFSSnapshotEntries entries = {{"/b", 1}, {"/a", 1}, {"/c", 2}};

    auto in_str = convertSnaphotEntiesToString(entries);
    String out_str;

    WALItems empty_wals;
    {
        ReadBufferFromString rb(in_str);
        WriteBufferFromString wb(out_str);

        EXPECT_THROW(auto to_remove = mergeWithWals(empty_wals, rb, wb), Exception) << "Snaphot is unsorted";
    }
}

TEST(DiskObjectStorageVFS, MergeWalWithSnaphot)
{
    WALItems wals = {{"/c", 1}, {"/d", 2}, {"/b", 1}};
    String in_str;
    String out_str;
    String expected_snaphot_state;
    {
        ReadBufferFromString rb(in_str);
        WriteBufferFromString wb(out_str);

        auto to_remove = mergeWithWals(wals, rb, wb);
        VFSSnapshotEntries expected_entries_to_remove = {};
        EXPECT_EQ(expected_entries_to_remove, to_remove);
    }
    expected_snaphot_state = "/b 1\n/c 1\n/d 2\n";
    EXPECT_EQ(expected_snaphot_state, out_str);
    in_str = out_str;

    wals = {{"/c", -1}, {"/d", -1}, {"/d", -1}, {"/e", -1}, {"/e", 1}, {"/f", 1}, {"/f", 1}, {"/f", -1}, {"/f", 1}};
    {
        ReadBufferFromString rb(in_str);
        WriteBufferFromString wb(out_str);

        auto to_remove = mergeWithWals(wals, rb, wb);
        VFSSnapshotEntries expected_entries_to_remove = {{"/c", 0}, {"/d", 0}, {"/e", 0}};

        EXPECT_EQ(expected_entries_to_remove, to_remove);
    }
    expected_snaphot_state = "/b 1\n/f 2\n";
    EXPECT_EQ(expected_snaphot_state, out_str);
    in_str = out_str;

    wals = {
        {"/a", 1},
        {"/b", 1},
        {"/c", 1},
    };

    {
        ReadBufferFromString rb(in_str);
        WriteBufferFromString wb(out_str);

        auto to_remove = mergeWithWals(wals, rb, wb);
        VFSSnapshotEntries expected_entries_to_remove;

        EXPECT_EQ(expected_entries_to_remove, to_remove);
    }
    expected_snaphot_state = "/a 1\n/b 2\n/c 1\n/f 2\n";
    EXPECT_EQ(expected_snaphot_state, out_str);
    in_str = out_str;

    wals = {{"/a", -1}, {"/b", -2}, {"/c", -1}, {"/f", -2}};
    {
        ReadBufferFromString rb(in_str);
        WriteBufferFromString wb(out_str);

        auto to_remove = mergeWithWals(wals, rb, wb);
        VFSSnapshotEntries expected_entries_to_remove = {{"/a", 0}, {"/b", 0}, {"/c", 0}, {"/f", 0}};

        EXPECT_EQ(expected_entries_to_remove, to_remove);
    }
    expected_snaphot_state = "";
    EXPECT_EQ(expected_snaphot_state, out_str);
}
