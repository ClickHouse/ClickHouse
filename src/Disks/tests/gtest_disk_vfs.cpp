#include <Disks/ObjectStorages/VFSLogItem.h>
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

TEST(DiskObjectStorageVFS, VFSLogItem)
{
    Poco::Logger * log = &Poco::Logger::root();
    EXPECT_THROW(VFSLogItem::parse(""), Exception);

    VFSLogItem item{{{"link", 1}, {"unlink", -1}}};
    EXPECT_EQ(item, VFSLogItem::parse(VFSLogItem::getSerialised({StoredObject{"link"}}, {StoredObject{"unlink"}})));

    item.merge({{{"link", 5}, {"unlink", 1}, {"delta", -2}}});
    EXPECT_EQ(item, (VFSLogItem{{{"link", 6}, {"unlink", 0}, {"delta", -2}}}));

    auto in_1 = ReadBufferFromString{"invalid"sv};
    auto out_1 = WriteBufferFromOwnString{};
    EXPECT_THROW(VFSLogItem{}.mergeWithSnapshot(in_1, out_1, log), Exception) << "Invalid input buffer";

    auto in_2 = ReadBufferFromString{""sv};
    auto out_2 = WriteBufferFromOwnString{};
    EXPECT_DEATH({ VFSLogItem{item}.mergeWithSnapshot(in_2, out_2, log); }, "Logical error: '-2 references to delta'.");

    auto in_3 = ReadBufferFromString{"delta 2\n"sv};
    auto out_3 = WriteBufferFromOwnString{};
    EXPECT_EQ(std::move(item).mergeWithSnapshot(in_3, out_3, log), (VFSObsoleteObjects{StoredObject{"delta"}, StoredObject{"unlink"}}));

    const String serialized_snapshot = out_3.str();
    auto in_4 = ReadBufferFromOwnString{serialized_snapshot}; // copy of serialized
    auto out_4 = WriteBufferFromOwnString{};

    EXPECT_EQ(VFSLogItem{}.mergeWithSnapshot(in_4, out_4, log), VFSObsoleteObjects{});
    EXPECT_EQ(serialized_snapshot, out_4.str());
}
