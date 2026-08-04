#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>
#include <IO/ReadBufferFromMemory.h>

using namespace DB::Cas;

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; }

TEST(CASFormatBattery, BlobMeta)
{
    BlobMeta m;
    m.state = MetaState::Clean;
    m.condemn_round = 0;
    m.size = 12345;
    runFormatBattery(FormatBatteryCase{
        .id = FormatId::BlobMeta,
        .encode = [&] { return sealObject(FormatId::BlobMeta, encodeBlobMeta(m)); },
        .decode = [](std::string_view s) { decodeBlobMeta(std::string(openObject(FormatId::BlobMeta, s))); },
        .golden = "{\"type\":\"cas_blob_meta\",\"v\":9}\n"
                  "{\"st\":\"clean\",\"cr\":\"0\",\"sz\":\"12345\"}\n"});
}

TEST(CASBlobMetaFormat, CondemnedRoundTripAllFields)
{
    BlobMeta m;
    m.state = MetaState::Condemned;
    m.condemn_round = 7;
    m.size = 4096;
    const BlobMeta back = decodeBlobMeta(encodeBlobMeta(m));
    EXPECT_EQ(back.state, MetaState::Condemned);
    EXPECT_EQ(back.condemn_round, 7u);
    EXPECT_EQ(back.size, 4096u);
    EXPECT_EQ(encodeBlobMeta(m),
        "{\"type\":\"cas_blob_meta\",\"v\":9}\n{\"st\":\"condemned\",\"cr\":\"7\",\"sz\":\"4096\"}\n");
}

TEST(CASBlobMetaFormat, FailsClosedOnUnknownStateAndTruncation)
{
    /// Unknown state word -> CORRUPTED_DATA (mirrors the old `state > Condemned` reject).
    /// `v:3` is deliberate and must NOT follow a future `G_BUILD` bump: any version <= G_BUILD passes
    /// the header gate, which is the point — the BODY is what has to fail here.
    const String bad_state = "{\"type\":\"cas_blob_meta\",\"v\":3}\n{\"st\":\"zombie\",\"cr\":\"0\",\"sz\":\"0\"}\n";
    EXPECT_THROW(decodeBlobMeta(bad_state), DB::Exception);
    /// Missing state key -> CORRUPTED_DATA.
    const String no_state = "{\"type\":\"cas_blob_meta\",\"v\":3}\n{\"cr\":\"0\",\"sz\":\"0\"}\n";
    EXPECT_THROW(decodeBlobMeta(no_state), DB::Exception);
    /// Truncated (header only) -> CORRUPTED_DATA.
    EXPECT_THROW(decodeBlobMeta("{\"type\":\"cas_blob_meta\",\"v\":3}\n"), DB::Exception);
}
