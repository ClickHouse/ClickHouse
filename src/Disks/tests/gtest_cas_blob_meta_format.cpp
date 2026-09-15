#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>
#include <IO/ReadBufferFromMemory.h>

#include <magic_enum.hpp>

using namespace DB::Cas;

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; }

namespace
{
/// Same tiny inline copy as `gtest_cas_wire_vocab.cpp`'s `expectThrowsCode`: stays clear of
/// `Disks/tests/cas_test_helpers.h`'s `DB::Cas::tests::expectThrowsCode`, which would both drag
/// in the whole CAS backend/store machinery this file otherwise has no need for AND collide (same
/// namespace, same name and signature) if that header were ever included here too.
template <typename F>
void expectThrowsCode(int expected_code, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}
}

CAS_BATTERY_COVERS(BlobMeta);

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
        .golden = "{\"type\":\"cas_blob_meta\",\"v\":1}\n"
                  "{\"state\":\"clean\",\"condemn_round\":\"0\",\"size\":\"12345\"}\n"});
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
        "{\"type\":\"cas_blob_meta\",\"v\":1}\n{\"state\":\"condemned\",\"condemn_round\":\"7\",\"size\":\"4096\"}\n");
}

/// Closed-set pin: the two `MetaState` wire words, walked through `magic_enum::enum_values` so a
/// future state a `MetaState` construction can reach but no table entry names would fail this
/// exhaustive check rather than silently pass through unspecified.
TEST(CASBlobMetaFormat, ClosedSetPinsMetaStateWords)
{
    EXPECT_EQ(metaStateToWireWord(MetaState::Clean), "clean");
    EXPECT_EQ(metaStateToWireWord(MetaState::Condemned), "condemned");
    for (const auto state : magic_enum::enum_values<MetaState>())
        EXPECT_EQ(metaStateFromWireWord(metaStateToWireWord(state)), state);
}

TEST(CASBlobMetaFormat, FailsClosedOnUnknownStateAndTruncation)
{
    /// Unknown state word -> CORRUPTED_DATA (mirrors the old `state > Condemned` reject).
    /// `v:1` is the baseline generation, so it always passes the header gate -- the BODY is what has
    /// to fail here.
    const String bad_state = "{\"type\":\"cas_blob_meta\",\"v\":1}\n{\"state\":\"zombie\",\"condemn_round\":\"0\",\"size\":\"0\"}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeBlobMeta(bad_state); });
    /// Missing state key -> CORRUPTED_DATA.
    const String no_state = "{\"type\":\"cas_blob_meta\",\"v\":1}\n{\"condemn_round\":\"0\",\"size\":\"0\"}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeBlobMeta(no_state); });
    /// Truncated (header only) -> CORRUPTED_DATA.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { decodeBlobMeta("{\"type\":\"cas_blob_meta\",\"v\":1}\n"); });
}
