#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>

using namespace DB::Cas;

namespace
{
template <typename F>
void expectThrowsCode(int expected_code, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected exception " << expected_code;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code) << e.message();
    }
}
}

/// The real cas_pool_meta case replaces the phase-1 toy proving instance. Every other control-plane
/// format registers its own battery row in its own gtest_cas_<object>_format.cpp file (Tasks 3-6).

CAS_BATTERY_COVERS(PoolMeta);

TEST(CASFormatBattery, PoolMeta)
{
    PoolMeta pm;
    pm.pool_id = hexToU128("00112233445566778899aabbccddeeff");
    pm.blob_header_len = 256;
    pm.min_reader_generation = 1;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};
    runFormatBattery(FormatBatteryCase{
        .id = FormatId::PoolMeta,
        .encode = [&] { return sealObject(FormatId::PoolMeta, encodePoolMeta(pm)); },
        .decode = [](std::string_view s) { decodePoolMeta(std::string(openObject(FormatId::PoolMeta, s))); },
        .golden = currentFormatHeader("cas_pool_meta") +
                  "{\"pool_id\":\"00112233445566778899aabbccddeeff\",\"blob_header_len\":256,\"gc_shards\":1,\"min_reader_generation\":1,\"algos_used\":[\"ch128\"]}\n"});
}

TEST(CASPoolMeta, RejectsInvalidAlgoArrays)
{
    const auto decode = [](std::string_view algos_used)
    {
        return decodePoolMeta("{\"type\":\"cas_pool_meta\",\"v\":1}\n"
            "{\"pool_id\":\"00112233445566778899aabbccddeeff\",\"blob_header_len\":256,\"gc_shards\":1,\"min_reader_generation\":1,\"algos_used\":" + String(algos_used) + "}\n");
    };

    /// The first value is the field's PREVIOUS encoding -- a comma-joined string inside one JSON
    /// value. It must fail closed rather than round-trip; the rest are malformed arrays.
    for (const std::string_view bad : {"\"ch128,sha256\"", "[\"ch128\",1]", "[]", "[\"sha256\",\"ch128\"]", "[\"ch128\",\"ch128\"]", "[\"unknown\"]"})
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decode(bad); });
}

TEST(CASPoolMeta, ValidateAlgosUsedRejectsUnknownByte)
{
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [] { validatePoolAlgosUsed({7}, DB::ErrorCodes::CORRUPTED_DATA, "t"); });
}
