#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>

using namespace DB::Cas;

/// The real cas_pool_meta case replaces the phase-1 toy proving instance. Every other control-plane
/// format registers its own battery row in its own gtest_cas_<object>_format.cpp file (Tasks 3-6).

TEST(CASFormatBattery, PoolMeta)
{
    PoolMeta pm;
    pm.pool_id = hexToU128("00112233445566778899aabbccddeeff");
    pm.blob_header_len = 256;
    pm.min_reader_generation = 3;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};
    runFormatBattery(FormatBatteryCase{
        .id = FormatId::PoolMeta,
        .encode = [&] { return sealObject(FormatId::PoolMeta, encodePoolMeta(pm)); },
        .decode = [](std::string_view s) { decodePoolMeta(std::string(openObject(FormatId::PoolMeta, s))); },
        .golden = currentFormatHeader("cas_pool_meta") +
                  "{\"pid\":\"00112233445566778899aabbccddeeff\",\"hln\":256,\"gcs\":1,\"mrg\":3,\"alg\":\"ch128\"}\n"});
}
