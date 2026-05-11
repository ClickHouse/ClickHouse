#pragma once

#include <Storages/MergeTree/ANNIndex/PartRowId.h>

#include <cstddef>
#include <cstdint>

namespace DB::PartRowIdMapFormat
{

/// `id_map.bin` on-disk layout:
///   header (32 bytes):
///       UInt32 magic                  = 0x50444D49 ("IDMP" little-endian)
///       UInt32 version                = 1
///       UInt32 record_size            = 24 (== sizeof(PartRowId))
///       UInt32 partition_hash_algo    = 1 (SIPHASH64); must match meta.json `hash_algo`
///       UInt64 record_count
///       UInt64 reserved               = 0
///   body:
///       record_count × PartRowId (24 bytes each)
///   footer (16 bytes):
///       UInt64 checksum               = XXH64 of body
///       UInt64 magic_end              = 0x5F5F50414D444E45 ("ENDMAP__")

constexpr UInt32 MAGIC = 0x50444D49u;                          /// "IDMP"
constexpr UInt32 VERSION = 1;
constexpr UInt32 RECORD_SIZE = 24;
constexpr UInt32 PARTITION_HASH_ALGO_SIPHASH64 = 1;
constexpr UInt64 MAGIC_END = 0x5F5F50414D444E45ULL;            /// "ENDMAP__"
constexpr size_t HEADER_SIZE = 32;
constexpr size_t FOOTER_SIZE = 16;

static_assert(sizeof(PartRowId) == RECORD_SIZE,
    "PartRowId must be 24 bytes for id_map.bin record layout");

/// XXH64 wrapper exposed so tests can verify on-disk checksums without
/// including `<xxhash.h>` directly. Keeping the hash call inside the dbms
/// object library avoids triggering clang's `-Wused-but-marked-unused`
/// diagnostic that fires on the inline symbol when linked into test TUs.
UInt64 hashBody(const void * data, size_t size);

}
