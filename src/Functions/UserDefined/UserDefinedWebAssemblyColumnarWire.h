#pragma once

/// Frame layout of the `COLUMNAR_V1` WASM UDF ABI. Encoder and decoder are in
/// UserDefinedWebAssemblyColumnarCodec; the format is documented in wasm_udf.mdx.
///
///   [ 16 B header: magic | version | reserved | num_rows | num_cols ]
///   [ ColDescriptor x num_cols ]
///   [ column data, at offsets given by the descriptors ]
///
/// All offsets are absolute byte offsets into the frame; 0 means absent.

#include <cstdint>

namespace DB::ColumnarWire
{

constexpr uint32_t COL_BYTES        = 0;  /// String
constexpr uint32_t COL_FIXED8       = 1;
constexpr uint32_t COL_FIXED16      = 2;
constexpr uint32_t COL_FIXED32      = 3;
constexpr uint32_t COL_FIXED64      = 4;
constexpr uint32_t COL_COMPLEX      = 5;  /// Array, Tuple, Map (as Array(Tuple(K, V)))
constexpr uint32_t COL_VARIANT      = 6;
constexpr uint32_t COL_FIXEDN       = 7;  /// Other fixed widths; width = data_size / num_rows
constexpr uint32_t COL_LOWCARD      = 8;  /// Top-level LowCardinality: dictionary + indexes

constexpr uint32_t COL_IS_NULLABLE  = 0x20u; /// u8 null map at null_offset
constexpr uint32_t COL_IS_CONST     = 0x80u; /// One row of data for all rows

constexpr uint32_t FRAME_MAGIC = 0x4E494243; /// "CBIN"
constexpr uint16_t FRAME_VERSION = 1;

/// 16 rather than 10 bytes so descriptors are 8-byte aligned. `reserved` must be 0.
constexpr uint32_t FRAME_HEADER_BYTES = 16;
constexpr uint32_t COL_DESC_BYTES   = 40;

struct ColDescriptor
{
    uint64_t type;
    uint64_t null_offset;
    uint64_t offsets_offset;
    uint64_t data_offset;
    uint64_t data_size;
};
static_assert(sizeof(ColDescriptor) == COL_DESC_BYTES);

}
