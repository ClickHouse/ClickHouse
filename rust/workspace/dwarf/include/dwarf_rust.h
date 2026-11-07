#ifndef CLICKHOUSE_DWARF_RUST_H
#define CLICKHOUSE_DWARF_RUST_H

// NOLINTBEGIN(modernize-deprecated-headers,modernize-use-using,google-objc-function-naming)

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
extern "C" {
#else
#include <stddef.h>
#include <stdint.h>
#endif

#define CH_DWARF_ERROR_BUF_SIZE 256u

typedef struct ChDwarfFile ChDwarfFile;
typedef struct ChDwarfUnit ChDwarfUnit;

typedef enum ChDwarfStatus
{
    CH_DWARF_OK = 0,
    CH_DWARF_CHUNK_READY = 1,
    CH_DWARF_UNIT_DONE = 2,
    CH_DWARF_CANCELLED = 3,

    CH_DWARF_ERR_INVALID_ARG = -1,
    CH_DWARF_ERR_ELF = -2,
    CH_DWARF_ERR_MISSING_REQUIRED_SECTION = -3,
    CH_DWARF_ERR_DWARF = -4,
    CH_DWARF_ERR_MISSING_AUX_SECTION = -5,
    CH_DWARF_ERR_CALLBACK = -6,
    CH_DWARF_ERR_INTERNAL = -7
} ChDwarfStatus;

typedef enum ChDwarfCancelStatus
{
    CH_DWARF_CANCEL_CONTINUE = 0,
    CH_DWARF_CANCEL_STOP = 1,
    CH_DWARF_CANCEL_FAILED = 2
} ChDwarfCancelStatus;

typedef enum ChDwarfReserveStatus
{
    CH_DWARF_RESERVE_OK = 0,
    CH_DWARF_RESERVE_FAILED = 1
} ChDwarfReserveStatus;

typedef enum ChDwarfNameTableKind
{
    CH_DWARF_NAME_TABLE_TAG = 0,
    CH_DWARF_NAME_TABLE_ATTR = 1,
    CH_DWARF_NAME_TABLE_FORM = 2
} ChDwarfNameTableKind;

typedef enum ChDwarfColumnBit
{
    CH_DWARF_COL_OFFSET = (1u << 0),
    CH_DWARF_COL_SIZE = (1u << 1),
    CH_DWARF_COL_TAG = (1u << 2),
    CH_DWARF_COL_UNIT_NAME = (1u << 3),
    CH_DWARF_COL_UNIT_OFFSET = (1u << 4),
    CH_DWARF_COL_ANCESTOR_TAGS = (1u << 5),
    CH_DWARF_COL_ANCESTOR_OFFSETS = (1u << 6),
    CH_DWARF_COL_NAME = (1u << 7),
    CH_DWARF_COL_LINKAGE_NAME = (1u << 8),
    CH_DWARF_COL_DECL_FILE = (1u << 9),
    CH_DWARF_COL_DECL_LINE = (1u << 10),
    CH_DWARF_COL_RANGES = (1u << 11),
    CH_DWARF_COL_ATTR_NAME = (1u << 12),
    CH_DWARF_COL_ATTR_FORM = (1u << 13),
    CH_DWARF_COL_ATTR_INT = (1u << 14),
    CH_DWARF_COL_ATTR_STR = (1u << 15)
} ChDwarfColumnBit;

typedef enum ChDwarfSinkId
{
    CH_DWARF_SINK_OFFSET = 0,
    CH_DWARF_SINK_SIZE = 1,
    CH_DWARF_SINK_TAG = 2,
    CH_DWARF_SINK_ANCESTOR_TAGS = 3,
    CH_DWARF_SINK_ANCESTOR_OFFSETS = 4,
    CH_DWARF_SINK_ANCESTOR_ARRAY_OFFSETS = 5,
    CH_DWARF_SINK_NAME_CHARS = 6,
    CH_DWARF_SINK_NAME_OFFSETS = 7,
    CH_DWARF_SINK_LINKAGE_NAME_CHARS = 8,
    CH_DWARF_SINK_LINKAGE_NAME_OFFSETS = 9,
    CH_DWARF_SINK_DECL_FILE_CHARS = 10,
    CH_DWARF_SINK_DECL_FILE_OFFSETS = 11,
    CH_DWARF_SINK_DECL_LINE = 12,
    CH_DWARF_SINK_RANGES_START = 13,
    CH_DWARF_SINK_RANGES_END = 14,
    CH_DWARF_SINK_RANGES_OFFSETS = 15,
    CH_DWARF_SINK_ATTR_NAME = 16,
    CH_DWARF_SINK_ATTR_FORM = 17,
    CH_DWARF_SINK_ATTR_INT = 18,
    CH_DWARF_SINK_ATTR_STR_CHARS = 19,
    CH_DWARF_SINK_ATTR_STR_OFFSETS = 20,
    CH_DWARF_SINK_ATTR_OFFSETS = 21,
    CH_DWARF_SINK_COUNT = 22
} ChDwarfSinkId;

typedef struct ChDwarfOptions
{
    uint32_t flags;
    uint32_t reserved;
} ChDwarfOptions;

typedef struct ChDwarfProjection
{
    uint32_t column_mask;
    uint32_t need_ancestor_offsets_vec;
    uint32_t need_attr_offsets_vec;
    uint32_t reserved;
} ChDwarfProjection;

typedef struct ChDwarfSink
{
    uint8_t * data;
    size_t len;
    size_t cap;
    uint32_t elem_size;
    uint32_t reserved;
} ChDwarfSink;

typedef struct ChDwarfStringSink
{
    ChDwarfSink chars;
    ChDwarfSink offsets;
} ChDwarfStringSink;

typedef struct ChDwarfBatchSinks
{
    ChDwarfSink sinks[CH_DWARF_SINK_COUNT];
} ChDwarfBatchSinks;

typedef struct ChDwarfReserveRequest
{
    uint32_t sink_id;
    uint32_t reserved;
    size_t min_cap;
} ChDwarfReserveRequest;

typedef struct ChDwarfProgress
{
    size_t rows;
    uint64_t bytes_advanced;
    const uint8_t * unit_name_ptr;
    size_t unit_name_len;
    uint32_t unit_done;
    uint32_t reserved;
} ChDwarfProgress;

typedef int32_t (*ChDwarfReserveFn)(
    void * opaque,
    ChDwarfBatchSinks * sinks,
    const ChDwarfReserveRequest * requests,
    size_t request_count);

typedef int32_t (*ChDwarfStringReserveFn)(
    void * opaque,
    ChDwarfStringSink * sink,
    size_t min_chars_cap,
    size_t min_offsets_cap);

typedef int32_t (*ChDwarfCancelFn)(void * opaque);

int32_t ch_dwarf_file_open(
    const uint8_t * bytes,
    size_t len,
    const ChDwarfOptions * options,
    ChDwarfFile ** out_file,
    char error[CH_DWARF_ERROR_BUF_SIZE]);

int32_t ch_dwarf_file_unit_count(
    const ChDwarfFile * file,
    size_t * out_count);

int32_t ch_dwarf_unit_open(
    const ChDwarfFile * file,
    size_t unit_index,
    ChDwarfUnit ** out_unit,
    uint64_t * out_unit_offset,
    char error[CH_DWARF_ERROR_BUF_SIZE]);

int32_t ch_dwarf_unit_parse_chunk(
    const ChDwarfFile * file,
    ChDwarfUnit * unit,
    const ChDwarfProjection * projection,
    ChDwarfBatchSinks * sinks,
    ChDwarfReserveFn reserve_cb,
    ChDwarfCancelFn cancel_cb,
    void * opaque,
    size_t max_rows,
    ChDwarfProgress * out_progress,
    char error[CH_DWARF_ERROR_BUF_SIZE]);

int32_t ch_dwarf_write_name_table(
    int32_t kind,
    ChDwarfStringSink * string_sink,
    ChDwarfStringReserveFn reserve_cb,
    void * opaque,
    char error[CH_DWARF_ERROR_BUF_SIZE]);

void ch_dwarf_unit_free(ChDwarfUnit * unit);

void ch_dwarf_file_free(ChDwarfFile * file);

#ifdef __cplusplus
}
#endif

// NOLINTEND(modernize-deprecated-headers,modernize-use-using,google-objc-function-naming)

#endif /* CLICKHOUSE_DWARF_RUST_H */
