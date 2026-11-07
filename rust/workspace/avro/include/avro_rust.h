#pragma once

/// C ABI of the Rust Avro decoder (`rust/workspace/avro`, package `_ch_rust_avro`).
///
/// Hand written; `src/ffi.rs` has tests that pin the struct layouts used here.
/// This header is valid C99 and C++.
///
/// # What is here
///
///  * A schema compiler (Apache Avro 1.12): parse the schema JSON, walk the
///    schema graph, read attributes (Iceberg `field-id`), get the Parsing
///    Canonical Form.
///  * A program compiler and a resumable executor: a *plan* (what to do with each
///    schema node) and *sinks* (caller-owned output buffers) are compiled into a
///    program; `avro_decode` runs it over a block of Avro binary data.
///  * Object Container File framing on a window of bytes (header, block header,
///    sync marker). Decompression is NOT here: the caller decompresses block
///    payloads with its own, memory-tracked codecs.
///
/// # Conventions
///
///  * Every fallible function returns `int32_t`: `AVRO_OK` (0), the positive
///    statuses `AVRO_NEED_MORE`, `AVRO_END`, `AVRO_NOT_FOUND`, or `-code` where
///    `code` is an `AvroErrorCode`. The message is written to `err` (capacity
///    `err_cap`, NUL terminated, truncated to fit; `err` may be NULL).
///  * Panics never cross this boundary: they become `-AVRO_ERR_INTERNAL`.
///  * A pointer parameter with a length may be NULL only when the length is 0.
///  * Handles (`AvroSchema *`, `AvroProgram *`, `AvroState *`, `AvroOcfHeader *`)
///    are released with the matching `*_free`; freeing NULL is fine. Strings and
///    byte ranges returned by a function point into the handle they came from,
///    are NOT NUL terminated, and stay valid until that handle is freed.
///  * A handle may be used from any thread, but not from two threads at once
///    unless it is only read: schemas, programs and headers are immutable after
///    creation; an `AvroState` is mutated by every `avro_decode`.
///  * Rust allocates nothing proportional to the data. It allocates for schemas
///    and programs (bounded by the schema text and the plan) and the explicit
///    frame stack of a state (bounded by `max_depth`). All output memory is the
///    caller's.
///
/// # Executor contract (`avro_decode`)
///
///  * Input is the plain (decompressed) payload of one block, or the rest of it.
///  * A *sink* receives only the values that are actually decoded into it:
///    fields inside a union branch get one value per row that took that branch,
///    array items are flattened, and offsets sinks say where each array or map
///    ends. Expanding to row alignment (for `Nullable`, `Variant`) uses the
///    null map and union tag sinks and is the caller's job.
///  * `avro_decode` returns when `max_rows` rows are done (`ROW_LIMIT`), when the
///    input ended exactly at a row boundary (`BLOCK_DONE`, only for programs
///    whose rows take at least one byte), or when a sink is full
///    (`NEED_SPACE`). `rows_done`/`consumed` and the sink fill levels cover
///    completed rows only, and a row that fails is rolled back, except that a
///    `NEED_SPACE` in the first row of a call *pauses* the row (below).
///    On `NEED_SPACE`, grow every buffer that `needs` (`need_count` entries; the
///    first is `need_sink`/`need_cap`/`need_cap2`) names to at least its `cap`
///    elements (`cap2` for the secondary buffer), keeping every byte below the fill
///    levels, update those `AvroSink`s, and call again with `input + consumed`, the
///    same state, and `max_rows - rows_done`.
///    - `needs` has every sink that lacks room for what the executor is about to
///      write: one entry for a value, and for a block of at least 16 array or map
///      items whose bytes can hold that many, every sink that each item writes to
///      (found at the start of the loop body, up to its first union), with the
///      room for all of the items, so that a record of many fields costs one stop
///      and not one per field, and one request per sink instead of one for every
///      capacity the caller doubles through. A caller that grows only the first
///      is stopped again for the others, correctly but one at a time.
///    - With `rows_done == 0` (and `consumed == 0`) the row is paused, not
///      discarded: the state keeps its position in the row and the sinks keep
///      what the row wrote so far, so their fill levels are past the row boundary.
///      The state keeps the sinks and their fill levels, so that resuming costs the
///      same whatever the number of sinks: the `AvroSink` array gets back the fill
///      levels of the sinks of `needs` only, and the other `AvroSink`s are not
///      read again until the row is done. The next call, with the same input bytes,
///      the same state and the same `sinks` array, continues where the row stopped
///      and decodes no byte twice, however many sinks grow while the row is
///      decoded. Change nothing but the buffers of the sinks of `needs`. Each `cap`
///      counts from the sink's current fill level. The paused row must be given at
///      least as many input bytes as before; `avro_state_reset` abandons it, and the
///      caller must then discard the fill levels of the row.
///    - With `rows_done > 0` the rows completed before the row that needs room
///      are committed, and that row is rolled back (fill levels at the end of the
///      committed rows) and starts the next call, which pauses on further
///      `NEED_SPACE`. This replays the completed rows of the call once.
///  * `max_rows` must be bounded by the caller (the block's object count).
///  * After an error the state is already reset and the committed rows are in
///    the sinks (see `AvroProgress` and `avro_decode`). The one exception is
///    `AVRO_ERR_EOF` and `AVRO_ERR_COUNT_EXCEEDS_PAYLOAD`, the input ending
///    inside a row: a caller that decodes a block window by window decodes that
///    row again from its start with more bytes, and the budget of zero-size
///    array/map items of the block (counted over the rows that completed) is kept
///    so that a window boundary does not grant a new one. `avro_state_reset`
///    starts a new block.
///  * Streaming (`avro_state_set_streaming(state, 1)`): for a caller that has
///    more bytes of the block than it passes, so that it need not hold a whole
///    row, however large the values that the query does not read are. The input
///    ending inside a row is then `AVRO_STATUS_NEED_INPUT`, not an error:
///    - `consumed` bytes (and `rows_done` rows) are done with. Call again with
///      `input + consumed`, extended with the bytes that follow, the same state
///      and `max_rows - rows_done`.
///    - With `rows_done == 0` the row is *paused* in the executor, as for
///      `NEED_SPACE`: the sinks keep what the row wrote (they and the
///      `AvroSink` array are not touched between the calls), the state keeps
///      where it is. The executor is done with every byte before the value that
///      the input ended in (and with the bytes of a skipped `bytes`, `string` or
///      `fixed` that were there: the rest of it is skipped in the next input), so
///      `consumed` is that much, and a value that is read into a sink is read again
///      from its first byte. The call that continues such a row completes it
///      and returns `ROW_LIMIT` with `rows_done == 1`, whatever `max_rows` is;
///      the caller calls again for the rows that follow.
///    - With `rows_done > 0` the row that was cut is rolled back as for
///      `NEED_SPACE` and the next call starts it (and pauses it).
///    - A caller that has no more bytes calls `avro_state_set_streaming(state, 0)`
///      and then again with what it has: the paused row fails with `AVRO_ERR_EOF`
///      or `AVRO_ERR_COUNT_EXCEEDS_PAYLOAD` as it does without streaming.
///      `BLOCK_DONE` (input exhausted at a row boundary) is the same in both modes.
///  * Sinks need not be initialized: the executor only writes to the buffers
///    (`data`, `data2`) and never reads a byte that it has not written (a
///    `PODArray::resize` leaves new bytes unwritten). It also keeps no reference
///    to them between calls except for a paused row (above).
///  * Bytes left in a block after the last row are not an error here: compare
///    `consumed` with the payload size if you want to reject them.
///  * `float`/`double` are read as little-endian (the specification). Numbers
///    are written to sinks in host byte order. Buffers need no alignment.
///
/// # Limits (all documented, all tested)
///
///  * Schema text: 16 MiB, 1 Mi JSON values; nesting depth `max_depth` (0 means
///    the internal hard cap of 100000). Depth is counted as in the C++ fork: the
///    root is 0, `depth > max_depth` is an error ("Avro schema nesting depth
///    exceeds the limit", "JSON nesting depth exceeds the limit").
///  * Data nesting (arrays, maps and calls into recursive records): `max_depth`
///    of the state, error code `AVRO_ERR_DATA_TOO_DEEP`.
///  * Array and map blocks: a count must not exceed the remaining bytes divided
///    by the minimum encoded size of an item. Items that occupy no bytes (null,
///    empty records) are capped at 16777216 per array and per block.
///  * Blocks: for the null codec `avro_ocf_block_header` checks that the object
///    count fits the payload; for compressed blocks use `avro_ocf_check_object_count`
///    with the decompressed size. Rows that occupy no bytes are capped at
///    268435456 per block.

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/// Statuses (non-negative returns).
enum
{
    AVRO_OK = 0,
    /// The window is too short; `*needed` says how long it must be in total.
    AVRO_NEED_MORE = 1,
    /// `avro_ocf_block_header`: the window is empty and the file ended.
    AVRO_END = 2,
    /// A lookup found nothing.
    AVRO_NOT_FOUND = 3,
};

/// Error codes; functions return their negation. The numbers are stable.
typedef enum AvroErrorCode
{
    AVRO_ERR_INVALID_ARGUMENT = 1,
    AVRO_ERR_JSON_SYNTAX = 2,
    /// "JSON nesting depth exceeds the limit"
    AVRO_ERR_JSON_DEPTH = 3,
    AVRO_ERR_SCHEMA_TOO_LARGE = 4,
    AVRO_ERR_SCHEMA_INVALID = 5,
    /// "Avro schema nesting depth exceeds the limit"
    AVRO_ERR_SCHEMA_DEPTH = 6,
    /// "EOF reached"
    AVRO_ERR_EOF = 10,
    /// "Invalid Avro varint"
    AVRO_ERR_INVALID_VARINT = 11,
    /// "Value out of range for Avro int: N"
    AVRO_ERR_INT_OUT_OF_RANGE = 12,
    /// "Invalid value for bool"
    AVRO_ERR_INVALID_BOOL = 13,
    /// "Cannot have negative length: N"
    AVRO_ERR_NEGATIVE_LENGTH = 14,
    /// "Avro enum index N is out of range, the schema has M symbols"
    AVRO_ERR_ENUM_INDEX_OUT_OF_RANGE = 15,
    /// "Union index out of boundary: got N, the number of branches is M"
    AVRO_ERR_UNION_INDEX_OUT_OF_RANGE = 16,
    AVRO_ERR_INVALID_COUNT = 17,
    AVRO_ERR_DATA_TOO_DEEP = 18,
    AVRO_ERR_TOO_MANY_ITEMS = 19,
    AVRO_ERR_COUNT_EXCEEDS_PAYLOAD = 20,
    /// A plan range check failed.
    AVRO_ERR_VALUE_OUT_OF_RANGE = 21,
    AVRO_ERR_DECIMAL_TOO_WIDE = 22,
    /// "Invalid data file. Magic does not match: "
    AVRO_ERR_BAD_MAGIC = 30,
    /// "No schema in metadata"
    AVRO_ERR_NO_SCHEMA = 31,
    /// "Unknown codec in data file: X"
    AVRO_ERR_UNKNOWN_CODEC = 32,
    /// "Sync mismatch"
    AVRO_ERR_SYNC_MISMATCH = 33,
    /// "Invalid data file. Negative object count in block header: N"
    AVRO_ERR_NEGATIVE_OBJECT_COUNT = 34,
    /// "Invalid data file. Negative byte count in block header: N"
    AVRO_ERR_NEGATIVE_BYTE_COUNT = 35,
    /// "Invalid data file. Object count N in block header cannot fit in M payload byte(s) for this schema"
    AVRO_ERR_OBJECT_COUNT_EXCEEDS_PAYLOAD = 36,
    AVRO_ERR_HEADER_TOO_LARGE = 37,
    AVRO_ERR_BLOCK_TOO_LARGE = 38,
    AVRO_ERR_PLAN_INVALID = 40,
    AVRO_ERR_PROGRAM_TOO_LARGE = 41,
    /// A bug in the decoder, including a caught panic.
    AVRO_ERR_INTERNAL = 99,
} AvroErrorCode;

typedef struct AvroSchema AvroSchema;
typedef struct AvroProgram AvroProgram;
typedef struct AvroState AvroState;
typedef struct AvroOcfHeader AvroOcfHeader;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// `AvroNodeInfo::kind`.
typedef enum AvroKind
{
    AVRO_KIND_NULL = 0,
    AVRO_KIND_BOOLEAN = 1,
    AVRO_KIND_INT = 2,
    AVRO_KIND_LONG = 3,
    AVRO_KIND_FLOAT = 4,
    AVRO_KIND_DOUBLE = 5,
    AVRO_KIND_BYTES = 6,
    AVRO_KIND_STRING = 7,
    AVRO_KIND_RECORD = 8,
    AVRO_KIND_ENUM = 9,
    AVRO_KIND_ARRAY = 10,
    AVRO_KIND_MAP = 11,
    AVRO_KIND_UNION = 12,
    AVRO_KIND_FIXED = 13,
} AvroKind;

/// `AvroNodeInfo::logical`. Invalid or unknown logical types are `NONE`; the raw
/// `logicalType` attribute stays readable with `avro_schema_attr`.
typedef enum AvroLogical
{
    AVRO_LOGICAL_NONE = 0,
    AVRO_LOGICAL_DECIMAL = 1,
    AVRO_LOGICAL_BIG_DECIMAL = 2,
    AVRO_LOGICAL_UUID = 3,
    AVRO_LOGICAL_DATE = 4,
    AVRO_LOGICAL_TIME_MILLIS = 5,
    AVRO_LOGICAL_TIME_MICROS = 6,
    AVRO_LOGICAL_TIMESTAMP_MILLIS = 7,
    AVRO_LOGICAL_TIMESTAMP_MICROS = 8,
    AVRO_LOGICAL_TIMESTAMP_NANOS = 9,
    AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS = 10,
    AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS = 11,
    AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS = 12,
    AVRO_LOGICAL_DURATION = 13,
} AvroLogical;

/// Parses and compiles a schema. `max_depth` limits the nesting of the JSON and
/// of the schema (0: only the internal hard cap). On success `*out` is a new
/// schema; free it with `avro_schema_free`.
///
/// Named types (record, enum, fixed) referenced by name, including recursively,
/// are the same node as their definition: the schema is a graph. A node id
/// stays valid for the life of the schema.
int32_t avro_schema_parse(const uint8_t * json, size_t len, uint32_t max_depth, AvroSchema ** out, char * err, size_t err_cap);
void avro_schema_free(AvroSchema * schema);

/// The root node id.
uint32_t avro_schema_root(const AvroSchema * schema);
/// Number of nodes; valid ids are `[0, count)`.
uint32_t avro_schema_node_count(const AvroSchema * schema);

typedef struct AvroNodeInfo
{
    uint8_t kind; ///< `AvroKind`
    uint8_t logical; ///< `AvroLogical`
    uint8_t has_max_size; ///< 1 if `max_size` is meaningful
    uint8_t reserved;
    uint32_t precision; ///< decimal precision, 0 otherwise
    uint32_t scale;
    uint32_t fixed_size; ///< size of a `fixed`, 0 otherwise
    /// Fields of a record, branches of a union, 1 for an array or a map.
    uint32_t child_count;
    uint32_t symbol_count; ///< symbols of an enum
    /// Edges that point at this node (the root counts once). Above 1 means a
    /// shared, possibly recursive, named type.
    uint32_t in_degree;
    /// Full name (`namespace.name`) of a record, enum or fixed, else NULL.
    const char * name;
    size_t name_len;
    /// Where the simple name starts inside `name`.
    uint32_t simple_name_offset;
    uint32_t reserved2;
    /// A lower bound on the encoded size of one value of this node, in bytes;
    /// UINT64_MAX if no finite value exists. Recursive types are bounded by
    /// their cheapest non-recursive derivation.
    uint64_t min_size;
    /// An upper bound on the encoded size, valid if `has_max_size`.
    uint64_t max_size;
} AvroNodeInfo;

int32_t avro_schema_node(const AvroSchema * schema, uint32_t node, AvroNodeInfo * out, char * err, size_t err_cap);

/// The `i`-th child of `node`: the type of a record field (and its name), a
/// union branch, or the items/values of an array/map (`i == 0`). `name` and
/// `name_len` may be NULL; for non-records they are set to NULL / 0.
int32_t avro_schema_child(
    const AvroSchema * schema, uint32_t node, uint32_t i, uint32_t * child, const char ** name, size_t * name_len, char * err, size_t err_cap);

/// The `i`-th symbol of an enum.
int32_t avro_schema_symbol(const AvroSchema * schema, uint32_t node, uint32_t i, const char ** symbol, size_t * len, char * err, size_t err_cap);

/// `AvroAttr::kind`.
enum
{
    AVRO_ATTR_NULL = 0,
    AVRO_ATTR_BOOL = 1,
    AVRO_ATTR_INT = 2,
    AVRO_ATTR_FLOAT = 3,
    AVRO_ATTR_STRING = 4,
    /// An array or an object: present, but not interpreted.
    AVRO_ATTR_COMPOSITE = 5,
};

typedef struct AvroAttr
{
    uint8_t kind;
    uint8_t reserved[7];
    int64_t int_value; ///< the integer, or 0/1 for a boolean
    double float_value;
    const char * string;
    size_t string_len;
} AvroAttr;

/// Looks up an attribute that the schema compiler does not interpret, by key.
/// With `field < 0` it is an attribute of the schema node (the JSON object that
/// defined it: `element-id` of an array, `logicalType`, ...); otherwise of the
/// `field`-th field of the record `node` (Iceberg's `field-id`). Returns
/// `AVRO_NOT_FOUND` when the attribute does not exist. If the key is present
/// twice in the JSON the first one wins.
int32_t avro_schema_attr(
    const AvroSchema * schema, uint32_t node, int64_t field, const uint8_t * key, size_t key_len, AvroAttr * out, char * err, size_t err_cap);

/// Parsing Canonical Form of the schema (Avro specification), computed by the first call and then kept with the
/// schema (valid until `avro_schema_free`). It is not bounded by the size of the schema text: a named type that is
/// referenced `n` times is printed as its full name `n` times, so a compact schema can have a form that is
/// millions of times larger. Do not use it as an identity for a cache (see `avro_schema_source`) and use
/// `avro_schema_canonical_prefix` to put it in a message.
int32_t avro_schema_canonical(const AvroSchema * schema, const char ** out, size_t * len, char * err, size_t err_cap);

/// Copies at most `cap` bytes of the Parsing Canonical Form (cut at a character boundary) to `buf` and sets `*len`
/// to their number and `*truncated` to 1 if the form is longer. Nothing is generated beyond about `cap` bytes and
/// nothing is retained.
int32_t avro_schema_canonical_prefix(
    const AvroSchema * schema, char * buf, size_t cap, size_t * len, uint8_t * truncated, char * err, size_t err_cap);

/// The JSON text that `avro_schema_parse` was given, byte for byte (valid until `avro_schema_free`). Parsing is a
/// function of these bytes alone, so it is an exact identity of the schema that is as large as the input, which
/// the canonical form is not.
int32_t avro_schema_source(const AvroSchema * schema, const uint8_t ** out, size_t * len, char * err, size_t err_cap);

/// CRC-64-AVRO of `data` (for example of the canonical form). Forgeable: it may
/// pick a hash bucket, it must not identify a schema.
uint64_t avro_fingerprint64(const uint8_t * data, size_t len);

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

#define AVRO_NO_SINK 0xFFFFFFFFu

/// `AvroSinkDesc::kind`. `arg` is the element width or size where noted.
typedef enum AvroSinkKind
{
    AVRO_SINK_INT32 = 1, ///< i32; from `int`
    AVRO_SINK_INT64 = 2, ///< i64; from `long`, or `int` widened
    AVRO_SINK_FLOAT32 = 3, ///< from `float`
    AVRO_SINK_FLOAT64 = 4, ///< from `double`, or `float` widened
    AVRO_SINK_BOOL = 5, ///< one byte, 0 or 1
    /// Primary buffer: value bytes. Secondary buffer: one u64 end offset per
    /// value, continuing from `AvroSink::offset`. From `string` and `bytes`.
    AVRO_SINK_BYTES = 6,
    AVRO_SINK_FIXED_BYTES = 7, ///< `arg` bytes per value, equal to the fixed size
    /// Two's complement integer of `arg` (4, 8, 16, 32) bytes in host order, sign
    /// extended from the big-endian Avro value; from `bytes` or `fixed`.
    AVRO_SINK_DECIMAL = 8,
    AVRO_SINK_ENUM_INDEX = 9, ///< symbol index, `arg` (1, 2, 4) bytes
    AVRO_SINK_NULL_MAP = 10, ///< one byte per union value: 1 if the null branch was taken
    AVRO_SINK_UNION_TAG = 11, ///< branch index, `arg` (1, 2, 4) bytes
    AVRO_SINK_ARRAY_OFFSETS = 12, ///< u64 end offset (counting items) per array value
    AVRO_SINK_MAP_OFFSETS = 13, ///< u64 end offset (counting entries) per map value
} AvroSinkKind;

/// `AvroSinkDesc::flags` for `AVRO_SINK_BYTES`: append a zero byte after each
/// value and count it in the offset. Optional, for consumers whose string column
/// stores a terminator; leave it 0 (the default) for plain value bytes, which is
/// what current ClickHouse `ColumnString` holds.
#define AVRO_SINK_TERMINATED 1u

typedef struct AvroSinkDesc
{
    uint32_t kind; ///< `AvroSinkKind`
    uint32_t arg;
    uint32_t flags;
} AvroSinkDesc;

/// `AvroPlanNode::action`.
typedef enum AvroAction
{
    /// Skip the whole value (still validated). No children, no sinks.
    AVRO_ACTION_SKIP = 0,
    /// Decode a primitive, enum or fixed into `sink`. No children.
    AVRO_ACTION_VALUE = 1,
    /// A record: one child per field, in order. No sinks.
    AVRO_ACTION_RECORD = 2,
    /// An array: one child (the items). `sink` is an `AVRO_SINK_ARRAY_OFFSETS` sink or `AVRO_NO_SINK`.
    AVRO_ACTION_ARRAY = 3,
    /// A map: one child (the values). `sink` is an `AVRO_SINK_MAP_OFFSETS` sink
    /// or `AVRO_NO_SINK`; `sink2` is a `AVRO_SINK_BYTES` sink for the keys, or
    /// `AVRO_NO_SINK` to skip them.
    AVRO_ACTION_MAP = 4,
    /// A union: one child per branch. `sink` is an `AVRO_SINK_NULL_MAP` sink
    /// (then `arg` is the index of the null branch), an `AVRO_SINK_UNION_TAG` sink,
    /// or `AVRO_NO_SINK`.
    AVRO_ACTION_UNION = 5,
} AvroAction;

/// One entry of the plan. The plan is the pre-order walk of the schema tree as
/// the caller wants it decoded: an entry, then the entries of its children. Each
/// entry names the schema node it describes (checked against the schema walk).
/// A named type that is referenced again is walked again (with its own sinks)
/// unless the entry is `AVRO_ACTION_SKIP`. Every sink is used by exactly one
/// entry.
typedef struct AvroPlanNode
{
    uint32_t action; ///< `AvroAction`
    uint32_t schema_node;
    uint32_t sink;
    uint32_t sink2;
    uint32_t arg;
    uint32_t n_children;
    /// Non-zero: `AVRO_ACTION_VALUE` into an Int32 or Int64 sink fails with
    /// `AVRO_ERR_VALUE_OUT_OF_RANGE` outside `[check_min, check_max]`.
    uint32_t check;
    uint32_t reserved;
    int64_t check_min;
    int64_t check_max;
} AvroPlanNode;

/// Compiles a program. The program does not refer to `schema`, `sinks` or `plan`
/// afterwards. Shared and recursive named records that are skipped become
/// subroutines, so the program size is linear in the schema.
int32_t avro_program_compile(
    const AvroSchema * schema,
    const AvroSinkDesc * sinks,
    size_t n_sinks,
    const AvroPlanNode * plan,
    size_t n_plan,
    AvroProgram ** out,
    char * err,
    size_t err_cap);
void avro_program_free(AvroProgram * program);

/// Lower bound on the size of one row. 0 means rows can occupy no bytes: then
/// the end of a block is known only from its object count.
uint64_t avro_program_min_row_size(const AvroProgram * program);

/// The bytes that the program occupies (itself and its heap), for a cache that has a byte budget.
size_t avro_program_memory_size(const AvroProgram * program);

// ---------------------------------------------------------------------------
// State and decode
// ---------------------------------------------------------------------------

/// `max_depth` bounds nested arrays, maps and calls (0: the internal hard cap).
AvroState * avro_state_new(uint32_t max_depth);
void avro_state_free(AvroState * state);
/// Back to the start of a row: for a new block, and after an error.
void avro_state_reset(AvroState * state);
/// Streaming mode of the state (default off): see "Executor contract". Nonzero when the bytes passed to
/// `avro_decode` may be followed by more of the block. May be changed between calls, also while a row is
/// paused.
void avro_state_set_streaming(AvroState * state, int32_t streaming);

/// A caller-owned output buffer and its fill levels. Buffers need no alignment.
/// Never let two sinks overlap.
typedef struct AvroSink
{
    uint8_t * data; ///< primary buffer
    uint64_t data_size; ///< its size in bytes
    uint8_t * data2; ///< secondary buffer (`AVRO_SINK_BYTES` offsets), else NULL
    uint64_t data2_size;
    uint64_t len; ///< elements in `data` (bytes for `AVRO_SINK_BYTES`): in/out
    uint64_t len2; ///< elements in `data2`: in/out
    /// Running end offset (`AVRO_SINK_BYTES` and the offsets sinks): in/out.
    /// Set it to the last existing offset of the column before the first call.
    uint64_t offset;
} AvroSink;

enum
{
    AVRO_STATUS_BLOCK_DONE = 0,
    AVRO_STATUS_NEED_SPACE = 1,
    AVRO_STATUS_ROW_LIMIT = 2,
    /// Only in `AvroProgress` after a failed call (the function returned `-code`).
    AVRO_STATUS_ERROR = 3,
    /// Only in streaming mode: the input ended inside a row, and more bytes may follow.
    AVRO_STATUS_NEED_INPUT = 4,
};

/// A sink that lacks room, and what it needs: the capacities (in elements) that its primary and its
/// secondary buffer must reach, counted from its fill level.
typedef struct AvroNeed
{
    uint32_t sink;
    uint64_t cap;
    uint64_t cap2;
} AvroNeed;

typedef struct AvroProgress
{
    uint64_t rows_done; ///< rows completed (committed) by this call
    uint64_t consumed; ///< bytes of `input` the completed rows occupy: the next call starts here (0 when a row is paused for space; with `AVRO_STATUS_NEED_INPUT` also what a paused row is done with)
    uint32_t status; ///< `AVRO_STATUS_*`
    uint32_t need_sink; ///< with NEED_SPACE: the first sink that is full (`needs[0].sink`)
    uint64_t need_cap; ///< capacity (elements) its primary buffer must reach
    uint64_t need_cap2; ///< capacity (elements) its secondary buffer must reach
    uint64_t need_count; ///< with NEED_SPACE: the number of sinks that lack room; 0 with every other status
    /// with NEED_SPACE: the sinks that lack room, and what each needs (the first is `need_sink`). Points into the
    /// state and is valid until the next call with it; NULL with every other status.
    const AvroNeed * needs;
} AvroProgress;

/// Decodes up to `max_rows` rows of `input` with `program`. `sinks` has exactly
/// the program's number of sinks. The call is transactional per row: on
/// `AVRO_STATUS_ROW_LIMIT`, `AVRO_STATUS_BLOCK_DONE`, on errors (the return value
/// is `-code`, `*out` is set with `AVRO_STATUS_ERROR` and the committed
/// `rows_done`/`consumed`; the failing row is rolled back) and on a
/// `AVRO_STATUS_NEED_SPACE` with `rows_done > 0` (the row that did not fit is
/// rolled back and starts the next call), `rows_done`/`consumed` and the fill
/// levels written back into the sinks cover exactly the rows completed by this
/// call, and the state is at a row boundary. On `AVRO_STATUS_NEED_SPACE` with
/// `rows_done == 0` the first row is paused: the state keeps the sinks with the
/// fill levels that include what the row wrote so far, and the next call with the
/// same input, state and `sinks` array continues it. Only the fill levels of the
/// sinks of `needs` are written back to the array by that call; the others are
/// written back by the call that completes the row (or fails). Between the calls
/// change only the buffers of the sinks of `needs` (`data`, `data_size`, `data2`,
/// `data2_size`); the next call converts those sinks again and no other.
/// Bytes in the buffers past the fill levels may hold stale data. Rolling back
/// replays the completed rows of the call once, only on a NEED_SPACE with
/// `rows_done > 0` and on errors. A call that continues a row paused with `AVRO_STATUS_NEED_INPUT` returns
/// after completing that row, so its `rows_done` can be 1 while `max_rows` is more. See "Executor contract"
/// above.
int32_t avro_decode(
    AvroState * state,
    const AvroProgram * program,
    const uint8_t * input,
    size_t input_len,
    uint64_t max_rows,
    AvroSink * sinks,
    size_t n_sinks,
    AvroProgress * out,
    char * err,
    size_t err_cap);

/// Returns 1 and sets `*bad_index` to the index of the first Int32 in `data`
/// (host byte order, `n_values` of them) outside `[min, max]`, 0 if there is
/// none. For the Date/Date32/DateTime range checks of decoded columns.
int32_t avro_first_out_of_range_i32(const uint8_t * data, size_t n_values, int32_t min, int32_t max, uint64_t * bad_index);

// ---------------------------------------------------------------------------
// Object Container Files
// ---------------------------------------------------------------------------

/// Codec numbers of `avro_ocf_header_codec` and `avro_ocf_block_header`.
enum
{
    AVRO_CODEC_NULL = 0,
    AVRO_CODEC_DEFLATE = 1, ///< raw deflate, no zlib header
    AVRO_CODEC_SNAPPY = 2, ///< the payload ends with a 4 byte big-endian CRC32 of the plain data
    AVRO_CODEC_ZSTANDARD = 3,
    AVRO_CODEC_BZIP2 = 4,
    AVRO_CODEC_XZ = 5,
};

/// Parses the file header at the start of the window `buf[0, len)`. Metadata
/// keys that repeat keep the last value. Returns `AVRO_OK` (`*out` is a new
/// header, `*consumed` its size), `AVRO_NEED_MORE` (`*needed`: the window must
/// be at least that long) or an error. With `eof != 0`, a window that ends too
/// early is `AVRO_ERR_EOF` ("EOF reached"). `max_header_bytes == 0` means 16 MiB.
/// Nothing is consumed on `AVRO_NEED_MORE`: call again with a longer window.
int32_t avro_ocf_header_parse(
    const uint8_t * buf,
    size_t len,
    int32_t eof,
    uint64_t max_header_bytes,
    AvroOcfHeader ** out,
    uint64_t * consumed,
    uint64_t * needed,
    char * err,
    size_t err_cap);
void avro_ocf_header_free(AvroOcfHeader * header);

/// Unknown codecs are rejected by `avro_ocf_header_parse` ("Unknown codec in
/// data file: X"); a missing `avro.codec` means null.
uint32_t avro_ocf_header_codec(const AvroOcfHeader * header);
/// Copies the 16 byte sync marker to `out`.
void avro_ocf_header_sync(const AvroOcfHeader * header, uint8_t * out);
/// The `avro.schema` JSON text, for `avro_schema_parse`.
int32_t avro_ocf_header_schema(const AvroOcfHeader * header, const uint8_t ** out, size_t * len, char * err, size_t err_cap);
/// Looks up a metadata value; `AVRO_NOT_FOUND` if the key is absent.
int32_t avro_ocf_header_meta(
    const AvroOcfHeader * header, const uint8_t * key, size_t key_len, const uint8_t ** value, size_t * value_len, char * err, size_t err_cap);
uint64_t avro_ocf_header_meta_count(const AvroOcfHeader * header);
/// The `i`-th metadata entry, in order of first appearance.
int32_t avro_ocf_header_meta_at(
    const AvroOcfHeader * header,
    uint64_t i,
    const uint8_t ** key,
    size_t * key_len,
    const uint8_t ** value,
    size_t * value_len,
    char * err,
    size_t err_cap);

typedef struct AvroBlockHeader
{
    uint64_t object_count;
    uint64_t byte_count; ///< payload bytes, after the block header
    uint64_t header_len; ///< the payload starts this many bytes into the window
} AvroBlockHeader;

/// Parses a block header at the start of the window, which begins right after
/// the header or the previous block's sync marker. Returns `AVRO_OK`,
/// `AVRO_NEED_MORE`, `AVRO_END` (window empty and `eof`) or an error. Rejects
/// negative counts. For the null codec the object count is checked against
/// `byte_count` and `min_record_size` (the root's `AvroNodeInfo::min_size`, 0 if
/// unknown); for compressed codecs call `avro_ocf_check_object_count` with the
/// decompressed size. Rows that occupy no bytes are capped per block.
/// `max_block_bytes == 0` means no limit.
int32_t avro_ocf_block_header(
    const uint8_t * buf,
    size_t len,
    int32_t eof,
    uint32_t codec,
    uint64_t min_record_size,
    uint64_t max_block_bytes,
    AvroBlockHeader * out,
    uint64_t * needed,
    char * err,
    size_t err_cap);

/// Checks the 16 bytes that follow every block payload against the header's sync marker.
/// Returns `AVRO_OK`, `AVRO_NEED_MORE` (`*needed` is 16) or an error ("Sync mismatch").
int32_t avro_ocf_check_sync(const uint8_t * buf, size_t len, int32_t eof, const uint8_t * sync, uint64_t * needed, char * err, size_t err_cap);

/// Rejects `object_count` rows that cannot fit `payload_len` plain bytes when each
/// takes at least `min_record_size` (see `avro_ocf_block_header`).
int32_t avro_ocf_check_object_count(uint64_t object_count, uint64_t payload_len, uint64_t min_record_size, char * err, size_t err_cap);

#ifdef __cplusplus
} // extern "C"
#endif
