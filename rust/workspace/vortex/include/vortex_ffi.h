#pragma once

#include <stdint.h>

/// C bindings for reading and writing Vortex files, implemented in Rust on top of the `vortex`
/// crate; the other half lives in `rust/workspace/vortex/src/lib.rs`. Arrays cross the boundary as
/// Arrow C Data Interface structs, and IO is delegated back through callbacks.
///
/// THIS FILE IS GENERATED FROM `src/lib.rs` BY `build.rs`. DO NOT EDIT IT BY HAND: change the Rust
/// side, including its doc comments, and regenerate with
/// `VORTEX_FFI_HEADER_UPDATE=1 cargo check -p _ch_rust_vortex`.
///
/// Ownership:
///   - an Arrow struct passed into a function is consumed by it;
///   - an Arrow struct written to an out-parameter belongs to the caller, who has to call its
///     `release` - importing it with `arrow::ImportRecordBatch` or `arrow::ImportSchema` does that;
///   - the Arrow struct passed to the scan's `on_chunk` is only borrowed for the duration of the
///     call: the callback has to consume it before returning, by moving out of it (importing it
///     with `arrow::ImportRecordBatch` does that) or by calling its `release`. The pointer must
///     not be kept, and the struct must not be released after the callback has returned;
///   - an error message has to be freed with `vortex_ffi_free_string`.
///
/// Every `vortex_ffi_expr_*` builder returns nullptr for input it cannot use, borrows rather than
/// consumes its arguments, and returns a handle that has to be freed with `vortex_ffi_expr_free`.
///
/// Nothing here owns a thread. An `FFI_VortexRuntime` is two queues of pending work plus a way to
/// report that something became runnable; the caller decides who runs it, when, and on how many
/// threads, and may send the two queues to different thread pools. Without a notification callback
/// the runtime only advances on the thread already inside a call, which is enough for opening a
/// file and for writing one. A scan does not wait to be asked: it pushes its results to callbacks.

/// The Arrow C Data Interface definitions, see
/// https://arrow.apache.org/docs/format/CDataInterface.html
/// They are compatible with the ones in `arrow/c/abi.h`.
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema
{
    const char * format;
    const char * name;
    const char * metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema ** children;
    struct ArrowSchema * dictionary;
    void (*release)(struct ArrowSchema *);
    void * private_data;
};

struct ArrowArray
{
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void ** buffers;
    struct ArrowArray ** children;
    struct ArrowArray * dictionary;
    void (*release)(struct ArrowArray *);
    void * private_data;
};

#endif // ARROW_C_DATA_INTERFACE


/// The queue a task waits in.
enum class FFI_VortexTaskQueue : int32_t
{
    /// Decoding, filtering and exporting to Arrow: work that needs a core.
    CPU = 0,
    /// Work that calls the read callback and blocks until it returns.
    IO = 1,
};

/// The type of a literal. It has to be exactly the type of the file column it is compared with:
/// Vortex requires both sides of a comparison to have the same type.
enum class FFI_VortexPrimitiveType : int32_t
{
    I8 = 0,
    I16 = 1,
    I32 = 2,
    I64 = 3,
    U8 = 4,
    U16 = 5,
    U32 = 6,
    U64 = 7,
    F32 = 8,
    F64 = 9,
};

/// The unit of a temporal literal; the values mirror the discriminants of the Vortex `TimeUnit`.
enum class FFI_VortexTimeUnit : int32_t
{
    Nanoseconds = 0,
    Microseconds = 1,
    Milliseconds = 2,
    Seconds = 3,
    Days = 4,
};

/// The operator of `vortex_ffi_expr_compare`.
enum class FFI_VortexComparisonOperator : int32_t
{
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
};

struct FFI_VortexExpression;

struct FFI_VortexReader;

struct FFI_VortexRuntime;

struct FFI_VortexScan;

struct FFI_VortexWriter;

/// Reports that a task of this queue became runnable. It must not call back into the library:
/// schedule `vortex_ffi_runtime_run` somewhere and return. Can be called on any thread, and
/// synchronously from inside any call that woke a task.
/// Optional: a null callback gives a runtime that only advances inside FFI calls. The `Option` is
/// part of the alias so that the generated header sees a plain, nullable function pointer.
using FFI_VortexTaskReadyCallback = void(*)(void *context, FFI_VortexTaskQueue queue);

/// Reads `length` bytes at `offset` into `out`. Returns zero on success. Called from the threads
/// that run the IO queue, concurrently when `io_concurrency` is greater than 1.
using FFI_VortexReadCallback = int32_t(*)(void *context, uint64_t offset, uint64_t length, uint8_t *out);

/// Zero-initialize for one read at a time and no merging.
struct FFI_VortexReaderOptions
{
    /// How many reads may be outstanding, 0 and 1 both meaning one. Above that the read callback
    /// has to tolerate being on several threads at once.
    uint32_t io_concurrency;
    /// Two segment reads are merged into one call when no more than `coalesce_max_gap_bytes`
    /// separate them and the result stays under `coalesce_max_read_bytes`. Both zero disables
    /// merging.
    uint64_t coalesce_max_gap_bytes;
    uint64_t coalesce_max_read_bytes;
};

/// Nothing here is required: zero-initialize to read every row of every column.
struct FFI_VortexScanOptions
{
    /// The top-level columns to read, in this order. Null means all of them.
    const char *const *columns;
    uint64_t num_columns;
    /// Only the rows matching it are returned, and the scan skips the statistics zones it rules
    /// out. Null means no filter.
    const FFI_VortexExpression *filter;
    /// The row range `[row_range_begin, row_range_end)`. Both zero means the whole file.
    uint64_t row_range_begin;
    uint64_t row_range_end;
    const uint64_t *row_selection_begin;
    /// Zero means the whole file.
    uint64_t row_selection_len;
    /// Prepends a `row_idx` column to the output.
    bool row_index_column;
    /// The number of splits that may be in flight at once: being read, being decoded, or already
    /// handed over and not yet released. 0 selects the default. This is what keeps the scan from
    /// running ahead of the caller; the reads underneath are bounded separately by
    /// `io_concurrency` and `coalesce_max_read_bytes`.
    uint32_t max_splits_in_flight;
};

/// The callbacks a scan reports to. Both run on the caller's own threads, possibly several at a
/// time. Neither of them may call back into the library. `on_chunk` has no handle to call with in
/// the first place - the scan is only handed over once `vortex_ffi_scan_create` returns, and chunks
/// can arrive before that - so it stops the scan by returning non-zero instead. `on_finish` runs
/// inside the scan's own driver task, which `vortex_ffi_scan_cancel` would drop from under itself.
/// `vortex_ffi_scan_cancel` and `vortex_ffi_scan_release` are for the other threads of the caller.
struct FFI_VortexScanCallbacks
{
    void *context;
    /// Delivers one chunk: an Arrow struct array in the scan's schema, together with the position
    /// of its split in the file. The array is borrowed for the duration of the call - the callback
    /// takes the data out of it (or releases it) before returning, and must not keep the pointer.
    /// A null array means the split matched no rows; it is still reported so that the caller can
    /// restore the file order. Returning non-zero stops the scan; it is the only way `on_chunk` has
    /// to stop it, and it surfaces from `on_finish` as an error.
    int32_t (*on_chunk)(void *context, ArrowArray *array, uint64_t split_index);
    /// Reports the end of the scan, exactly once: null if every split was delivered, otherwise
    /// a message that is only valid for the duration of the call. Every other outcome - a failed
    /// split, a non-zero return from `on_chunk`, a panic in the driver - is reported here; the one
    /// scan that reports nothing is one the caller cancelled, and a `vortex_ffi_scan_cancel` from
    /// another thread can land at any point, so a cancelling caller must not wait for this. After a
    /// failure a split task already in flight can still reach `on_chunk`, so the context has to
    /// outlive the caller's last pass over the queues.
    void (*on_finish)(void *context, const char *error);
};

/// Consumes `length` bytes of the file being written. Returns zero on success.
using FFI_VortexWriteCallback = int32_t(*)(void *context, const uint8_t *data, uint64_t length);

extern "C" {

/// Creates a runtime. A null `notify`, together with `context`, gives one that only advances
/// inside FFI calls. It has to outlive every reader, scan and writer created on it.
FFI_VortexRuntime *vortex_ffi_runtime_new(void *context, FFI_VortexTaskReadyCallback notify);

/// Runs up to `max_tasks` runnable tasks of the queue, 0 meaning no limit, and returns how many
/// were run. Returns -1 if a panic was caught; no panic ever crosses the boundary.
///
/// Any number of threads may run the same queue at once. A task may queue further tasks, on either
/// queue, which is reported through the notification callback.
int64_t vortex_ffi_runtime_run(const FFI_VortexRuntime *runtime, FFI_VortexTaskQueue queue, uint32_t max_tasks, char **error);

/// Returns the number of tasks waiting in the given queue. Safe to call from any thread.
uint64_t vortex_ffi_runtime_pending(const FFI_VortexRuntime *runtime, FFI_VortexTaskQueue queue);

/// Frees the runtime. Everything created on it has to be freed first, and no thread may be inside
/// `vortex_ffi_runtime_run` on it.
void vortex_ffi_runtime_free(FFI_VortexRuntime *runtime);

/// Opens a file for reading. It is accessed through `read` with the given `context`; `file_size`
/// has to be the exact size of the file, and `options` may be null. Reading the footer happens on
/// the calling thread, so the runtime does not have to be driven yet. Returns null on failure.
FFI_VortexReader *vortex_ffi_reader_open(const FFI_VortexRuntime *runtime,
                                         void *context,
                                         FFI_VortexReadCallback read,
                                         uint64_t file_size,
                                         const FFI_VortexReaderOptions *options,
                                         char **error);

/// Returns the total number of rows in the file.
uint64_t vortex_ffi_reader_row_count(const FFI_VortexReader *reader);

/// Exports the file schema into `out_schema`, which the caller then owns and has to release.
/// Returns zero on success.
int32_t vortex_ffi_reader_schema(const FFI_VortexReader *reader, ArrowSchema *out_schema, char **error);

/// Frees the reader. Every scan created on it has to be freed first.
void vortex_ffi_reader_free(FFI_VortexReader *reader);

/// Creates a scan and starts it. Optimizing the expression and computing the splits happens here,
/// on the calling thread; from then on the scan is a driver task that spawns one task per split, as
/// far ahead as the `max_splits_in_flight` permits allow. A split task reads, decodes and exports
/// its data, then calls `on_chunk` on the thread it is running on, and the permit it took is only
/// returned when the caller releases that chunk - so the caller sets the pace.
///
/// A split that matched no rows returns its own permit, as there is nothing for the caller to
/// release. The driver joins finished tasks even while it is waiting for a permit: with every
/// permit held by the caller, a split that failed would otherwise never be noticed, having
/// delivered nothing.
///
/// The reader and the callbacks' context both have to outlive the scan. `filter` is borrowed, not
/// consumed. Only one scan of a reader may be alive at a time - everything that bounds the reads,
/// `io_concurrency` above all, is set up per scan - so this fails while another scan of the same
/// reader has not been freed. Returns null on failure.
FFI_VortexScan *vortex_ffi_scan_create(const FFI_VortexReader *reader,
                                       const FFI_VortexScanOptions *options,
                                       const FFI_VortexScanCallbacks *callbacks,
                                       char **error);

/// Exports the schema of the scan's chunks into `out_schema`, which the caller then owns and has
/// to release. Returns zero on success.
int32_t vortex_ffi_scan_schema(const FFI_VortexScan *scan, ArrowSchema *out_schema, char **error);

/// Returns the capacity taken by `count` chunks the caller has finished with, letting the scan
/// read that many splits further ahead. Safe from any thread and a no-op once the scan has ended.
/// Must not be called from inside `on_chunk`.
void vortex_ffi_scan_release(const FFI_VortexScan *scan, uint64_t count);

/// Cancels the scan; safe from any thread. Pending tasks are dropped and no callback happens after
/// this returns, except from a task that was already running - so stop driving the queues before
/// releasing the callbacks' context.
void vortex_ffi_scan_cancel(const FFI_VortexScan *scan);

/// Frees the scan. The queues must no longer be driven: no task of it may still be running.
void vortex_ffi_scan_free(FFI_VortexScan *scan);

/// Creates an expression referencing the top-level column `name`.
FFI_VortexExpression *vortex_ffi_expr_column(const char *name);

/// Creates a signed integer literal of the given type. Returns null if the value does not fit.
FFI_VortexExpression *vortex_ffi_expr_literal_int(FFI_VortexPrimitiveType ptype, int64_t value);

/// Creates an unsigned integer literal of the given type. Returns null if the value does not fit.
FFI_VortexExpression *vortex_ffi_expr_literal_uint(FFI_VortexPrimitiveType ptype, uint64_t value);

/// Creates a floating-point literal of the given type. An `F32` value has to be exactly
/// representable as `f32`; a rounded bound would change which rows the comparison matches.
FFI_VortexExpression *vortex_ffi_expr_literal_float(FFI_VortexPrimitiveType ptype, double value);

/// Creates a boolean literal.
FFI_VortexExpression *vortex_ffi_expr_literal_bool(bool value);

/// Creates a string literal. `is_utf8` selects a `Utf8` literal, whose bytes have to be valid
/// UTF-8, or a `Binary` one. A null `data` is only accepted for length 0.
FFI_VortexExpression *vortex_ffi_expr_literal_string(const uint8_t *data, uint64_t length, bool is_utf8);

/// Creates a `vortex.date` literal: days or milliseconds since the Unix epoch. The only units a
/// date supports are `Days`, whose value has to fit `i32`, and `Milliseconds`. Returns null
/// otherwise.
FFI_VortexExpression *vortex_ffi_expr_literal_date(FFI_VortexTimeUnit unit, int64_t value);

/// Creates a `vortex.timestamp` literal: ticks of `unit` since the Unix epoch, with `timezone`
/// naming the zone or null for a zone-less timestamp. `Days` is not a timestamp unit. The unit and
/// the zone have to be exactly the file column's: Vortex only compares timestamps whose metadata
/// is identical.
FFI_VortexExpression *vortex_ffi_expr_literal_timestamp(FFI_VortexTimeUnit unit, const char *timezone, int64_t value);

/// Creates a comparison `lhs op rhs`. A comparison with a null value yields null, which the scan
/// treats as a row that does not match.
FFI_VortexExpression *vortex_ffi_expr_compare(FFI_VortexComparisonOperator comparison,
                                              const FFI_VortexExpression *lhs,
                                              const FFI_VortexExpression *rhs);

/// Creates a Kleene, three-valued AND of two boolean expressions.
FFI_VortexExpression *vortex_ffi_expr_and(const FFI_VortexExpression *lhs, const FFI_VortexExpression *rhs);

/// Creates a Kleene, three-valued OR of two boolean expressions.
FFI_VortexExpression *vortex_ffi_expr_or(const FFI_VortexExpression *lhs, const FFI_VortexExpression *rhs);

/// Creates a logical NOT of a boolean expression. NOT of a null is null.
FFI_VortexExpression *vortex_ffi_expr_not(const FFI_VortexExpression *child);

/// Creates an expression that is true for the rows where the child expression is null.
FFI_VortexExpression *vortex_ffi_expr_is_null(const FFI_VortexExpression *child);

/// Renders the expression the way the library prints it, for logs and error messages. The string
/// has to be freed with `vortex_ffi_free_string`.
char *vortex_ffi_expr_display(const FFI_VortexExpression *expr);

/// Frees an expression handle.
void vortex_ffi_expr_free(FFI_VortexExpression *expr);

/// Creates a writer for a file with the given schema, which it consumes. The bytes are sent to
/// `write` with the given `context`. It drives a runtime of its own on the calling thread, so
/// writing needs no threads from the caller. Returns null on failure.
FFI_VortexWriter *vortex_ffi_writer_create(void *context, FFI_VortexWriteCallback write, ArrowSchema *schema, char **error);

/// Appends one record batch, which it consumes, in the schema the writer was created with.
/// Returns zero on success.
int32_t vortex_ffi_writer_write(FFI_VortexWriter *writer, ArrowArray *array, ArrowSchema *schema, char **error);

/// Flushes the remaining data and writes the file footer. Must be called exactly once, before
/// freeing the writer. Returns zero on success.
int32_t vortex_ffi_writer_finish(FFI_VortexWriter *writer, char **error);

/// Frees the writer. Without a preceding `vortex_ffi_writer_finish` the file is left incomplete.
void vortex_ffi_writer_free(FFI_VortexWriter *writer);

/// Frees a string returned by this library, such as an error message.
void vortex_ffi_free_string(char *string);

}  // extern "C"
