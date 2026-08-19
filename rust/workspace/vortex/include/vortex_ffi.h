#pragma once

#include <stdint.h>

/// C bindings for reading and writing Vortex files (https://github.com/vortex-data/vortex),
/// implemented in Rust on top of the `vortex` crate. See `rust/workspace/vortex/src/lib.rs`.
///
/// Data crosses the boundary through the Arrow C Data Interface (`struct ArrowArray` and
/// `struct ArrowSchema`), and IO is delegated to the caller through callbacks.
///
/// Ownership conventions:
///   - Arrow structs passed into a function are consumed (moved) by the callee.
///   - Arrow structs returned through out-parameters, and the one passed to the scan consumer's
///     `on_chunk`, are owned by the caller, which must release them through their `release`
///     callback (e.g. by importing them with `arrow::ImportRecordBatch` / `arrow::ImportSchema`).
///   - Error messages are heap-allocated and must be freed with `vortex_ffi_free_string`.
///
/// Threading: the library owns no threads and no executor. A `VortexFFIRuntime` is a pair of task
/// queues (CPU and IO) plus a notification callback: whenever a task becomes runnable, the callback
/// tells the caller, which then runs tasks by calling `vortex_ffi_runtime_run` from as many of its
/// own threads as it wants, and may send the two queues to different thread pools. A runtime
/// created without a notification callback runs its tasks only on the threads that are inside FFI
/// calls on it, which is enough for opening a file and for writing.
///
/// A scan pushes its results: the task that produced a chunk calls the consumer's `on_chunk` on
/// whichever thread ran it, so the caller converts chunks in parallel too, and the end of the scan
/// (or its first error) is reported to `on_finish`.

/// The standard Arrow C Data Interface definitions, see
/// https://arrow.apache.org/docs/format/CDataInterface.html
/// (compatible with the definitions in `arrow/c/abi.h`).
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

extern "C" {

struct VortexFFIRuntime;
struct VortexFFIReader;
struct VortexFFIScan;
struct VortexFFIWriter;
struct VortexFFIExpression;

/// The queue a task belongs to.
enum class VortexFFIQueue : int32_t
{
    /// Decoding, filtering, Arrow export: tasks that use the CPU.
    Cpu = 0,
    /// Tasks that call the read callback.
    IO = 1,
};

/// Called when a task becomes runnable on the given queue of the runtime. It must not call back
/// into the library; the caller is expected to schedule `vortex_ffi_runtime_run` somewhere and
/// return. May be called from any thread, including from inside `vortex_ffi_runtime_run` itself.
using VortexFFINotifyCallback = void (*)(void * context, VortexFFIQueue queue);

/// Creates a runtime. `notify` (which may be nullptr, together with `context`) is called whenever a
/// task becomes runnable; the caller is then expected to call `vortex_ffi_runtime_run` for that
/// queue from one of its threads. A runtime without a callback only ever runs tasks on the threads
/// that are inside FFI calls on it. The runtime must outlive everything created on it.
VortexFFIRuntime * vortex_ffi_runtime_new(void * context, VortexFFINotifyCallback notify);

/// Runs at most `max_tasks` runnable tasks of the given queue (0 means no limit) and returns how
/// many were run, or -1 if a task panicked (the panic does not cross the FFI boundary).
///
/// Thread-safe: any number of threads may run the same queue at once. A task may queue more tasks,
/// including on the other queue, which is reported through the notification callback.
int64_t vortex_ffi_runtime_run(const VortexFFIRuntime * runtime, VortexFFIQueue queue, uint32_t max_tasks, char ** error);

/// Returns the number of tasks waiting in the given queue. Thread-safe.
uint64_t vortex_ffi_runtime_pending(const VortexFFIRuntime * runtime, VortexFFIQueue queue);

/// Frees the runtime. Everything created on it must be freed first, and no thread may be inside
/// `vortex_ffi_runtime_run` on it.
void vortex_ffi_runtime_free(VortexFFIRuntime * runtime);

/// Reads `length` bytes at `offset` into `out`. Returns 0 on success, non-zero on failure.
/// Called from the threads that run the IO queue of the runtime; concurrently from several of them
/// if the reader was opened with `io_concurrency > 1`.
using VortexFFIReadCallback = int32_t (*)(void * context, uint64_t offset, uint64_t length, uint8_t * out);

/// Consumes `length` bytes from `data`. Returns 0 on success, non-zero on failure.
using VortexFFIWriteCallback = int32_t (*)(void * context, const uint8_t * data, uint64_t length);

/// The primitive type of a literal built through `vortex_ffi_expr_literal_*`. Must match the
/// exact type of the file column it is compared with: Vortex comparisons require both sides to
/// have the same type.
enum class VortexFFIPType : int32_t
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

/// A comparison operator for `vortex_ffi_expr_compare`.
enum class VortexFFIComparison : int32_t
{
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
};

/// Options of `vortex_ffi_reader_open`. A zero-initialized struct means: one read at a time and no
/// coalescing.
struct VortexFFIReaderOptions
{
    /// The maximum number of reads the library may have in flight at once (0 or 1 = one). The read
    /// callback must be thread-safe if it is greater than 1.
    uint32_t io_concurrency;
    /// Nearby segment reads are merged into one callback invocation when the gap between them is at
    /// most `coalesce_distance` bytes and the merged read is at most `coalesce_max_size` bytes. Both
    /// zero disables coalescing: one callback per segment.
    uint64_t coalesce_distance;
    uint64_t coalesce_max_size;
};

/// Opens a Vortex file for reading on the given runtime. The file is accessed through `read` with
/// the given opaque `context`; `file_size` must be the exact file size in bytes; `options` may be
/// nullptr (the defaults). Reading the footer happens on the calling thread, so the caller does not
/// have to be running the runtime yet. Returns nullptr on error.
VortexFFIReader * vortex_ffi_reader_open(
    const VortexFFIRuntime * runtime,
    void * context,
    VortexFFIReadCallback read,
    uint64_t file_size,
    const VortexFFIReaderOptions * options,
    char ** error);

/// Returns the total number of rows in the file.
uint64_t vortex_ffi_reader_row_count(const VortexFFIReader * reader);

/// Exports the file schema into `out_schema`. Returns 0 on success.
int32_t vortex_ffi_reader_schema(const VortexFFIReader * reader, struct ArrowSchema * out_schema, char ** error);

void vortex_ffi_reader_free(VortexFFIReader * reader);

/// Options of a scan. All fields are optional: a zero-initialized struct scans all rows of all
/// columns.
struct VortexFFIScanOptions
{
    /// The names of the top-level columns to read, in the given order. nullptr means all columns.
    const char * const * columns;
    uint64_t num_columns;
    /// If not nullptr, only the rows matching the filter expression are returned; selective
    /// queries decode only the matching rows. Whole segments are not yet pruned by statistics.
    const VortexFFIExpression * filter;
    /// The row range `[row_range_begin, row_range_end)` to scan. Both zero means the whole file.
    uint64_t row_range_begin;
    uint64_t row_range_end;
    /// The maximum number of chunks that may be in flight at once: being read, decoded, or already
    /// handed to `on_chunk` and not yet released with `vortex_ffi_scan_release` (0 = default).
    /// This bounds both the memory the scan holds and the amount of IO lookahead.
    uint32_t in_flight;
};

/// The callbacks a scan reports to. They are called on the threads that run the scan's tasks,
/// concurrently, and must not call back into the library (except `vortex_ffi_scan_release`, which
/// must not be called from `on_chunk` itself).
struct VortexFFIScanConsumer
{
    void * context;
    /// Receives one chunk of the scan: an Arrow struct array in the scan schema, whose ownership
    /// passes to the consumer, and the 0-based index of its row split in file order. A null array
    /// means the split matched no rows (reported so that the consumer can restore the file order).
    /// Returns 0 on success; a non-zero return stops the scan and is reported as an error to
    /// `on_finish`.
    int32_t (*on_chunk)(void * context, struct ArrowArray * array, uint64_t split_index);
    /// Called exactly once when the scan ends: with nullptr when all splits were delivered, or with
    /// an error message (valid only during the call) when the scan failed. Not called when the scan
    /// is cancelled with `vortex_ffi_scan_cancel`.
    void (*on_finish)(void * context, const char * error);
};

/// Creates a scan over the file and starts it: the split tasks are spawned onto the reader's
/// runtime as capacity allows, and every chunk they produce is handed to `consumer.on_chunk` on the
/// thread that ran the task. The end of the scan (or its first error) is reported to
/// `consumer.on_finish`.
///
/// Expression optimization and split computation happen here, on the calling thread. The reader and
/// the consumer's context must stay alive for the whole lifetime of the scan; `filter` is not
/// consumed. Returns nullptr on error.
VortexFFIScan * vortex_ffi_scan_create(
    const VortexFFIReader * reader, const VortexFFIScanOptions * options, const VortexFFIScanConsumer * consumer, char ** error);

/// Exports the schema of the chunks produced by the scan into `out_schema`. Returns 0 on success.
int32_t vortex_ffi_scan_schema(const VortexFFIScan * scan, struct ArrowSchema * out_schema, char ** error);

/// Returns the capacity of `count` chunks that were delivered to `on_chunk` and are not needed by
/// the caller anymore, letting the scan read that many splits further ahead. Thread-safe, and safe
/// to call after the scan has finished or was cancelled. Must not be called from `on_chunk`.
void vortex_ffi_scan_release(const VortexFFIScan * scan, uint64_t count);

/// Cancels the scan. Thread-safe. The pending tasks are dropped; `on_chunk` and `on_finish` are not
/// called anymore once this returns, except from a task that is running at that moment, so the
/// caller must stop running the runtime's queues before it frees the consumer's context.
void vortex_ffi_scan_cancel(const VortexFFIScan * scan);

/// Frees the scan. The caller must have stopped running the runtime's queues (no task of this scan
/// may be running).
void vortex_ffi_scan_free(VortexFFIScan * scan);

/// Filter expression builders. All of them return nullptr on invalid input (an unrepresentable
/// literal value, invalid UTF-8, or a nullptr argument), and none of them consume their
/// arguments: every returned handle must be freed with `vortex_ffi_expr_free`.

/// Creates an expression referencing the top-level column `name`.
VortexFFIExpression * vortex_ffi_expr_column(const char * name);

/// Creates a signed integer literal of the given type. Returns nullptr if the value does not fit.
VortexFFIExpression * vortex_ffi_expr_literal_int(VortexFFIPType ptype, int64_t value);

/// Creates an unsigned integer literal of the given type. Returns nullptr if the value does not fit.
VortexFFIExpression * vortex_ffi_expr_literal_uint(VortexFFIPType ptype, uint64_t value);

/// Creates a floating-point literal of the given type. For `F32` the value must be exactly
/// representable as `float`, otherwise nullptr is returned.
VortexFFIExpression * vortex_ffi_expr_literal_float(VortexFFIPType ptype, double value);

/// Creates a boolean literal.
VortexFFIExpression * vortex_ffi_expr_literal_bool(bool value);

/// Creates a string literal: `Utf8` if `is_utf8` is true (the bytes must be valid UTF-8,
/// otherwise nullptr is returned), `Binary` otherwise.
VortexFFIExpression * vortex_ffi_expr_literal_string(const uint8_t * data, uint64_t length, bool is_utf8);

/// Creates a comparison `lhs op rhs`. A comparison with a null value yields null, which the
/// scan treats as "row does not match".
VortexFFIExpression * vortex_ffi_expr_compare(
    VortexFFIComparison comparison, const VortexFFIExpression * lhs, const VortexFFIExpression * rhs);

/// Creates a Kleene (three-valued) AND of two boolean expressions.
VortexFFIExpression * vortex_ffi_expr_and(const VortexFFIExpression * lhs, const VortexFFIExpression * rhs);

/// Creates a Kleene (three-valued) OR of two boolean expressions.
VortexFFIExpression * vortex_ffi_expr_or(const VortexFFIExpression * lhs, const VortexFFIExpression * rhs);

/// Creates a logical NOT of a boolean expression (NOT of null is null).
VortexFFIExpression * vortex_ffi_expr_not(const VortexFFIExpression * child);

/// Creates an expression that is true where the child expression is null.
VortexFFIExpression * vortex_ffi_expr_is_null(const VortexFFIExpression * child);

void vortex_ffi_expr_free(VortexFFIExpression * expr);

/// Creates a writer producing a Vortex file with the given schema (consumed). The bytes of the
/// file are sent to `write` with the given opaque `context`. The writer drives its own runtime on
/// the calling thread, so writing needs no threads from the caller. Returns nullptr on error.
VortexFFIWriter * vortex_ffi_writer_create(
    void * context, VortexFFIWriteCallback write, struct ArrowSchema * schema, char ** error);

/// Appends one record batch (consumed) to the file. The batch must have the same schema the
/// writer was created with. Returns 0 on success.
int32_t vortex_ffi_writer_write(
    VortexFFIWriter * writer, struct ArrowArray * array, struct ArrowSchema * schema, char ** error);

/// Flushes the remaining data and writes the file footer. Must be called exactly once before
/// `vortex_ffi_writer_free`. Returns 0 on success.
int32_t vortex_ffi_writer_finish(VortexFFIWriter * writer, char ** error);

void vortex_ffi_writer_free(VortexFFIWriter * writer);

/// Frees a string returned by this library (for example an error message).
void vortex_ffi_free_string(char * string);
}
