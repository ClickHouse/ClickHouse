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
///   - Arrow structs returned through out-parameters are owned by the caller, which must
///     release them through their `release` callback (e.g. by importing them with
///     `arrow::ImportRecordBatch` / `arrow::ImportSchema`).
///   - Error messages are heap-allocated and must be freed with `vortex_ffi_free_string`.
///
/// Handles must not be used from multiple threads concurrently, but may be moved between
/// threads. All work, including IO callbacks, happens on the thread of the FFI call.

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

/// Reads `length` bytes at `offset` into `out`. Returns 0 on success, non-zero on failure.
using VortexFFIReadCallback = int32_t (*)(void * context, uint64_t offset, uint64_t length, uint8_t * out);

/// Consumes `length` bytes from `data`. Returns 0 on success, non-zero on failure.
using VortexFFIWriteCallback = int32_t (*)(void * context, const uint8_t * data, uint64_t length);

struct VortexFFIReader;
struct VortexFFIScanner;
struct VortexFFIWriter;
struct VortexFFIExpression;

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

/// Opens a Vortex file for reading. The file is accessed through `read` with the given opaque
/// `context`; `file_size` must be the exact file size in bytes. Returns nullptr on error.
VortexFFIReader * vortex_ffi_reader_open(
    void * context, VortexFFIReadCallback read, uint64_t file_size, char ** error);

/// Returns the total number of rows in the file.
uint64_t vortex_ffi_reader_row_count(const VortexFFIReader * reader);

/// Exports the file schema into `out_schema`. Returns 0 on success.
int32_t vortex_ffi_reader_schema(const VortexFFIReader * reader, struct ArrowSchema * out_schema, char ** error);

/// Creates a scanner over the file. If `columns` is not nullptr, only the `num_columns` columns
/// with the given names are read, in the given order. If `filter` is not nullptr, only the rows
/// matching the filter expression are returned; the file may use it to prune whole segments by
/// statistics and to decode only the matching rows. The reader must stay alive for the whole
/// lifetime of the scanner; `filter` is not consumed. Returns nullptr on error.
VortexFFIScanner * vortex_ffi_scanner_create(
    const VortexFFIReader * reader,
    const char * const * columns,
    uint64_t num_columns,
    const VortexFFIExpression * filter,
    char ** error);

/// Reads the next batch of rows into `out_array` + `out_schema`. Returns 1 if a batch was
/// produced, 0 at the end of the file, -1 on error.
int32_t vortex_ffi_scanner_next(
    VortexFFIScanner * scanner, struct ArrowArray * out_array, struct ArrowSchema * out_schema, char ** error);

void vortex_ffi_scanner_free(VortexFFIScanner * scanner);

void vortex_ffi_reader_free(VortexFFIReader * reader);

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
/// file are sent to `write` with the given opaque `context`. Returns nullptr on error.
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
