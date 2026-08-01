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

/// Opens a Vortex file for reading. The file is accessed through `read` with the given opaque
/// `context`; `file_size` must be the exact file size in bytes. Returns nullptr on error.
VortexFFIReader * vortex_ffi_reader_open(
    void * context, VortexFFIReadCallback read, uint64_t file_size, char ** error);

/// Returns the total number of rows in the file.
uint64_t vortex_ffi_reader_row_count(const VortexFFIReader * reader);

/// Exports the file schema into `out_schema`. Returns 0 on success.
int32_t vortex_ffi_reader_schema(const VortexFFIReader * reader, struct ArrowSchema * out_schema, char ** error);

/// Creates a scanner over the file. If `columns` is not nullptr, only the `num_columns` columns
/// with the given names are read, in the given order. The reader must stay alive for the whole
/// lifetime of the scanner. Returns nullptr on error.
VortexFFIScanner * vortex_ffi_scanner_create(
    const VortexFFIReader * reader, const char * const * columns, uint64_t num_columns, char ** error);

/// Reads the next batch of rows into `out_array` + `out_schema`. Returns 1 if a batch was
/// produced, 0 at the end of the file, -1 on error.
int32_t vortex_ffi_scanner_next(
    VortexFFIScanner * scanner, struct ArrowArray * out_array, struct ArrowSchema * out_schema, char ** error);

void vortex_ffi_scanner_free(VortexFFIScanner * scanner);

void vortex_ffi_reader_free(VortexFFIReader * reader);

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
