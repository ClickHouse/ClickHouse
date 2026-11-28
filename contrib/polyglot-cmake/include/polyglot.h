#pragma once

#include <cstdint>

extern "C" {

/// Transpiles SQL from a source dialect to ClickHouse SQL.
/// @param query is a pointer to the beginning of the SQL query.
/// @param query_size is the size of the SQL query.
/// @param source_dialect is a pointer to the beginning of the dialect name string (e.g. "sqlite", "mysql").
/// @param source_dialect_size is the size of the dialect name string.
/// @param out is a pointer to a uint8_t pointer which will be set to the beginning of the null terminated transpiled SQL or the error message.
/// @param out_size is the size of the string pointed by `out`.
/// @returns zero in case of success, non-zero in case of failure.
int64_t polyglot_transpile(
    const uint8_t * query,
    uint64_t query_size,
    const uint8_t * source_dialect,
    uint64_t source_dialect_size,
    uint8_t ** out,
    uint64_t * out_size);

/// Frees the passed in pointer whose memory was allocated by Rust allocators previously.
void polyglot_free_pointer(uint8_t * ptr_to_free);

} // extern "C"
