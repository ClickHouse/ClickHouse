#pragma once

#include <cstdint>

extern "C" {

/// Converts a PRQL query to an SQL query.
/// @param query is a pointer to the beginning of the PRQL query.
/// @param size is the size of the PRQL query.
/// @param out is a pointer to a uint8_t pointer which will be set to the beginning of the null terminated SQL query or the error message.
/// @param out_size is the size of the string pointed by `out`.
/// @returns zero in case of success, non-zero in case of failure.
int64_t prql_to_sql(const uint8_t * query, uint64_t size, uint8_t ** out, uint64_t * out_size);

/// Frees the passed in pointer which's memory was allocated by Rust allocators previously.
void prql_free_pointer(uint8_t * ptr_to_free);

} // extern "C"
