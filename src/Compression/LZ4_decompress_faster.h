#pragma once

#include <cmath>
#include <random>
#include <pcg_random.hpp>

#include <absl/container/inlined_vector.h>

namespace LZ4
{

/** Both buffers passed to 'decompress' function must have
  *  at least this amount of excessive bytes after end of data
  *  that is allowed to read/write.
  * This value is a little overestimation.
  */
static constexpr size_t ADDITIONAL_BYTES_AT_END_OF_BUFFER = 64;


/** This method dispatch to one of different implementations depending on performance statistics.
  */
bool decompress(
    const char * const source, /// NOLINT
    char * const dest, /// NOLINT
    size_t source_size,
    size_t dest_size);

}
