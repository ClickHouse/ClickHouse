#pragma once

#include <stdexcept>
#include <string>

/** Exception type thrown by the pcodec port on corrupt input or invalid arguments.
  *
  * The `Pcodec` library is kept self-contained (so it can be unit-tested in isolation), hence it
  * does not throw `DB::Exception` directly. The `CompressionCodecPco` wrapper catches this and
  * rethrows as a ClickHouse exception with the appropriate error code (`CANNOT_DECOMPRESS` etc.).
  */
namespace DB::Pcodec
{

class PcodecError : public std::runtime_error
{
public:
    explicit PcodecError(const std::string & message) : std::runtime_error(message) { }
    explicit PcodecError(const char * message) : std::runtime_error(message) { }
};

}
