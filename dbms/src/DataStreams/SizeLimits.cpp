#include <DataStreams/SizeLimits.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <string>


namespace DB
{

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const
{
    if (max_rows)
    {
        if (rows > max_rows && overflow_mode == OverflowMode::THROW)
            throw Exception("Limit for " + std::string(what) + " exceeded, max rows: " + formatReadableQuantity(max_rows)
                + ", current rows: " + formatReadableQuantity(rows), exception_code);
        else if (rows >= max_rows && overflow_mode == OverflowMode::BREAK)
            return false;
    }

    if (max_bytes)
    {
        if (bytes > max_bytes && overflow_mode == OverflowMode::THROW)
            throw Exception("Limit for " + std::string(what) + " exceeded, max bytes: " + formatReadableSizeWithBinarySuffix(max_bytes)
                + ", current bytes: " + formatReadableSizeWithBinarySuffix(bytes), exception_code);
        else if (bytes >= max_bytes && overflow_mode == OverflowMode::BREAK)
            return false;
    }

    return true;
}

}
