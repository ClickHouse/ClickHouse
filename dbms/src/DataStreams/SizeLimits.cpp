#include <DataStreams/SizeLimits.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <string>


namespace DB
{

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int too_many_rows_exception_code, int too_many_bytes_exception_code) const
{
    if (max_rows && rows > max_rows)
    {
        if (overflow_mode == OverflowMode::THROW)
            throw Exception("Limit for " + std::string(what) + " exceeded, max rows: " + formatReadableQuantity(max_rows)
                + ", current rows: " + formatReadableQuantity(rows), too_many_rows_exception_code);
        else
            return false;
    }

    if (max_bytes && bytes > max_bytes)
    {
        if (overflow_mode == OverflowMode::THROW)
            throw Exception("Limit for " + std::string(what) + " exceeded, max bytes: " + formatReadableSizeWithBinarySuffix(max_bytes)
                + ", current bytes: " + formatReadableSizeWithBinarySuffix(bytes), too_many_bytes_exception_code);
        else
            return false;
    }

    return true;
}

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const
{
    return check(rows, bytes, what, exception_code, exception_code);
}

}
