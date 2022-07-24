#include <QueryPipeline/SizeLimits.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <string>


namespace DB
{

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int too_many_rows_exception_code, int too_many_bytes_exception_code) const
{
    if (overflow_mode == OverflowMode::THROW)
    {
        if (max_rows && rows > max_rows)
            throw Exception("Limit for " + std::string(what) + " exceeded, max rows: " + formatReadableQuantity(max_rows)
                + ", current rows: " + formatReadableQuantity(rows), too_many_rows_exception_code);

        if (max_bytes && bytes > max_bytes)
            throw Exception(fmt::format("Limit for {} exceeded, max bytes: {}, current bytes: {}",
                std::string(what), ReadableSize(max_bytes), ReadableSize(bytes)), too_many_bytes_exception_code);

        return true;
    }

    return softCheck(rows, bytes);
}

bool SizeLimits::softCheck(UInt64 rows, UInt64 bytes) const
{
    if (max_rows && rows > max_rows)
        return false;
    if (max_bytes && bytes > max_bytes)
        return false;
    return true;
}

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const
{
    return check(rows, bytes, what, exception_code, exception_code);
}

}
