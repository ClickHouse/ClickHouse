#include <QueryPipeline/SizeLimits.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event OverflowThrow;
    extern const Event OverflowBreak;
}


namespace DB
{

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int too_many_rows_exception_code, int too_many_bytes_exception_code) const
{
    if (overflow_mode == OverflowMode::THROW)
    {
        if (max_rows && rows > max_rows)
        {
            ProfileEvents::increment(ProfileEvents::OverflowThrow);
            throw Exception(
                too_many_rows_exception_code,
                "Limit for {} exceeded, max rows: {}, current rows: {}",
                what,
                formatReadableQuantity(max_rows),
                formatReadableQuantity(rows));
        }

        if (max_bytes && bytes > max_bytes)
        {
            ProfileEvents::increment(ProfileEvents::OverflowThrow);
            throw Exception(
                too_many_bytes_exception_code,
                "Limit for {} exceeded, max bytes: {}, current bytes: {}",
                what,
                ReadableSize(max_bytes),
                ReadableSize(bytes));
        }

        return true;
    }

    return softCheck(rows, bytes);
}

bool SizeLimits::softCheck(UInt64 rows, UInt64 bytes) const
{
    /// For result_overflow_mode = 'break', we check for >= to tell that no more data is needed.
    /// Last chunk will be processed.
    if ((max_rows && rows >= max_rows)
        || (max_bytes && bytes >= max_bytes))
    {
        ProfileEvents::increment(ProfileEvents::OverflowBreak);
        return false;
    }
    return true;
}

bool SizeLimits::check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const
{
    return check(rows, bytes, what, exception_code, exception_code);
}

}
