#include <Processors/Formats/Impl/ORC/Timezone.h>

#include <Common/Exception.h>
#include <base/unaligned.h>

#include <string_view>

/// The embedded time zone files, see `DateLUTImpl.cpp`.
std::string_view getTimeZone(const char * name);  /// NOLINT(misc-use-internal-linkage)

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::ORC
{

TimezoneOffset::TimezoneOffset(const DateLUTImpl & lut_)
    : lut(lut_)
{
    const std::string & name = lut.getTimeZone();
    const std::string_view file = getTimeZone(name.c_str());
    /// `Fixed/UTC±HH:MM:SS` zones have no file. On this branch `DateLUT` also loads zones that are not embedded
    /// (`libc:` names, the host database), so keep the offsets of `DateLUTImpl` for all of them.
    if (file.empty())
        return;

    /// Raw records, as `cctz::time_zone::next_transition` skips no-op ones (Europe/Lisbon has one in 1884).
    static constexpr size_t header_size = 44;
    size_t pos = 0;
    auto check = [&](size_t size)
    {
        if (size > file.size() - pos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Malformed time zone file for {}", name);
    };
    auto read_count = [&](size_t index) { return unalignedLoadBigEndian<UInt32>(file.data() + pos + 20 + index * 4); };

    check(header_size);
    if (!file.starts_with("TZif"))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Malformed time zone file for {}", name);
    size_t time_size = 4;
    /// Version 2 and later repeat the data with 64-bit times after the 32-bit section.
    if (file[4] != '\0')
    {
        const size_t v1_size = read_count(3) * 5 + read_count(4) * 6 + read_count(5) + read_count(2) * 8 + read_count(1) + read_count(0);
        check(header_size + v1_size);
        pos += header_size + v1_size;
        check(header_size);
        time_size = 8;
    }
    const size_t time_count = read_count(3);
    const size_t type_count = read_count(4);
    pos += header_size;
    check(time_count * (time_size + 1) + type_count * 6);
    if (time_count == 0)
        return;

    const char * times = file.data() + pos;
    const auto * type_indices = reinterpret_cast<const UInt8 *>(times + time_count * time_size);
    const char * types = times + time_count * (time_size + 1);
    auto type_record = [&](size_t type)
    {
        if (type >= type_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Malformed time zone file for {}", name);
        return types + type * 6;
    };

    first_transition = time_size == 8 ? unalignedLoadBigEndian<Int64>(times) : unalignedLoadBigEndian<Int32>(times);
    /// The oldest standard time, or the first type if all of them are daylight saving time.
    const char * ancient = type_record(0);
    for (size_t i = 0; i < time_count; ++i)
    {
        const char * record = type_record(type_indices[i]);
        if (record[4] == 0)
        {
            ancient = record;
            break;
        }
    }
    ancient_offset = unalignedLoadBigEndian<Int32>(ancient);
}

}
