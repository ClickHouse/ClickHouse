#include <Storages/TimeSeries/getPrometheusLabelsHash.h>

#include <base/types.h>

#include <xxhash.h>


namespace DB
{

UInt64 getPrometheusLabelsHash(const std::vector<std::pair<std::string_view, std::string_view>> & sorted_labels)
{
    /// A length below 255 takes one byte, a longer one takes the byte 255 and three bytes in little endian.
    String data;
    auto append = [&](std::string_view str)
    {
        if (str.size() < 255)
        {
            data += static_cast<char>(str.size());
        }
        else
        {
            data += static_cast<char>(255);
            for (size_t i = 0; i != 3; ++i)
                data += static_cast<char>((str.size() >> (8 * i)) & 0xFF);
        }
        data += str;
    };

    for (const auto & [name, value] : sorted_labels)
    {
        append(name);
        append(value);
    }

/// With XXH_INLINE_ALL (from contrib/xxHash) every XXH function is marked as unused, so any actual use triggers this warning.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
    return XXH_INLINE_XXH64(data.data(), data.size(), 0);
#pragma clang diagnostic pop
}

}
