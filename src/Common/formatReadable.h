#pragma once

#include <string>
#include <concepts>
#include <fmt/format.h>


namespace DB
{

class WriteBuffer;

}

/// Displays the passed size in bytes as 123.45 GiB.
void formatReadableSizeWithBinarySuffix(double value, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableSizeWithBinarySuffix(double value, int precision = 2);
template <std::integral T>
std::string formatReadableSizeWithBinarySuffix(T value, int precision = 2)
{
    return formatReadableSizeWithBinarySuffix(static_cast<double>(value), precision);
}

/// Displays the passed size in bytes as 132.55 GB.
void formatReadableSizeWithDecimalSuffix(double value, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableSizeWithDecimalSuffix(double value, int precision = 2);
template <std::integral T>
std::string formatReadableSizeWithDecimalSuffix(T value, int precision = 2)
{
    return formatReadableSizeWithDecimalSuffix(static_cast<double>(value), precision);
}

/// Prints the number as 123.45 billion.
void formatReadableQuantity(double value, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableQuantity(double value, int precision = 2);
template <std::integral T>
std::string formatReadableQuantity(T value, int precision = 2)
{
    return formatReadableQuantity(static_cast<double>(value), precision);
}

/// Prints the passed time in nanoseconds as 123.45 ms.
void formatReadableTime(double ns, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableTime(double ns, int precision = 2);

/// Wrapper around value. If used with fmt library (e.g. for log messages),
///  value is automatically formatted as size with binary suffix.
struct ReadableSize
{
    double value;
    explicit ReadableSize(double value_) : value(value_) {}

    template <std::integral T>
    explicit ReadableSize(T value_) : value(static_cast<double>(value_)) {}
};

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<ReadableSize>
{
    constexpr static auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const ReadableSize & size, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", formatReadableSizeWithBinarySuffix(size.value));
    }
};
