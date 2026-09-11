#pragma once

#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <Common/NoDumpAllocator.h>

namespace DB
{

/// Bytes are kept in memory that is excluded from core dumps.
class SensitiveString
{
public:
    SensitiveString() = default;
    explicit SensitiveString(std::string_view str) : data(str.begin(), str.end()) {}
    SensitiveString & operator=(std::string_view str) { data.assign(str.begin(), str.end()); return *this; }

    operator std::string_view() const { return {data.data(), data.size()}; }
    bool empty() const { return data.empty(); }
    void clear() { data.clear(); }

    bool operator==(const SensitiveString & rhs) const = default;
    bool operator==(std::string_view rhs) const { return std::string_view(*this) == rhs; }

private:
    std::vector<char, NoDumpAllocator<char>> data;
};

}

template <>
struct fmt::formatter<DB::SensitiveString> : fmt::formatter<std::string_view>
{
    auto format(const DB::SensitiveString & str, format_context & ctx) const
    {
        return fmt::formatter<std::string_view>::format(str, ctx);
    }
};
