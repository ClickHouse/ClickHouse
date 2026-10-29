#pragma once

#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <Common/NoDumpAllocator.h>

namespace DB
{

/// Bytes are kept in memory that is excluded from core dumps.
class SecretString
{
public:
    SecretString() = default;

    explicit SecretString(std::string_view str) : bytes(str.begin(), str.end()) {}

    SecretString(size_t count, char c) : bytes(count, c) {}

    SecretString & operator=(std::string_view str) { bytes.assign(str.begin(), str.end()); return *this; }

    operator std::string_view() const { return {bytes.data(), bytes.size()}; } /// NOLINT(google-explicit-constructor)

    bool empty() const { return bytes.empty(); }
    size_t size() const { return bytes.size(); }
    const char * data() const { return bytes.data(); }
    void clear() { bytes.clear(); }

    bool operator==(const SecretString & rhs) const = default;
    bool operator==(std::string_view rhs) const { return std::string_view(*this) == rhs; }

private:
    std::vector<char, NoDumpAllocator<char>> bytes;
};

}

template <>
struct fmt::formatter<DB::SecretString> : fmt::formatter<std::string_view>
{
    auto format(const DB::SecretString & str, format_context & ctx) const
    {
        return fmt::formatter<std::string_view>::format(str, ctx);
    }
};
