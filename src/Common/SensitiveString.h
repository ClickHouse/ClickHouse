#pragma once

#include <string_view>
#include <vector>

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

    std::string_view view() const { return {data.data(), data.size()}; }
    bool empty() const { return data.empty(); }
    void clear() { data.clear(); }

private:
    std::vector<char, NoDumpAllocator<char>> data;
};

}
