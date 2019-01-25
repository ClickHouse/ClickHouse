#pragma once
#include <Core/Types.h>

#include <sys/stat.h>
#include <type_traits>
#include <vector>
#include <map>

namespace DB
{

/// NOTE The code is totally wrong.
class JSONString
{
private:
    std::map<String, String> content;
    size_t padding;

public:
    explicit JSONString(size_t padding_ = 1) : padding(padding_) {}

    void set(const String key, String value, bool wrap = true);

    template <typename T>
    std::enable_if_t<std::is_arithmetic_v<T>> set(const String key, T value)
    {
        set(key, std::to_string(value), /*wrap= */ false);
    }

    void set(const String key, const std::vector<JSONString> & run_infos);

    String asString() const
    {
        return asString(padding);
    }

    String asString(size_t cur_padding) const;
};
}
