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
    std::map<std::string, std::string> content;
    size_t padding;

public:
    explicit JSONString(size_t padding_ = 1) : padding(padding_) {}

    void set(const std::string & key, std::string value, bool wrap = true);

    template <typename T>
    std::enable_if_t<is_arithmetic_v<T>> set(const std::string key, T value)
    {
        set(key, std::to_string(value), /*wrap= */ false);
    }

    void set(const std::string & key, const std::vector<JSONString> & run_infos);

    std::string asString() const
    {
        return asString(padding);
    }

    std::string asString(size_t cur_padding) const;
};

}
