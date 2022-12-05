#pragma once

namespace DB
{

template <typename R>
struct KeyValuePairEscapingProcessor
{
    using Response = R;
    using ResponseViews = std::unordered_map<std::string_view, std::string_view>;

    KeyValuePairEscapingProcessor() = default;
    virtual ~KeyValuePairEscapingProcessor() = default;

    virtual Response process(const ResponseViews&) const = 0;
};

}
