#pragma once

#include <Common/LoggingFormatStringHelpers.h>

#include <boost/noncopyable.hpp>
#include <fmt/format.h>

#include <cstddef>
#include <string>

namespace DB::LazyPreformattedMessageImpl
{

class Message : private boost::noncopyable
{
    void destroy() noexcept;

public:
    using FormatFn = std::string (*)(fmt::string_view, const void *);
    using DestroyFn = void (*)(void *) noexcept;

    Message(fmt::string_view fmt_str_, FormatFn format_fn_, DestroyFn destroy_fn_, void * args_, size_t lane_);
    Message(Message && other) noexcept;
    Message & operator=(Message && other) noexcept;
    ~Message();

    PreformattedMessage format() const;

private:
    fmt::string_view fmt_str;
    FormatFn format_fn = nullptr;
    DestroyFn destroy_fn = nullptr;
    void * args = nullptr;
    size_t lane = 0;
};

}
