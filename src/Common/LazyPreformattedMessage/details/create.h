#pragma once

#include <Common/LazyPreformattedMessage/details/Arguments.h>
#include <Common/LazyPreformattedMessage/details/Message.h>
#include <Common/LazyPreformattedMessage/details/Storage.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <base/defines.h>

#include <fmt/format.h>

#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace DB::LazyPreformattedMessage
{

template <typename Stored>
std::string formatStored(fmt::string_view fmt_str, const void * storage)
{
    const auto & stored = *static_cast<const Stored *>(storage);
    return std::apply(
        [&](const auto &... args) { return fmt::vformat(fmt_str, fmt::make_format_args(args.value...)); },
        stored);
}

template <typename Stored>
void destroyStored(void * storage) noexcept
{
    static_cast<Stored *>(storage)->~Stored();
}

template <typename... Markers>
Message create(FormatStringHelper<typename std::remove_cvref_t<Markers>::value_type...> fmt, Markers &&... markers)
{
    chassert(!fmt.message_format_string.empty(), "LazyPreformattedMessage requires a static format string");

    using Stored = std::tuple<std::remove_cvref_t<Markers>...>;
    fmt::string_view fmt_str(fmt.message_format_string.data(), fmt.message_format_string.size());

    auto [lane, space] = Storage::allocate(fmt.message_format_string_hash, sizeof(Stored), alignof(Stored));
    new (space) Stored{std::forward<Markers>(markers)...};
    return Message(fmt_str, &formatStored<Stored>, &destroyStored<Stored>, space, lane);
}

}
