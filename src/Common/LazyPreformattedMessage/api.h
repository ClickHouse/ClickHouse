#pragma once

#include <Common/LazyPreformattedMessage/details/Arguments.h>
#include <Common/LazyPreformattedMessage/details/Message.h>
#include <Common/LazyPreformattedMessage/details/create.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <type_traits>
#include <utility>

namespace DB
{

template <typename T>
LazyPreformattedMessage::RefArg<std::remove_cvref_t<T>> refArg(const T & x)
{
    return {x};
}

template <typename T>
LazyPreformattedMessage::CopyArg<std::remove_cvref_t<T>> copyArg(T && x)
{
    return {std::forward<T>(x)};
}

template <typename... Markers>
LazyPreformattedMessage::Message createLazyMessage(FormatStringHelper<typename std::remove_cvref_t<Markers>::value_type...> fmt, Markers &&... markers)
{
    return LazyPreformattedMessage::create(std::move(fmt), std::forward<Markers>(markers)...);
}

}
