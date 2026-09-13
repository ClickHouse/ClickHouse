#pragma once

#include <Common/LazyPreformattedMessage/impl/Arguments.h>
#include <Common/LazyPreformattedMessage/impl/Message.h>
#include <Common/LazyPreformattedMessage/impl/create.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <type_traits>
#include <utility>

namespace DB
{

using LazyPreformattedMessage = LazyPreformattedMessageImpl::Message;

template <typename T>
LazyPreformattedMessageImpl::RefArg<std::remove_cvref_t<T>> refArg(const T & x)
{
    return {x};
}

template <typename T>
LazyPreformattedMessageImpl::CopyArg<std::remove_cvref_t<T>> copyArg(T && x)
{
    return {std::forward<T>(x)};
}

template <typename... Markers>
LazyPreformattedMessage createLazyMessage(FormatStringHelper<typename std::remove_cvref_t<Markers>::value_type...> fmt, Markers &&... markers)
{
    return LazyPreformattedMessageImpl::create(std::move(fmt), std::forward<Markers>(markers)...);
}

}
