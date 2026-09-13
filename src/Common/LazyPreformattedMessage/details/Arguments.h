#pragma once

namespace DB::LazyPreformattedMessage
{

template <typename T>
struct RefArg
{
    using value_type = T;
    const T & value;
};

template <typename T>
struct CopyArg
{
    using value_type = T;
    T value;
};

}
