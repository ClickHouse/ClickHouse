#pragma once

#include <type_traits>

namespace DB
{

namespace FunctionJSONHelpersDetails
{
    template <class T, class = void>
    struct has_index_operator : std::false_type {};

    template <class T>
    struct has_index_operator<T, std::void_t<decltype(std::declval<T>()[0])>> : std::true_type {};
}

}
