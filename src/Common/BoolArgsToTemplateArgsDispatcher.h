#pragma once
#include <utility>

namespace DB
{

/// Special struct that helps to convert bool variables to function template bool arguments.
/// It can be used to avoid multiple nested if/else on bool arguments. How to use it:
/// Imagine you have template function
/// template <bool b1, bool b2, ..., bn> return_type foo(...);
/// and bool variables b1, b2, ..., bool bn. To pass these variables as template for foo you can do the following:
///
/// auto call_foo = []<bool b1, bool b2, ..., bool bn>()
/// {
///     return foo<b1, b2, ..., bn>(...);
/// }
///
/// BoolArgsToTemplateArgsDispatcher::call<decltype(call_foo)>(call_foo, b1, b2, ..., bn);

template <class Functor, bool... Args>
struct BoolArgsToTemplateArgsDispatcher
{
    template <typename... Args1>
    static auto call(Functor f, Args1&&... args)
    {
        return f.template operator()<Args...>(std::forward<Args1>(args)...);
    }

    template <class... Args1>
    static auto call(Functor f, bool b, Args1&&... ar1)
    {
        if (b)
            return BoolArgsToTemplateArgsDispatcher<Functor, Args..., true>::call(f, std::forward<Args1>(ar1)...);
        else
            return BoolArgsToTemplateArgsDispatcher<Functor, Args..., false>::call(f, std::forward<Args1>(ar1)...);
    }
};

}
