#pragma once
#include <Common/OptimizedRegularExpression.h>
#include <Functions/ObjectPool.h>
#include <Functions/likePatternToRegexp.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event RegexpCreated;
}


namespace DB
{

namespace Regexps
{
    using Regexp = OptimizedRegularExpressionImpl<false>;
    using Pool = ObjectPoolMap<Regexp, String>;

    template <bool like>
    inline Regexp createRegexp(const std::string & pattern, int flags) { return {pattern, flags}; }

    template <>
    inline Regexp createRegexp<true>(const std::string & pattern, int flags) { return {likePatternToRegexp(pattern), flags}; }

    template <bool like, bool no_capture>
    inline Pool::Pointer get(const std::string & pattern)
    {
        /// C++11 has thread-safe function-local statics on most modern compilers.
        static Pool known_regexps;    /// Different variables for different pattern parameters.

        return known_regexps.get(pattern, [&pattern]
        {
            int flags = OptimizedRegularExpression::RE_DOT_NL;
            if (no_capture)
                flags |= OptimizedRegularExpression::RE_NO_CAPTURE;

            ProfileEvents::increment(ProfileEvents::RegexpCreated);
            return new Regexp{createRegexp<like>(pattern, flags)};
        });
    }
}

}
