#pragma once

#include <Functions/likePatternToRegexp.h>
#include <Common/ObjectPool.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/ProfileEvents.h>
#include <common/StringRef.h>
#include <memory>
#include <string>
#include <vector>

#include <Common/config.h>
#if USE_HYPERSCAN
#   if __has_include(<hs/hs.h>)
#       include <hs/hs.h>
#   else
#       include <hs.h>
#   endif
#endif

namespace ProfileEvents
{
extern const Event RegexpCreated;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}

namespace Regexps
{
    using Regexp = OptimizedRegularExpressionImpl<false>;
    using Pool = ObjectPoolMap<Regexp, String>;

    template <bool like>
    inline Regexp createRegexp(const std::string & pattern, int flags)
    {
        return {pattern, flags};
    }

    template <>
    inline Regexp createRegexp<true>(const std::string & pattern, int flags)
    {
        return {likePatternToRegexp(pattern), flags};
    }

    template <bool like, bool no_capture>
    inline Pool::Pointer get(const std::string & pattern)
    {
        /// C++11 has thread-safe function-local statics on most modern compilers.
        static Pool known_regexps; /// Different variables for different pattern parameters.

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

#if USE_HYPERSCAN

namespace MultiRegexps
{
    template <typename Deleter, Deleter deleter>
    struct HyperscanDeleter
    {
        template <typename T>
        void operator()(T * ptr) const
        {
            deleter(ptr);
        }
    };

    using Regexps = std::unique_ptr<hs_database_t, HyperscanDeleter<decltype(&hs_free_database), &hs_free_database>>;

    using Pool = ObjectPoolMap<Regexps, std::vector<String>>;

    template <bool FindAnyIndex>
    inline Pool::Pointer get(const std::vector<StringRef> & patterns)
    {
        /// C++11 has thread-safe function-local statics on most modern compilers.
        static Pool known_regexps; /// Different variables for different pattern parameters.

        std::vector<String> str_patterns;
        str_patterns.reserve(patterns.size());
        for (const StringRef & ref : patterns)
            str_patterns.push_back(ref.toString());

        return known_regexps.get(str_patterns, [&str_patterns]
        {
            std::vector<const char *> ptrns;
            std::vector<unsigned int> flags;
            ptrns.reserve(str_patterns.size());
            flags.reserve(str_patterns.size());
            for (const StringRef ref : str_patterns)
            {
                ptrns.push_back(ref.data);
                flags.push_back(HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_SINGLEMATCH);
            }
            hs_database_t * db = nullptr;
            hs_compile_error_t * compile_error;


            std::unique_ptr<unsigned int[]> ids;

            if constexpr (FindAnyIndex)
            {
                ids.reset(new unsigned int[ptrns.size()]);
                for (size_t i = 0; i < ptrns.size(); ++i)
                    ids[i] = i + 1;
            }

            hs_error_t err
                = hs_compile_multi(ptrns.data(), flags.data(), ids.get(), ptrns.size(), HS_MODE_BLOCK, nullptr, &db, &compile_error);
            if (err != HS_SUCCESS)
            {
                std::unique_ptr<
                    hs_compile_error_t,
                    HyperscanDeleter<decltype(&hs_free_compile_error), &hs_free_compile_error>> error(compile_error);

                if (error->expression < 0)
                    throw Exception(String(error->message), ErrorCodes::LOGICAL_ERROR);
                else
                    throw Exception(
                        "Pattern '" + str_patterns[error->expression] + "' failed with error '" + String(error->message),
                        ErrorCodes::LOGICAL_ERROR);
            }

            ProfileEvents::increment(ProfileEvents::RegexpCreated);

            return new Regexps{db};
        });
    }
}

#endif // USE_HYPERSCAN

}
