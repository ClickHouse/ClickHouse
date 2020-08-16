#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Functions/likePatternToRegexp.h>
#include <Common/Exception.h>
#include <Common/ObjectPool.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/ProfileEvents.h>
#include <common/StringRef.h>

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_HYPERSCAN
#    include <hs.h>
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
    extern const int BAD_ARGUMENTS;
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

    /** Returns holder of an object from Pool.
      * You must hold the ownership while using the object.
      * In destructor, it returns the object back to the Pool for further reuse.
      */
    template <bool like, bool no_capture>
    inline Pool::Pointer get(const std::string & pattern, int flags = 0)
    {
        /// C++11 has thread-safe function-local statics on most modern compilers.
        static Pool known_regexps; /// Different variables for different pattern parameters.

        return known_regexps.get(pattern, [flags, &pattern]
        {
            int flags_final = flags | OptimizedRegularExpression::RE_DOT_NL;

            if (no_capture)
                flags_final |= OptimizedRegularExpression::RE_NO_CAPTURE;

            ProfileEvents::increment(ProfileEvents::RegexpCreated);
            return new Regexp{createRegexp<like>(pattern, flags_final)};
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

    /// Helper unique pointers to correctly delete the allocated space when hyperscan cannot compile something and we throw an exception.
    using CompilerError = std::unique_ptr<hs_compile_error_t, HyperscanDeleter<decltype(&hs_free_compile_error), &hs_free_compile_error>>;
    using ScratchPtr = std::unique_ptr<hs_scratch_t, HyperscanDeleter<decltype(&hs_free_scratch), &hs_free_scratch>>;
    using DataBasePtr = std::unique_ptr<hs_database_t, HyperscanDeleter<decltype(&hs_free_database), &hs_free_database>>;

    /// Database is thread safe across multiple threads and Scratch is not but we can copy it whenever we use it in the searcher.
    class Regexps
    {
    public:
        Regexps(hs_database_t * db_, hs_scratch_t * scratch_) : db{db_}, scratch{scratch_} { }

        hs_database_t * getDB() const { return db.get(); }
        hs_scratch_t * getScratch() const { return scratch.get(); }

    private:
        DataBasePtr db;
        ScratchPtr scratch;
    };

    struct Pool
    {
        /// Mutex for finding in map.
        std::mutex mutex;
        /// Patterns + possible edit_distance to database and scratch.
        std::map<std::pair<std::vector<String>, std::optional<UInt32>>, Regexps> storage;
    };

    template <bool save_indices, bool CompileForEditDistance>
    inline Regexps constructRegexps(const std::vector<String> & str_patterns, std::optional<UInt32> edit_distance)
    {
        (void)edit_distance;
        /// Common pointers
        std::vector<const char *> patterns;
        std::vector<unsigned int> flags;

        /// Pointer for external edit distance compilation
        std::vector<hs_expr_ext> ext_exprs;
        std::vector<const hs_expr_ext *> ext_exprs_ptrs;

        patterns.reserve(str_patterns.size());
        flags.reserve(str_patterns.size());

        if constexpr (CompileForEditDistance)
        {
            ext_exprs.reserve(str_patterns.size());
            ext_exprs_ptrs.reserve(str_patterns.size());
        }

        for (const StringRef ref : str_patterns)
        {
            patterns.push_back(ref.data);
            /* Flags below are the pattern matching flags.
             * HS_FLAG_DOTALL is a compile flag where matching a . will not exclude newlines. This is a good
             * performance practice according to Hyperscan API. https://intel.github.io/hyperscan/dev-reference/performance.html#dot-all-mode
             * HS_FLAG_ALLOWEMPTY is a compile flag where empty strings are allowed to match.
             * HS_FLAG_UTF8 is a flag where UTF8 literals are matched.
             * HS_FLAG_SINGLEMATCH is a compile flag where each pattern match will be returned only once. it is a good performance practice
             * as it is said in the Hyperscan documentation. https://intel.github.io/hyperscan/dev-reference/performance.html#single-match-flag
             */
            flags.push_back(HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8);
            if constexpr (CompileForEditDistance)
            {
                /// Hyperscan currently does not support UTF8 matching with edit distance.
                flags.back() &= ~HS_FLAG_UTF8;
                ext_exprs.emplace_back();
                /// HS_EXT_FLAG_EDIT_DISTANCE is a compile flag responsible for Levenstein distance.
                ext_exprs.back().flags = HS_EXT_FLAG_EDIT_DISTANCE;
                ext_exprs.back().edit_distance = edit_distance.value();
                ext_exprs_ptrs.push_back(&ext_exprs.back());
            }
        }
        hs_database_t * db = nullptr;
        hs_compile_error_t * compile_error;


        std::unique_ptr<unsigned int[]> ids;

        /// We mark the patterns to provide the callback results.
        if constexpr (save_indices)
        {
            ids.reset(new unsigned int[patterns.size()]);
            for (size_t i = 0; i < patterns.size(); ++i)
                ids[i] = i + 1;
        }

        hs_error_t err;
        if constexpr (!CompileForEditDistance)
            err = hs_compile_multi(
                patterns.data(),
                flags.data(),
                ids.get(),
                patterns.size(),
                HS_MODE_BLOCK,
                nullptr,
                &db,
                &compile_error);
        else
            err = hs_compile_ext_multi(
                patterns.data(),
                flags.data(),
                ids.get(),
                ext_exprs_ptrs.data(),
                patterns.size(),
                HS_MODE_BLOCK,
                nullptr,
                &db,
                &compile_error);

        if (err != HS_SUCCESS)
        {
            /// CompilerError is a unique_ptr, so correct memory free after the exception is thrown.
            CompilerError error(compile_error);

            if (error->expression < 0)
                throw Exception(String(error->message), ErrorCodes::LOGICAL_ERROR);
            else
                throw Exception(
                    "Pattern '" + str_patterns[error->expression] + "' failed with error '" + String(error->message),
                    ErrorCodes::BAD_ARGUMENTS);
        }

        ProfileEvents::increment(ProfileEvents::RegexpCreated);

        /// We allocate the scratch space only once, then copy it across multiple threads with hs_clone_scratch
        /// function which is faster than allocating scratch space each time in each thread.
        hs_scratch_t * scratch = nullptr;
        err = hs_alloc_scratch(db, &scratch);

        /// If not HS_SUCCESS, it is guaranteed that the memory would not be allocated for scratch.
        if (err != HS_SUCCESS)
            throw Exception("Could not allocate scratch space for hyperscan", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        return Regexps{db, scratch};
    }

    /// If CompileForEditDistance is False, edit_distance must be nullopt
    /// Also, we use templates here because each instantiation of function
    /// template has its own copy of local static variables which must not be the same
    /// for different hyperscan compilations.
    template <bool save_indices, bool CompileForEditDistance>
    inline Regexps * get(const std::vector<StringRef> & patterns, std::optional<UInt32> edit_distance)
    {
        /// C++11 has thread-safe function-local statics on most modern compilers.
        static Pool known_regexps; /// Different variables for different pattern parameters.

        std::vector<String> str_patterns;
        str_patterns.reserve(patterns.size());
        for (const StringRef & ref : patterns)
            str_patterns.push_back(ref.toString());

        /// Get the lock for finding database.
        std::unique_lock lock(known_regexps.mutex);

        auto it = known_regexps.storage.find({str_patterns, edit_distance});

        /// If not found, compile and let other threads wait.
        if (known_regexps.storage.end() == it)
            it = known_regexps.storage
                     .emplace(
                         std::pair{str_patterns, edit_distance},
                         constructRegexps<save_indices, CompileForEditDistance>(str_patterns, edit_distance))
                     .first;
        /// If found, unlock and return the database.
        lock.unlock();

        return &it->second;
    }
}

#endif // USE_HYPERSCAN

}
