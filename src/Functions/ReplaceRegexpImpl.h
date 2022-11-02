#pragma once

#include <base/scope_guard.h>
#include <base/types.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>
#include <shared_mutex>

#include "config.h"

#include <re2_st/re2.h>
#if USE_VECTORSCAN
#    include <hs.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
    extern const int LOGICAL_ERROR;
}

struct ReplaceRegexpTraits
{
    enum class Replace
    {
        First,
        All
    };
};

/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' can contain substitutions, for example: '\2-\3-\1'
  */
template <ReplaceRegexpTraits::Replace replace>
struct ReplaceRegexpImpl
{
    struct Instruction
    {
        /// If not negative, perform substitution of n-th subpattern from the regexp match.
        int substitution_num = -1;
        /// Otherwise, paste this literal string verbatim.
        String literal;

        explicit Instruction(int substitution_num_) : substitution_num(substitution_num_) {}
        explicit Instruction(String literal_) : literal(std::move(literal_)) {}
    };

    /// Decomposes the replacement string into a sequence of substitutions and literals.
    /// E.g. "abc\1de\2fg\1\2" --> inst("abc"), inst(1), inst("de"), inst(2), inst("fg"), inst(1), inst(2)
    using Instructions = std::vector<Instruction>;

    static constexpr int max_captures_re2 = 10;

    static Instructions createInstructions(std::string_view replacement, int num_captures)
    {
        Instructions instructions;

        String literals;
        for (size_t i = 0; i < replacement.size(); ++i)
        {
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// Substitution
                {
                    if (!literals.empty())
                    {
                        instructions.emplace_back(literals);
                        literals = "";
                    }
                    instructions.emplace_back(replacement[i + 1] - '0');
                }
                else
                    literals += replacement[i + 1]; /// Escaping
                ++i;
            }
            else
                literals += replacement[i]; /// Plain character
        }

        if (!literals.empty())
            instructions.emplace_back(literals);

        for (const auto & instr : instructions)
            if (instr.substitution_num >= num_captures)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Id {} in replacement string is an invalid substitution, regexp has only {} capturing groups",
                    instr.substitution_num, num_captures - 1);

        return instructions;
    }

    static void processStringWithRe2(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        const re2_st::RE2 & searcher,
        int num_captures,
        const Instructions & instructions)
    {
        re2_st::StringPiece haystack(haystack_data, haystack_length);
        re2_st::StringPiece matches[max_captures_re2];

        size_t copy_pos = 0;
        size_t match_pos = 0;

        while (match_pos < haystack_length)
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(haystack, match_pos, haystack_length, re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                const auto & match = matches[0]; /// Complete match (\0)
                size_t bytes_to_copy = (match.data() - haystack.data()) - copy_pos;

                /// Copy prefix before current match without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                copy_pos += bytes_to_copy + match.length();
                match_pos = copy_pos;

                /// Substitute inside current match using instructions
                for (const auto & instr : instructions)
                {
                    std::string_view replacement;
                    if (instr.substitution_num >= 0)
                        replacement = std::string_view(matches[instr.substitution_num].data(), matches[instr.substitution_num].size());
                    else
                        replacement = instr.literal;
                    res_data.resize(res_data.size() + replacement.size());
                    memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                    res_offset += replacement.size();
                }

                if constexpr (replace == ReplaceRegexpTraits::Replace::First)
                    can_finish_current_string = true;

                if (match.empty())
                {
                    /// Step one character to avoid infinite loop
                    ++match_pos;
                    if (match_pos >= haystack_length)
                        can_finish_current_string = true;
                }
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + haystack_length - copy_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, haystack_length - copy_pos);
                res_offset += haystack_length - copy_pos;
                copy_pos = haystack_length;
                match_pos = copy_pos;
            }
        }

        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        ++res_offset;
    }

#if USE_VECTORSCAN
    static void processStringWithVectorscan(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        hs_database_t * db,
        hs_scratch_t * scratch,
        const Instructions & instructions,
        std::unordered_map<size_t, size_t> matches,
        std::vector<size_t> matches_from)
    {
        /// Vectorscan poses two challenges:
        /// 1. Unlike re2, matches are not guaranteed to be in order. E.g. on_match() may be called
        ///    sequentially for matches starting at positions 10, 6, 12, 3 etc. Therefore, we cannot
        ///    assemble the result string on the go as done in processStringWithRe2(). We instead
        ///    collect all matches in an ordered structure and assemble the result string afterwards.
        ///    As a sideeffect, replaceRegexpOne() looses its performance advantage over
        ///    replaceRegexpAll(). For simplicity, replace == ReplaceRegexpTraits::One is disabled.
        /// 2. While re2 does greedy matching (which it inherited from libpcre), vectorscan reports
        ///    *all* matches for a given position. So to emulate greedy matching, we retain only the
        ///    rightmost match for each position.

        auto on_match = [](unsigned int /*id*/,
                           unsigned long long from, // NOLINT(google-runtime-int)
                           unsigned long long to, // NOLINT(google-runtime-int)
                           unsigned int /*flags*/,
                           void * context) -> int
        {
            std::unordered_map<size_t, size_t> * ctx_matches = static_cast<std::unordered_map<size_t, size_t> *>(context);

            auto match = ctx_matches->find(from);
            if (match == ctx_matches->end())
                (*ctx_matches)[from] = to;
            else
                if (match->second < to)
                    (*ctx_matches)[from] = to;

            return 0; /// Continue matching
        };

        hs_error_t err = hs_scan(db, haystack_data, static_cast<unsigned>(haystack_length), 0, scratch, on_match, &matches);
        if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
            throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan with vectorscan");

        /// Sort matches
        matches_from.reserve(matches.size());
        for (auto [from, _] : matches)
            matches_from.push_back(from);
        std::sort(matches_from.begin(), matches_from.end());

        size_t match_pos = 0;

        for (size_t from : matches_from)
        {
            size_t to = matches[from];
            /// Copy prefix before current match without modification
            size_t bytes_to_copy = from - match_pos;
            res_data.resize(res_data.size() + bytes_to_copy);
            memcpy(&res_data[res_offset], haystack_data + match_pos, bytes_to_copy);
            res_offset += bytes_to_copy;

            /// Substitute inside current match using instructions
            for (const auto & instr : instructions)
            {
                std::string_view replacement;
                if (instr.substitution_num >= 0)
                    replacement = std::string_view(haystack_data + from, haystack_data + to); // capturing group \0
                else
                    replacement = instr.literal;
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
            }

            match_pos = to;
        }

        matches.clear();
        matches_from.clear();

        /// Copy suffix behind last match without modification
        if (match_pos != haystack_length)
        {
            size_t bytes_to_copy = haystack_length - match_pos;
            res_data.resize(res_data.size() + bytes_to_copy);
            memcpy(&res_data[res_offset], haystack_data + match_pos, bytes_to_copy);
            res_offset += bytes_to_copy;
        }

        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        ++res_offset;
    }
#endif

    struct MatchState
    {
#if USE_VECTORSCAN
        std::shared_mutex mutex;
        hs_database_t * db = nullptr;
        hs_scratch_t * scratch = nullptr;

        ~MatchState()
        {
            std::lock_guard lock(mutex);
            assert((!db && !scratch) || (db && !scratch) || (db && scratch)); // (!db && scratch) should be impossible
            if (scratch)
                hs_free_scratch(scratch);
            if (db)
                hs_free_database(db);
        }
#endif
    };

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        [[maybe_unused]] bool allow_hyperscan,
        [[maybe_unused]] MatchState & match_state)
    {
        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        re2_st::RE2::Options regexp_options;
        /// Don't write error messages to stderr.
        regexp_options.set_log_errors(false);

        re2_st::RE2 searcher(needle, regexp_options);

        if (!searcher.ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The pattern argument is not a valid re2 pattern: {}",
                searcher.error());

        int num_captures_in_needle = searcher.NumberOfCapturingGroups();

#if USE_VECTORSCAN
        /// 1. Vectorscan matches generally faster than re2. The caveat is that vectorscan recognizes
        /// capturing groups but ignores them, so we can use vectorscan only when the pattern has
        /// zero capturing groups.
        /// 2. Because evaluation in vectorscan with flag HS_FLAG_SOM_LEFTMOST may reject some complex
        /// but valid patterns according to documentation, we allow to turn vectorscan on/off via cfg.
        /// 3. Don't use vectorscan for replaceRegexpOne(), see processStringWithVectorscan() why.
        bool use_vectorscan = (num_captures_in_needle == 0) && allow_hyperscan && replace == ReplaceRegexpTraits::Replace::All;
        if (use_vectorscan)
        {
            // TODO
            /// Because we rely on the HS_FLAG_SOM_LEFTMOST flag here and other usages of vectorscan
            /// in ClickHouse don't, we can't use the internal regexp cache. Instead construct database
            /// and scratch area directly.

            std::shared_lock shared_lock(match_state.mutex);

            hs_error_t err;

            if (match_state.db == nullptr || match_state.scratch == nullptr)
            {
                shared_lock.unlock();
                {
                    std::lock_guard exclusive_lock(match_state.mutex);

                    if (match_state.db == nullptr)
                    {
                        hs_compile_error_t * compile_error = nullptr;
                        err = hs_compile(
                            needle.c_str(),
                            HS_FLAG_SOM_LEFTMOST | HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                            HS_MODE_BLOCK,
                            nullptr,
                            &match_state.db,
                            &compile_error);
                        if (err != HS_SUCCESS)
                        {
                            SCOPE_EXIT({ hs_free_compile_error(compile_error); });
                            throw Exception(
                                    ErrorCodes::LOGICAL_ERROR,
                                    compile_error->message);
                        }
                    }
                    if (match_state.scratch == nullptr)
                    {
                        err = hs_alloc_scratch(match_state.db, &match_state.scratch);
                        if (err != HS_SUCCESS)
                            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not allocate scratch space for vectorscan");
                    }
                }
                shared_lock.lock();
            }

            hs_scratch_t * local_scratch = nullptr;
            err = hs_clone_scratch(match_state.scratch, &local_scratch);
            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for vectorscan");
            SCOPE_EXIT({ hs_free_scratch(local_scratch); });

            Instructions instructions = createInstructions(replacement, num_captures_in_needle /* "\0" capture */ + 1);
            std::unordered_map<size_t, size_t> matches; // (from, to)
            std::vector<size_t> matches_from;

            for (size_t i = 0; i < size; ++i)
            {
                size_t from = i > 0 ? offsets[i - 1] : 0;
                const char * haystack_data = reinterpret_cast<const char *>(data.data() + from);
                const size_t haystack_length = static_cast<unsigned>(offsets[i] - from - 1);

                processStringWithVectorscan(haystack_data, haystack_length, res_data, res_offset, match_state.db, local_scratch, instructions, matches, matches_from);
                res_offsets[i] = res_offset;
            }

            return;
        }
#endif
        num_captures_in_needle = std::min(num_captures_in_needle /* "\0" capture */ + 1, max_captures_re2);
        Instructions instructions = createInstructions(replacement, num_captures_in_needle);

        for (size_t i = 0; i < size; ++i)
        {
            size_t from = i > 0 ? offsets[i - 1] : 0;
            const char * haystack_data = reinterpret_cast<const char *>(data.data() + from);
            const size_t haystack_length = static_cast<unsigned>(offsets[i] - from - 1);

            processStringWithRe2(haystack_data, haystack_length, res_data, res_offset, searcher, num_captures_in_needle, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        [[maybe_unused]] bool allow_hyperscan,
        [[maybe_unused]] MatchState & match_state)
    {
        ColumnString::Offset res_offset = 0;
        size_t size = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(size);

        re2_st::RE2::Options regexp_options;
        /// Don't write error messages to stderr.
        regexp_options.set_log_errors(false);

        re2_st::RE2 searcher(needle, regexp_options);

        if (!searcher.ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The pattern argument is not a valid re2 pattern: {}",
                searcher.error());

        int num_captures_in_needle = searcher.NumberOfCapturingGroups();

#if USE_VECTORSCAN
        /// 1. Vectorscan matches generally faster than re2. The caveat is that vectorscan recognizes
        /// capturing groups but ignores them, so we can use vectorscan only when the pattern has
        /// zero capturing groups.
        /// 2. Because evaluation in vectorscan with flag HS_FLAG_SOM_LEFTMOST may reject some complex
        /// but valid patterns according to documentation, we allow to turn vectorscan on/off via cfg.
        /// 3. Don't use vectorscan for replaceRegexpOne(), see processStringWithVectorscan() why.
        bool use_vectorscan = (num_captures_in_needle == 0) && allow_hyperscan && replace == ReplaceRegexpTraits::Replace::All;
        if (use_vectorscan)
        {
            // TODO
            /// Because we rely on the HS_FLAG_SOM_LEFTMOST flag here and other usages of vectorscan
            /// in ClickHouse don't, we can't use the internal regexp cache. Instead construct database
            /// and scratch area directly.

            std::shared_lock shared_lock(match_state.mutex);

            hs_error_t err;

            if (match_state.db == nullptr || match_state.scratch == nullptr)
            {
                shared_lock.unlock();
                {
                    std::lock_guard exclusive_lock(match_state.mutex);

                    if (match_state.db == nullptr)
                    {
                        hs_compile_error_t * compile_error = nullptr;
                        err = hs_compile(
                            needle.c_str(),
                            HS_FLAG_SOM_LEFTMOST | HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                            HS_MODE_BLOCK,
                            nullptr,
                            &match_state.db,
                            &compile_error);
                        if (err != HS_SUCCESS)
                        {
                            SCOPE_EXIT({ hs_free_compile_error(compile_error); });
                            throw Exception(
                                    ErrorCodes::LOGICAL_ERROR,
                                    compile_error->message);
                        }
                    }
                    if (match_state.scratch == nullptr)
                    {
                        err = hs_alloc_scratch(match_state.db, &match_state.scratch);
                        if (err != HS_SUCCESS)
                            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not allocate scratch space for vectorscan");
                    }
                }
                shared_lock.lock();
            }

            hs_scratch_t * local_scratch = nullptr;
            err = hs_clone_scratch(match_state.scratch, &local_scratch);
            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for vectorscan");
            SCOPE_EXIT({ hs_free_scratch(local_scratch); });

            Instructions instructions = createInstructions(replacement, num_captures_in_needle /* "\0" capture */ + 1);
            std::unordered_map<size_t, size_t> matches; // (from, to)
            std::vector<size_t> matches_from;

            for (size_t i = 0; i < size; ++i)
            {
                size_t from = i * n;
                const char * haystack_data = reinterpret_cast<const char *>(data.data() + from);
                const size_t haystack_length = n;

                processStringWithVectorscan(haystack_data, haystack_length, res_data, res_offset, match_state.db, local_scratch, instructions, matches, matches_from);
                res_offsets[i] = res_offset;
            }

            return;
        }
#endif

        num_captures_in_needle = std::min(num_captures_in_needle /* "\0" capture */ + 1, max_captures_re2);
        Instructions instructions = createInstructions(replacement, num_captures_in_needle);

        for (size_t i = 0; i < size; ++i)
        {
            size_t from = i * n;
            const char * haystack_data = reinterpret_cast<const char *>(data.data() + from);
            const size_t haystack_length = n;

            processStringWithRe2(haystack_data, haystack_length, res_data, res_offset, searcher, num_captures_in_needle, instructions);
            res_offsets[i] = res_offset;
        }
    }
};

}
