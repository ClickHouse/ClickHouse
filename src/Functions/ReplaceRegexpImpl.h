#pragma once

#include <Columns/ColumnString.h>
#include <Common/HashTable/HashMap.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/re2.h>
#include <Functions/Regexps.h>
#include <Functions/ReplaceStringImpl.h>
#include <Interpreters/JIT/CompileRegexp.h>
#include <base/types.h>

#include <functional>
#include <limits>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

enum class ReplaceRegexpTraits : uint8_t
{
    First,
    All
};

/// Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
/// 'replacement' can contain substitutions, for example: '\2-\3-\1'

/// Please note that it is not necessarily the canonical behavior.
/// Many programming languages, libraries, and databases disagree on how the global replacement function
/// should work in the presence of empty string matches, especially at the beginning or the end of the string:

/// $ perl -e 'my $x = "x"; $x =~ s/^|.*/Hello/g; print $x';
/// HelloHelloHello

/// $ php -r "echo preg_replace('/^|.*/', 'Hello', 'x');"
/// HelloHelloHello

/// $ python3 -c 'import re; print(re.sub(r"^|.*", "Hello", "x"))'
/// HelloHelloHello

/// $ node -e "console.log('x'.replace(/^|.*/g, 'Hello'))"
/// HelloxHello

/// $ ruby -e "puts 'x'.gsub(/^|.*/, 'Hello')"
/// HelloxHello

/// $ echo 'x' | sed -r -e 's/^|.*/Hello/g'
/// Hello

/// $ echo 'x' | ssed -r -e 's/^|.*/Hello/g'
/// HelloxHello

/// PostgreSQL 17: SELECT REGEXP_REPLACE('x', '^|.*', 'Hello')
/// Hello

template <typename Name, ReplaceRegexpTraits replace>
struct ReplaceRegexpImpl
{
    static constexpr auto name = Name::name;

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
    using Instructions = VectorWithMemoryTracking<Instruction>;

    static constexpr int max_captures = 10;

    static re2::RE2::Options createRegexpOptions()
    {
        re2::RE2::Options regexp_options;
        regexp_options.set_log_errors(false); /// don't write error messages to stderr
        regexp_options.set_dot_nl(true);
        return regexp_options;
    }

    /// The replacement string references must not contain non-existing capturing groups.
    static void checkSubstitutions(std::string_view replacement, int num_captures, CancellationBudget & budget)
    {
        for (size_t i = 0; i < replacement.size(); ++i)
        {
            budget.charge(1);
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// substitution
                {
                    int substitution_num = replacement[i + 1] - '0';
                    if (substitution_num >= num_captures)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Substitution '\\{}' in replacement argument is invalid, regexp has only {} capturing groups", substitution_num, num_captures - 1);
                }
            }
        }
    }

    static Instructions createInstructions(std::string_view replacement, int num_captures, CancellationBudget & budget)
    {
        checkSubstitutions(replacement, num_captures, budget);

        Instructions instructions;

        String literals;
        literals.reserve(replacement.size());

        for (size_t i = 0; i < replacement.size(); ++i)
        {
            budget.charge(1);
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// substitution
                {
                    if (!literals.empty())
                    {
                        instructions.emplace_back(literals);
                        literals = "";
                    }
                    int substitution_num = replacement[i + 1] - '0';
                    instructions.emplace_back(substitution_num);
                }
                else
                    literals += replacement[i + 1]; /// escaping
                ++i;
            }
            else
                literals += replacement[i]; /// plain character
        }

        if (!literals.empty())
            instructions.emplace_back(literals);

        return instructions;
    }

    /// `budget` is mandatory: this helper scans the whole replacement, and `analyze` the whole needle.
    static bool canFallbackToStringReplacement(
        const String & needle, const String & replacement, const re2::RE2 & searcher, int num_captures,
        CancellationBudget & budget)
    {
        if (searcher.NumberOfCapturingGroups())
            return false;

        checkSubstitutions(replacement, num_captures, budget);
        /// The needle's length is independent of the replacement charged above.
        budget.charge(needle.size());
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(needle);
        return result.is_trivial && result.required_substring_is_prefix && result.required_substring == needle;
    }

    /// Returns whether at least one match was found.
    static bool processString(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        const re2::RE2 & searcher,
        int num_captures,
        const Instructions & instructions,
        CancellationBudget & budget)
    {
        std::string_view haystack(haystack_data, haystack_length);
        std::string_view matches[max_captures];

        bool found_match = false;
        size_t copy_pos = 0;
        size_t match_pos = 0;

        /// It's possible to find empty match at the end of the string (e.g, '$' matches even an empty string), so non-strict comparison.
        while (match_pos <= haystack_length)
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(haystack, match_pos, haystack_length, re2::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                found_match = true;

                const auto & match = matches[0]; /// Complete match
                size_t bytes_to_copy = (match.data() - haystack.data()) - copy_pos;

                /// Copy prefix before current match without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                copy_pos += bytes_to_copy + match.length();
                match_pos = copy_pos;

                /// Substitute inside current match using instructions
                /// Charged from INSIDE the loop: the list runs in full for every match and is itself
                /// unbounded, so a single match can carry the whole list. Flushed in chunks.
                size_t units_since_charge = 0;
                for (const auto & instr : instructions)
                {
                    std::string_view replacement;
                    if (instr.substitution_num >= 0)
                        replacement = matches[instr.substitution_num];
                    else
                        replacement = instr.literal;
                    res_data.resize(res_data.size() + replacement.size());
                    /// re2 reports a capturing group that did not participate in the match as a null
                    /// string_view, and passing that to memcpy is undefined behavior even for a zero size.
                    if (!replacement.empty())
                        memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                    res_offset += replacement.size();
                    units_since_charge += 1 + replacement.size() / CancellationBudget::bytes_per_unit;
                    if (units_since_charge >= CancellationBudget::units_per_instruction_charge)
                    {
                        budget.chargeUnits(units_since_charge);
                        units_since_charge = 0;
                    }
                }

                /// This iteration, whatever the loop has not flushed, plus the prefix bytes copied.
                budget.chargeUnits(1 + units_since_charge + bytes_to_copy / CancellationBudget::bytes_per_unit);

                if constexpr (replace == ReplaceRegexpTraits::First)
                    can_finish_current_string = true;

                if (match.empty())
                {
                    /// Step one character to avoid infinite loop
                    ++match_pos;
                    if (match_pos > haystack_length)
                        can_finish_current_string = true;
                }
                else if (instructions.empty() && match_pos == haystack_length)
                {
                    /// Optimization: if we are already at the end of the string, and the replacement is an empty string,
                    /// then we can't do anything other than replacing an empty match with an empty string,
                    /// so we can skip it.
                    can_finish_current_string = true;
                }
            }
            else
            {
                can_finish_current_string = true;
                budget.charge();
            }

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + haystack_length - copy_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, haystack_length - copy_pos);
                res_offset += haystack_length - copy_pos;
                budget.charge(haystack_length - copy_pos);
                copy_pos = haystack_length;
                match_pos = copy_pos;
                break;
            }
        }

        return found_match;
    }

    /// `processString` for a JIT-compiled matcher (see `CompileRegexp.h`). Mirrors the loop above,
    /// but uses the native matcher to find matches and capture pointers for `\N` substitutions.
    /// `capture_starts`/`capture_ends` are scratch arrays of at least `matcher.num_captures` elements.
    static void processStringJIT(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        const RegexpJITMatcher & matcher,
        const uint8_t ** capture_starts,
        const uint8_t ** capture_ends,
        const Instructions & instructions,
        CancellationBudget & budget)
    {
        const auto * begin = reinterpret_cast<const uint8_t *>(haystack_data);
        const auto * end = begin + haystack_length;

        size_t copy_pos = 0;
        size_t match_pos = 0;

        while (match_pos <= haystack_length)
        {
            bool can_finish_current_string = false;

            if (matcher.func(begin, end, begin + match_pos, capture_starts, capture_ends) == 1)
            {
                const size_t match_start = capture_starts[0] - begin;
                const size_t match_end = capture_ends[0] - begin;
                const size_t match_length = match_end - match_start;

                const size_t bytes_to_copy = match_start - copy_pos;
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack_data + copy_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                copy_pos += bytes_to_copy + match_length;
                match_pos = copy_pos;

                /// Charged from inside the loop in chunks, see `processString`.
                size_t units_since_charge = 0;
                for (const auto & instr : instructions)
                {
                    std::string_view replacement;
                    if (instr.substitution_num >= 0)
                    {
                        const uint8_t * s = capture_starts[instr.substitution_num];
                        const uint8_t * e = capture_ends[instr.substitution_num];
                        if (s != nullptr && e != nullptr)
                            replacement = std::string_view(reinterpret_cast<const char *>(s), e - s);
                    }
                    else
                        replacement = instr.literal;

                    res_data.resize(res_data.size() + replacement.size());
                    if (!replacement.empty())
                        memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                    res_offset += replacement.size();
                    units_since_charge += 1 + replacement.size() / CancellationBudget::bytes_per_unit;
                    if (units_since_charge >= CancellationBudget::units_per_instruction_charge)
                    {
                        budget.chargeUnits(units_since_charge);
                        units_since_charge = 0;
                    }
                }

                /// See `processString`.
                budget.chargeUnits(1 + units_since_charge + bytes_to_copy / CancellationBudget::bytes_per_unit);

                if constexpr (replace == ReplaceRegexpTraits::First)
                    can_finish_current_string = true;

                if (match_length == 0)
                {
                    /// Step one character to avoid infinite loop
                    ++match_pos;
                    if (match_pos > haystack_length)
                        can_finish_current_string = true;
                }
                else if (instructions.empty() && match_pos == haystack_length)
                {
                    can_finish_current_string = true;
                }
            }
            else
            {
                can_finish_current_string = true;
                budget.charge();
            }

            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + haystack_length - copy_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack_data + copy_pos, haystack_length - copy_pos);
                res_offset += haystack_length - copy_pos;
                budget.charge(haystack_length - copy_pos);
                copy_pos = haystack_length;
                match_pos = copy_pos;
                break;
            }
        }
    }

    /// Haystacks are often repetitive (e.g. URLs), so run the regexp once per distinct value and copy the
    /// cached result for repeats. Only worth it for RE2 matches: a JIT-compiled match is about as cheap as
    /// the hash table probe it would save, which is why the JIT loop processes every row directly.
    /// A row without a match comes out identical to its input, so caching it saves only the match attempt,
    /// and for short-circuiting patterns such as '^foo' that attempt is cheaper than hashing the haystack.
    /// Once a block proves to be almost entirely non-matching, a capture-free existence check therefore runs
    /// first and rejected rows are copied through directly, at the cost of the plain loop. While the cache
    /// is enabled, rejects are cached like matches, so repeats of a rejecting value skip the re-check; only
    /// after the distinct-ratio guard disables the cache do rejected rows bypass hashing entirely.
    /// `get_haystack(i)` must stay valid for the whole call.
    template <typename GetHaystack>
    static void processStringsDeduplicated(
        GetHaystack && get_haystack,
        size_t input_rows_count,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnString::Offset & res_offset,
        const re2::RE2 & searcher,
        int num_captures,
        const Instructions & instructions,
        CancellationBudget & budget)
    {
        struct CachedResult
        {
            UInt64 start;
            UInt64 length;
            bool matched;
        };

        HashMap<std::string_view, CachedResult> results_cache;
        bool map_enabled = true;
        /// The heuristics are re-evaluated once per window of rows. The window is small so that even a
        /// block far shorter than the default `max_block_size` samples its match ratio, and so that a
        /// wrong decision is corrected within a bounded number of rows.
        static constexpr size_t ratio_check_window = 32;
        /// The distinct ratio needs a longer sample: values that repeat with a longer cycle would be
        /// written off as distinct before the first repeat can arrive.
        static constexpr size_t distinct_ratio_window = 256;
        static_assert(distinct_ratio_window % ratio_check_window == 0);
        /// The number of rows following a sampled haystack that are compared against it, and so the
        /// longest cycle of values the sample can catch.
        static constexpr size_t sample_probe_rows = 32;
        static_assert(sample_probe_rows < distinct_ratio_window);

        bool precheck_non_matching = false;
        size_t rows_in_window = 0;
        size_t matched_in_window = 0;
        size_t cache_size_at_distinct_check = 0;

        std::string_view prev_haystack;
        bool has_prev_haystack = false;
        CachedResult prev_result{0, 0, false};

        /// While the cache is off, one haystack per distinct-ratio window is kept as a sample and the
        /// cache comes back on the spot as soon as a row equal to it arrives, so that a block whose
        /// remainder turns repetitive after a mostly-distinct window is not stuck on the plain path for
        /// the rest of the block. Only the rows right after the sample are compared against it: a compare
        /// of distinct values runs to the first differing byte, which for equal-length values sharing a
        /// long prefix (URLs that differ only in the tail) is about the cost of the copy every row pays
        /// anyway, so comparing every row would slow the mostly-distinct blocks the guard exists to
        /// protect. Comparing the consecutive rows that follow the sample rather than one row per ratio
        /// window catches every cycle of up to `sample_probe_rows` values regardless of its length: probes
        /// spaced at a fixed stride only ever land on the cycle positions the stride happens to reach (a
        /// stride of 32 never meets a cycle of 9). An all-distinct block never re-enables the cache at
        /// all, and a value that recurs only after more rows than the probes cover escapes the sample
        /// just as a value that recurs after more than a window escapes the distinct ratio.
        std::string_view sample_haystack;

        /// `haystack_bytes_read` is the size of the haystack the compare or lookup scanned to find the
        /// entry. Charging only the copied result would let a run of repeated multi-megabyte haystacks
        /// with a tiny cached result advance the budget by little more than the per-row unit.
        auto copy_cached = [&](const CachedResult & cached, size_t haystack_bytes_read)
        {
            res_data.resize(res_data.size() + cached.length);
            /// Plain memcpy: the gap to the source region can be smaller than the 15 bytes of
            /// slack that memcpySmallAllowReadWriteOverflow15 requires.
            if (cached.length)
                memcpy(&res_data[res_offset], &res_data[cached.start], cached.length);
            res_offset += cached.length;
            budget.chargeUnits((haystack_bytes_read + cached.length) / CancellationBudget::bytes_per_unit);
        };

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();

            const std::string_view haystack = get_haystack(i);

            /// The checkpoint runs ahead of every fast path below, so every row advances it and no
            /// `continue` can starve it: a block of pre-check rejects, of adjacent duplicates, or of
            /// cache-disabled rows all keep re-evaluating the heuristics on schedule. The match ratio
            /// covers only the last window, so the pre-check follows a shift between rejecting and
            /// matching runs within at most two windows regardless of the block's shape.
            if (rows_in_window == ratio_check_window)
            {
                precheck_non_matching = matched_in_window * 20 < ratio_check_window;
                if (i % distinct_ratio_window == 0)
                {
                    /// While the cache is enabled every row either reaches it or is an adjacent duplicate
                    /// (a repeat the caching strategy serves), so the insertions since the previous check
                    /// count the distinct rows of the last window. The ratio covers only that window rather
                    /// than the whole prefix: a repetitive prefix must not keep the cache on for a
                    /// mostly-distinct remainder of the block, where the lookups would pay for nothing.
                    if (map_enabled)
                    {
                        const size_t distinct_in_window = results_cache.size() - cache_size_at_distinct_check;
                        if (distinct_in_window * 10 > distinct_ratio_window * 9)
                        {
                            map_enabled = false;
                            /// Not `clearAndShrink`: that frees the buffer and leaves the table unusable,
                            /// while the sample below may bring the cache back later in the block.
                            results_cache.clear();
                        }
                        cache_size_at_distinct_check = results_cache.size();
                    }

                    if (!map_enabled)
                        sample_haystack = haystack;
                }
                rows_in_window = 0;
                matched_in_window = 0;
            }
            ++rows_in_window;

            /// The sample is taken on the first row of a distinct-ratio window, so the probed rows are
            /// the ones right after it and the sampled row never counts as a repeat of itself.
            /// The recurrence proves the rows are not all-distinct, so the cache is rebuilt from this row
            /// on rather than at the next distinct-ratio boundary: a block shorter than a boundary apart
            /// would otherwise end before the recurrence could be acted on. The distinct ratio of the
            /// window the rebuilt cache runs into judges it again, so a wrong re-enable costs a window of
            /// lookups, while a cycle of a few values keeps the cache for good.
            const size_t offset_in_distinct_window = i % distinct_ratio_window;
            if (!map_enabled && offset_in_distinct_window != 0 && offset_in_distinct_window <= sample_probe_rows
                && haystack == sample_haystack)
                map_enabled = true;

            const UInt64 result_start = res_offset;
            bool row_matched = false;

            if (map_enabled)
            {
                /// Ahead of the lookup: an adjacent duplicate is served by a compare against a value that
                /// is still cache-hot, which costs about as much as the copy both paths pay, while hashing
                /// the haystack reads all of it again. A distinct row loses only a compare that exits at
                /// the first differing byte (or at the length check), and while the cache is enabled the
                /// window is repetitive enough for the hits to pay for those misses.
                if (has_prev_haystack && haystack == prev_haystack)
                {
                    copy_cached(prev_result, haystack.size());
                    matched_in_window += prev_result.matched;
                    res_offsets[i] = res_offset;
                    continue;
                }

                typename HashMap<std::string_view, CachedResult>::LookupResult it;
                bool inserted = false;
                results_cache.emplace(haystack, it, inserted);
                if (!inserted)
                {
                    copy_cached(it->getMapped(), haystack.size());
                    row_matched = it->getMapped().matched;
                }
                else
                {
                    /// Hashing the key read the whole haystack before any of the paths below charge for it.
                    budget.chargeUnits(haystack.size() / CancellationBudget::bytes_per_unit);
                    /// A rejected row is cached exactly like a processed one: it produces the same
                    /// output either way, and caching it is what spares its later repeats the reject.
                    /// The reject runs here rather than ahead of the lookup so that non-adjacent repeats
                    /// of a rejecting value are served by the cache instead of re-running a reject that
                    /// may have to scan the whole haystack.
                    /// The check scans the haystack whichever way it goes, and a hit then scans it a
                    /// second time inside `processString`, which charges only what it copies. Charged
                    /// ahead of the outcome so that a matching row does not advance the budget by less
                    /// than a rejected one and postpone the cancellation checkpoint by a whole scan.
                    bool precheck_rejected = false;
                    if (precheck_non_matching)
                    {
                        budget.charge(haystack.size());
                        precheck_rejected
                            = !searcher.Match(haystack, 0, haystack.size(), re2::RE2::Anchor::UNANCHORED, nullptr, 0);
                    }

                    if (precheck_rejected)
                    {
                        res_data.resize(res_data.size() + haystack.size());
                        memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data(), haystack.size());
                        res_offset += haystack.size();
                        budget.charge();
                        row_matched = false;
                    }
                    else
                        row_matched = processString(haystack.data(), haystack.size(), res_data, res_offset, searcher, num_captures, instructions, budget);
                    it->getMapped() = {result_start, res_offset - result_start, row_matched};
                }
            }
            else
            {
                /// No per-row compare against the previous value here: the cache is off because the
                /// window was mostly distinct, so an adjacent duplicate is rare while every distinct row
                /// would pay the compare, and the sample probe above brings the cache (and with it the
                /// adjacent-duplicate compare) back once the rows turn repetitive. A rejected row plus
                /// the copy is exactly the plain loop's cost. It is non-matching by definition, so
                /// leaving `matched_in_window` untouched counts it correctly.
                /// The attempt is charged ahead of its outcome, as on the cached path above.
                bool precheck_rejected = false;
                if (precheck_non_matching)
                {
                    budget.charge(haystack.size());
                    precheck_rejected
                        = !searcher.Match(haystack, 0, haystack.size(), re2::RE2::Anchor::UNANCHORED, nullptr, 0);
                }

                if (precheck_rejected)
                {
                    res_data.resize(res_data.size() + haystack.size());
                    memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data(), haystack.size());
                    res_offset += haystack.size();
                    budget.charge();
                }
                else
                    row_matched = processString(haystack.data(), haystack.size(), res_data, res_offset, searcher, num_captures, instructions, budget);
            }

            matched_in_window += row_matched;
            prev_haystack = haystack;
            has_prev_haystack = true;
            prev_result = {result_start, res_offset - result_start, row_matched};
            res_offsets[i] = res_offset;
        }
    }

    static void vectorConstantConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        size_t regexp_jit_min_count = std::numeric_limits<size_t>::max(),
        const std::function<void()> & check_cancellation = {})
    {
        CancellationBudget budget(check_cancellation);

        if (needle.empty())
        {
            res_data.assign(haystack_data);
            res_offsets.assign(haystack_offsets);
            return;
        }

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        auto regexp_options = createRegexpOptions();

        re2::RE2 searcher(needle, regexp_options);
        if (!searcher.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        /// Try to use non-regexp string replacement. This shortcut is implemented only for const-needles + const-replacement as
        /// pattern analysis incurs some cost too.
        if (canFallbackToStringReplacement(needle, replacement, searcher, num_captures, budget))
        {
            auto convert_trait = [](ReplaceRegexpTraits first_or_all)
            {
                switch (first_or_all)
                {
                    case ReplaceRegexpTraits::First: return ReplaceStringTraits::Replace::First;
                    case ReplaceRegexpTraits::All:   return ReplaceStringTraits::Replace::All;
                }
            };
            /// The delegated call starts its own budget, which covers the traversal it performs.
            ReplaceStringImpl<Name, convert_trait(replace)>::vectorConstantConstant(
                haystack_data, haystack_offsets, needle, replacement, res_data, res_offsets, input_rows_count,
                check_cancellation);
            return;
        }

        Instructions instructions = createInstructions(replacement, num_captures, budget);

        /// `replace` builds RE2 with `dot_nl` enabled (see `createRegexpOptions`), so `.` matches newline (dot_all = true).
        RegexpJITMatcher matcher = getRegexpJITMatcher(needle, /* case_insensitive */ false, /* dot_all */ true, regexp_jit_min_count);
        /// Allocated once per call (not per row); reused by `processStringJIT` for every row.
        VectorWithMemoryTracking<const uint8_t *> capture_starts;
        VectorWithMemoryTracking<const uint8_t *> capture_ends;
        if (matcher)
        {
            const size_t n = std::max<size_t>(matcher.num_captures, num_captures);
            capture_starts.resize(n);
            capture_ends.resize(n);
        }

        if (matcher)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                budget.charge();

                size_t from = haystack_offsets[i - 1];

                const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + from);
                const size_t hs_length = static_cast<size_t>(haystack_offsets[i] - from);

                processStringJIT(hs_data, hs_length, res_data, res_offset, matcher, capture_starts.data(), capture_ends.data(), instructions, budget);
                res_offsets[i] = res_offset;
            }
            return;
        }

        processStringsDeduplicated(
            [&](size_t i)
            {
                const size_t from = haystack_offsets[i - 1];
                return std::string_view(
                    reinterpret_cast<const char *>(haystack_data.data() + from),
                    static_cast<size_t>(haystack_offsets[i] - from));
            },
            input_rows_count, res_data, res_offsets, res_offset, searcher, num_captures, instructions, budget);
    }

    static void vectorVectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        const std::function<void()> & check_cancellation = {})
    {
        chassert(haystack_offsets.size() == needle_offsets.size());

        CancellationBudget budget(check_cancellation);

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        auto regexp_options = createRegexpOptions();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();

            size_t hs_from = haystack_offsets[i - 1];
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + hs_from);
            const size_t hs_length = static_cast<size_t>(haystack_offsets[i] - hs_from);

            size_t ndl_from = needle_offsets[i - 1];
            const char * ndl_data = reinterpret_cast<const char *>(needle_data.data() + ndl_from);
            const size_t ndl_length = static_cast<size_t>(needle_offsets[i] - ndl_from);
            std::string_view needle(ndl_data, ndl_length);

            if (needle.empty())
            {
                res_data.insert(res_data.end(), hs_data, hs_data + hs_length);
                res_offset += hs_length;
                res_offsets[i] = res_offset;
                /// This branch skips every other checkpoint, so the copied row must be charged here.
                budget.charge(hs_length);
                continue;
            }

            /// Matcher and instruction list are rebuilt per row, and scale with the pattern.
            budget.charge(needle.size());

            re2::RE2 searcher(needle, regexp_options);
            if (!searcher.ok())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

            int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);
            Instructions instructions = createInstructions(replacement, num_captures, budget);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions, budget);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorConstantVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const ColumnString::Chars & replacement_data,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        const std::function<void()> & check_cancellation = {})
    {
        chassert(haystack_offsets.size() == replacement_offsets.size());

        CancellationBudget budget(check_cancellation);

        if (needle.empty())
        {
            res_data.assign(haystack_data);
            res_offsets.assign(haystack_offsets);
            return;
        }

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        auto regexp_options = createRegexpOptions();

        re2::RE2 searcher(needle, regexp_options);
        if (!searcher.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();

            size_t hs_from = haystack_offsets[i - 1];
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + hs_from);
            const size_t hs_length = static_cast<size_t>(haystack_offsets[i] - hs_from);

            size_t repl_from = replacement_offsets[i - 1];
            const char * repl_data = reinterpret_cast<const char *>(replacement_data.data() + repl_from);
            const size_t repl_length = static_cast<size_t>(replacement_offsets[i] - repl_from);
            std::string_view replacement(repl_data, repl_length);

            /// The instruction list is rebuilt for every row from that row's replacement.
            Instructions instructions = createInstructions(replacement, num_captures, budget);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions, budget);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorVectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnString::Chars & replacement_data,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        const std::function<void()> & check_cancellation = {})
    {
        chassert(haystack_offsets.size() == needle_offsets.size());
        chassert(needle_offsets.size() == replacement_offsets.size());

        CancellationBudget budget(check_cancellation);

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        auto regexp_options = createRegexpOptions();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();

            size_t hs_from = haystack_offsets[i - 1];
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + hs_from);
            const size_t hs_length = static_cast<size_t>(haystack_offsets[i] - hs_from);

            size_t ndl_from = needle_offsets[i - 1];
            const char * ndl_data = reinterpret_cast<const char *>(needle_data.data() + ndl_from);
            const size_t ndl_length = static_cast<size_t>(needle_offsets[i] - ndl_from);
            std::string_view needle(ndl_data, ndl_length);

            if (needle.empty())
            {
                res_data.insert(res_data.end(), hs_data, hs_data + hs_length);
                res_offsets[i] = res_offsets[i - 1] + hs_length;
                res_offset = res_offsets[i];
                /// This branch skips every other checkpoint, so the copied row must be charged here.
                budget.charge(hs_length);
                continue;
            }

            size_t repl_from = replacement_offsets[i - 1];
            const char * repl_data = reinterpret_cast<const char *>(replacement_data.data() + repl_from);
            const size_t repl_length = static_cast<size_t>(replacement_offsets[i] - repl_from);
            std::string_view replacement(repl_data, repl_length);

            /// Per-row matcher construction, its cost scales with the pattern rather than the haystack.
            budget.charge(needle.size());

            re2::RE2 searcher(needle, regexp_options);
            if (!searcher.ok())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

            int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);
            Instructions instructions = createInstructions(replacement, num_captures, budget);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions, budget);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixedConstantConstant(
        const ColumnString::Chars & haystack_data,
        size_t n,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        const std::function<void()> & check_cancellation = {})
    {
        CancellationBudget budget(check_cancellation);

        if (needle.empty())
        {
            chassert(input_rows_count == haystack_data.size() / n);
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.resize(input_rows_count);
            /// Per-row loop over an unbounded row count, see the same charge in `ReplaceStringImpl`.
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                res_offsets[i] = (i + 1) * n;
                budget.charge();
            }
            return;
        }

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        auto regexp_options = createRegexpOptions();

        re2::RE2 searcher(needle, regexp_options);
        if (!searcher.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pattern argument is not a valid re2 pattern: {}", searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);
        Instructions instructions = createInstructions(replacement, num_captures, budget);

        processStringsDeduplicated(
            [&](size_t i) { return std::string_view(reinterpret_cast<const char *>(haystack_data.data() + i * n), n); },
            input_rows_count, res_data, res_offsets, res_offset, searcher, num_captures, instructions, budget);
    }
};

}
