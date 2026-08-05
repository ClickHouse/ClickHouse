#pragma once

#include <base/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>

#include <functional>


namespace DB
{

struct ReplaceStringTraits
{
    enum class Replace : uint8_t
    {
        First,
        All
    };
};

/// Throttled cancellation checkpoint for the `replace*` functions. Lives on the stack of a single vector
/// call, so it is never shared between threads. Counted in loop ITERATIONS, not bytes: an empty regexp
/// match advances one byte and may copy nothing, yet costs a full `RE2::Match`. Bytes are charged on top,
/// scaled down, because one iteration can copy megabytes.
struct ReplaceCancellationBudget
{
    /// About 30ms of granularity on the cheapest loop.
    static constexpr size_t units_per_check = 1ULL << 16;
    /// Bytes of data read or written that count as one unit of work.
    static constexpr size_t bytes_per_unit = 16;
    /// Accumulated inside a substitution loop before being charged, so the loop body stays short.
    static constexpr size_t units_per_instruction_charge = 4096;

    explicit ReplaceCancellationBudget(const std::function<void()> & check_)
        : check(check_ ? &check_ : nullptr) {}

    /// One iteration of an unbounded loop, plus the bytes of data that iteration touched.
    void charge(size_t bytes = 0) { chargeUnits(1 + bytes / bytes_per_unit); }

    /// Work that is not proportional to the amount of data, e.g. constructing a matcher from a pattern.
    void chargeUnits(size_t units)
    {
        if (units_left > units)
        {
            units_left -= units;
            return;
        }
        units_left = units_per_check;
        if (check)
            (*check)();
    }

private:
    const std::function<void()> * check;
    size_t units_left = units_per_check;
};

/** Replace one or all occurencies of substring 'needle' to 'replacement'.
  */
template <typename Name, ReplaceStringTraits::Replace replace>
struct ReplaceStringImpl
{
    static constexpr auto name = Name::name;

    static void vectorConstantConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        const std::function<void()> & check_cancellation = {})
    {
        ReplaceCancellationBudget budget(check_cancellation);

        if (needle.empty())
        {
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.assign(haystack_offsets.begin(), haystack_offsets.end());
            return;
        }

        /// Replacing the needle with itself is a no-op for any needle length and both modes, so
        /// copy the column verbatim and skip the search.
        if (needle == replacement)
        {
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.assign(haystack_offsets.begin(), haystack_offsets.end());
            return;
        }

        /// One-byte needle and one-byte replacement in "replace all" mode: every match keeps the
        /// string layout, so offsets are unchanged and we can copy the buffer once and flip matching
        /// bytes in place. Row boundaries are defined by offsets (not by in-band terminators), which
        /// we copy verbatim, so any needle byte is safe here.
        if constexpr (replace == ReplaceStringTraits::Replace::All)
        {
            if (needle.size() == 1 && replacement.size() == 1)
            {
                res_data.assign(haystack_data.begin(), haystack_data.end());
                res_offsets.assign(haystack_offsets.begin(), haystack_offsets.end());
                const auto from = static_cast<UInt8>(needle[0]);
                const auto to = static_cast<UInt8>(replacement[0]);
                /// Chunked so the time limit is observed inside this single whole-column loop. Chunking
                /// measurably perturbs how the scan is compiled, so the chunk is kept large.
                static constexpr size_t chunk_size = 64 * 1024 * 1024;
                UInt8 * const data = res_data.data();
                const size_t size = res_data.size();
                for (size_t chunk_begin = 0; chunk_begin < size; chunk_begin += chunk_size)
                {
                    const size_t chunk_end = std::min(chunk_begin + chunk_size, size);
                    for (UInt8 * p = data + chunk_begin, * const chunk_last = data + chunk_end; p != chunk_last; ++p)
                        if (*p == from)
                            *p = to;
                    budget.charge(chunk_end - chunk_begin);
                }
                return;
            }
        }

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        /// The current index in the column of strings.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (i < input_rows_count)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy the data before the match without changing
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);

            budget.charge(match - pos);

            /// Determine which index the match belongs to.
            while (i < input_rows_count && begin + haystack_offsets[i] <= match)
            {
                res_offsets[i] = res_offset + ((begin + haystack_offsets[i]) - pos);
                ++i;
                budget.charge();
            }
            res_offset += (match - pos);

            /// If we have reached the end, it's time to stop
            if (i == input_rows_count)
                break;

            /// Is it true that this string no longer needs to perform transformations.
            bool can_finish_current_string = false;

            /// We check that the entry does not go through the boundaries of strings.
            if (match + needle.size() <= begin + haystack_offsets[i])
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                /// The output is work too: a large replacement writes far more than it reads.
                budget.charge(replacement.size());
                pos = match + needle.size();
                if constexpr (replace == ReplaceStringTraits::Replace::First)
                    can_finish_current_string = true;
                else if (pos == begin + haystack_offsets[i])
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                const size_t rest_of_string = begin + haystack_offsets[i] - pos;
                res_data.resize(res_data.size() + rest_of_string);
                memcpy(&res_data[res_offset], pos, rest_of_string);
                res_offset += rest_of_string;
                res_offsets[i] = res_offset;
                pos = begin + haystack_offsets[i];
                ++i;
                /// Unconditional, and the whole rest of the row.
                budget.charge(rest_of_string);
            }
        }
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    static void copyToOutput(
        const CharT * what_start, size_t what_size,
        ColumnString::Chars & output, ColumnString::Offset & output_offset)
    {
        output.resize(output.size() + what_size);
        memcpy(&output[output_offset], what_start, what_size);
        output_offset += what_size;
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

        ReplaceCancellationBudget budget(check_cancellation);

        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset res_offset = 0;

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();
            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset;

            const auto * const cur_needle_data = &needle_data[prev_needle_offset];
            const size_t cur_needle_length = needle_offsets[i] - prev_needle_offset;

            const auto * last_match = static_cast<UInt8 *>(nullptr);
            const auto * start_pos = cur_haystack_data;
            const auto * const cur_haystack_end = cur_haystack_data + cur_haystack_length;

            if (cur_needle_length)
            {
                /// Cheap-to-initialize searcher instead of Volnitsky because there is a different pattern in each row
                CaseSensitiveStringSearcher searcher(cur_needle_data, cur_needle_length);

                while (start_pos < cur_haystack_end)
                {
                    if (const auto * const match = searcher.search(start_pos, cur_haystack_end); match != cur_haystack_end)
                    {
                        /// Copy prefix before match
                        copyToOutput(start_pos, match - start_pos, res_data, res_offset);

                        /// Insert replacement for match
                        copyToOutput(replacement.data(), replacement.size(), res_data, res_offset);

                        budget.charge((match - start_pos) + replacement.size());

                        last_match = match;
                        start_pos = match + cur_needle_length;

                        if constexpr (replace == ReplaceStringTraits::Replace::First)
                            break;
                    }
                    else
                        break;
                }
            }

            /// Copy suffix after last match
            size_t bytes = (last_match == nullptr) ? (cur_haystack_end - cur_haystack_data)
                                                   : (cur_haystack_end - last_match - cur_needle_length);
            copyToOutput(start_pos, bytes, res_data, res_offset);
            /// Unconditional, and the whole row when it never matches.
            budget.charge(bytes);

            res_offsets[i] = res_offset;

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
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

        ReplaceCancellationBudget budget(check_cancellation);

        if (needle.empty())
        {
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.assign(haystack_offsets.begin(), haystack_offsets.end());
            return;
        }

        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset res_offset = 0;

        size_t prev_haystack_offset = 0;
        size_t prev_replacement_offset = 0;

        CaseSensitiveStringSearcher searcher(needle.data(), needle.size());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();

            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset;

            const auto * const cur_replacement_data = &replacement_data[prev_replacement_offset];
            const size_t cur_replacement_length = replacement_offsets[i] - prev_replacement_offset;

            const auto * last_match = static_cast<UInt8 *>(nullptr);
            const auto * start_pos = cur_haystack_data;
            const auto * const cur_haystack_end = cur_haystack_data + cur_haystack_length;

            while (start_pos < cur_haystack_end)
            {
                if (const auto * const match = searcher.search(start_pos, cur_haystack_end); match != cur_haystack_end)
                {
                    /// Copy prefix before match
                    copyToOutput(start_pos, match - start_pos, res_data, res_offset);

                    /// Insert replacement for match
                    copyToOutput(cur_replacement_data, cur_replacement_length, res_data, res_offset);

                    budget.charge((match - start_pos) + cur_replacement_length);

                    last_match = match;
                    start_pos = match + needle.size();

                    if constexpr (replace == ReplaceStringTraits::Replace::First)
                        break;
                }
                else
                    break;
            }

            /// Copy suffix after last match
            size_t bytes = (last_match == nullptr) ? (cur_haystack_end - cur_haystack_data)
                                                   : (cur_haystack_end - last_match - needle.size());
            copyToOutput(start_pos, bytes, res_data, res_offset);
            /// See `vectorVectorConstant`: unconditional, and the whole row when it never matches.
            budget.charge(bytes);

            res_offsets[i] = res_offset;

            prev_haystack_offset = haystack_offsets[i];
            prev_replacement_offset = replacement_offsets[i];
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

        ReplaceCancellationBudget budget(check_cancellation);

        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset res_offset = 0;

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;
        size_t prev_replacement_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            budget.charge();

            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset;

            const auto * const cur_needle_data = &needle_data[prev_needle_offset];
            const size_t cur_needle_length = needle_offsets[i] - prev_needle_offset;

            const auto * const cur_replacement_data = &replacement_data[prev_replacement_offset];
            const size_t cur_replacement_length = replacement_offsets[i] - prev_replacement_offset;

            const auto * last_match = static_cast<UInt8 *>(nullptr);
            const auto * start_pos = cur_haystack_data;
            const auto * const cur_haystack_end = cur_haystack_data + cur_haystack_length;

            if (cur_needle_length)
            {
                /// Cheap-to-initialize searcher instead of Volnitsky because there is a different pattern in each row
                CaseSensitiveStringSearcher searcher(cur_needle_data, cur_needle_length);

                while (start_pos < cur_haystack_end)
                {
                    if (const auto * const match = searcher.search(start_pos, cur_haystack_end); match != cur_haystack_end)
                    {
                        /// Copy prefix before match
                        copyToOutput(start_pos, match - start_pos, res_data, res_offset);

                        /// Insert replacement for match
                        copyToOutput(cur_replacement_data, cur_replacement_length, res_data, res_offset);

                        budget.charge((match - start_pos) + cur_replacement_length);

                        last_match = match;
                        start_pos = match + cur_needle_length;

                        if constexpr (replace == ReplaceStringTraits::Replace::First)
                            break;
                    }
                    else
                        break;
                }
            }
            /// Copy suffix after last match
            size_t bytes = (last_match == nullptr) ? (cur_haystack_end - cur_haystack_data)
                                                   : (cur_haystack_end - last_match - cur_needle_length);
            copyToOutput(start_pos, bytes, res_data, res_offset);
            /// See `vectorVectorConstant`: unconditional, and the whole row when it never matches.
            budget.charge(bytes);

            res_offsets[i] = res_offset;

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
            prev_replacement_offset = replacement_offsets[i];
        }
    }

    /// Note: this function converts fixed-length strings to variable-length strings
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
        ReplaceCancellationBudget budget(check_cancellation);

        if (needle.empty() || needle == replacement)
        {
            chassert(input_rows_count == haystack_data.size() / n);
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.resize(input_rows_count);
            /// The copy above is one bulk operation, but filling the offsets is an unbounded per-row loop.
            for (size_t i = 1; i <= input_rows_count; ++i)
            {
                res_offsets[i - 1] = i * n;
                budget.charge();
            }
            return;
        }

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (i < input_rows_count)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            budget.charge(match - pos);

#define COPY_REST_OF_CURRENT_STRING() \
    do \
    { \
        const size_t len = begin + n * (i + 1) - pos; \
        res_data.resize(res_data.size() + len); \
        memcpy(&res_data[res_offset], pos, len); \
        res_offset += len; \
        res_offsets[i] = res_offset; \
        pos = begin + n * (i + 1); \
        ++i; \
        budget.charge(len); \
    } while (false)

            /// Copy skipped strings without any changes.
            while (i < input_rows_count && begin + n * (i + 1) <= match)
            {
                COPY_REST_OF_CURRENT_STRING();
            }

            /// If you have reached the end, it's time to stop
            if (i == input_rows_count)
                break;

            /// Copy unchanged part of current string.
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);
            res_offset += (match - pos);

            /// Is it true that this string no longer needs to perform conversions.
            bool can_finish_current_string = false;

            /// We check that the entry does not pass through the boundaries of strings.
            if (match + needle.size() <= begin + n * (i + 1))
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                /// See the same charge in `vectorConstantConstant`: the written bytes are work as well.
                budget.charge(replacement.size());
                pos = match + needle.size();
                if constexpr (replace == ReplaceStringTraits::Replace::First)
                    can_finish_current_string = true;
                else if (pos == begin + n * (i + 1))
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                COPY_REST_OF_CURRENT_STRING();
            }
#undef COPY_REST_OF_CURRENT_STRING
        }
    }
};

}
