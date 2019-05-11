#include "FunctionsStringRegex.h"
#include "FunctionsStringSearch.h"

#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <IO/WriteHelpers.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Poco/UTF8String.h>
#include <Common/Volnitsky.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <Common/config.h>
#if USE_HYPERSCAN
#    if __has_include(<hs/hs.h>)
#        include <hs/hs.h>
#    else
#        include <hs.h>
#    endif
#endif

#if USE_RE2_ST
#    include <re2_st/re2.h> // Y_IGNORE
#else
#    define re2_st re2
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_MANY_BYTES;
    extern const int NOT_IMPLEMENTED;
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
}

/// Is the LIKE expression reduced to finding a substring in a string?
inline bool likePatternIsStrstr(const String & pattern, String & res)
{
    res = "";

    if (pattern.size() < 2 || pattern.front() != '%' || pattern.back() != '%')
        return false;

    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * end = pos + pattern.size();

    ++pos;
    --end;

    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
                return false;
            case '\\':
                ++pos;
                if (pos == end)
                    return false;
                else
                    res += *pos;
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return true;
}

/** 'like' - if true, treat pattern as SQL LIKE; if false - treat pattern as re2 regexp.
  * NOTE: We want to run regexp search for whole block by one call (as implemented in function 'position')
  *  but for that, regexp engine must support \0 bytes and their interpretation as string boundaries.
  */
template <bool like, bool revert = false>
struct MatchImpl
{
    using ResultType = UInt8;

    static void vector_constant(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets, const std::string & pattern, PaddedPODArray<UInt8> & res)
    {
        if (offsets.empty())
            return;

        String strstr_pattern;
        /// A simple case where the LIKE expression reduces to finding a substring in a string
        if (like && likePatternIsStrstr(pattern, strstr_pattern))
        {
            const UInt8 * begin = data.data();
            const UInt8 * pos = begin;
            const UInt8 * end = pos + data.size();

            /// The current index in the array of strings.
            size_t i = 0;

            /// TODO You need to make that `searcher` is common to all the calls of the function.
            Volnitsky searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

            /// We will search for the next occurrence in all rows at once.
            while (pos < end && end != (pos = searcher.search(pos, end - pos)))
            {
                /// Let's determine which index it refers to.
                while (begin + offsets[i] <= pos)
                {
                    res[i] = revert;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                if (pos + strstr_pattern.size() < begin + offsets[i])
                    res[i] = !revert;
                else
                    res[i] = revert;

                pos = begin + offsets[i];
                ++i;
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
        }
        else
        {
            size_t size = offsets.size();

            const auto & regexp = Regexps::get<like, true>(pattern);

            std::string required_substring;
            bool is_trivial;
            bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

            regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

            if (required_substring.empty())
            {
                if (!regexp->getRE2()) /// An empty regexp. Always matches.
                {
                    if (size)
                        memset(res.data(), 1, size * sizeof(res[0]));
                }
                else
                {
                    size_t prev_offset = 0;
                    for (size_t i = 0; i < size; ++i)
                    {
                        res[i] = revert
                            ^ regexp->getRE2()->Match(
                                  re2_st::StringPiece(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1),
                                  0,
                                  offsets[i] - prev_offset - 1,
                                  re2_st::RE2::UNANCHORED,
                                  nullptr,
                                  0);

                        prev_offset = offsets[i];
                    }
                }
            }
            else
            {
                /// NOTE This almost matches with the case of LikePatternIsStrstr.

                const UInt8 * begin = data.data();
                const UInt8 * pos = begin;
                const UInt8 * end = pos + data.size();

                /// The current index in the array of strings.
                size_t i = 0;

                Volnitsky searcher(required_substring.data(), required_substring.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Determine which index it refers to.
                    while (begin + offsets[i] <= pos)
                    {
                        res[i] = revert;
                        ++i;
                    }

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + strstr_pattern.size() < begin + offsets[i])
                    {
                        /// And if it does not, if necessary, we check the regexp.

                        if (is_trivial)
                            res[i] = !revert;
                        else
                        {
                            const char * str_data = reinterpret_cast<const char *>(&data[offsets[i - 1]]);
                            size_t str_size = offsets[i] - offsets[i - 1] - 1;

                            /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                              *  so that it can match when `required_substring` occurs into the string several times,
                              *  and at the first occurrence, the regexp is not a match.
                              */

                            if (required_substring_is_prefix)
                                res[i] = revert
                                    ^ regexp->getRE2()->Match(
                                          re2_st::StringPiece(str_data, str_size),
                                          reinterpret_cast<const char *>(pos) - str_data,
                                          str_size,
                                          re2_st::RE2::UNANCHORED,
                                          nullptr,
                                          0);
                            else
                                res[i] = revert
                                    ^ regexp->getRE2()->Match(
                                          re2_st::StringPiece(str_data, str_size), 0, str_size, re2_st::RE2::UNANCHORED, nullptr, 0);
                        }
                    }
                    else
                        res[i] = revert;

                    pos = begin + offsets[i];
                    ++i;
                }

                if (i < res.size())
                    memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
            }
        }
    }

    static void constant_constant(const std::string & data, const std::string & pattern, UInt8 & res)
    {
        const auto & regexp = Regexps::get<like, true>(pattern);
        res = revert ^ regexp->match(data);
    }

    template <typename... Args>
    static void vector_vector(Args &&...)
    {
        throw Exception("Functions 'like' and 'match' don't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    /// Search different needles in single haystack.
    template <typename... Args>
    static void constant_vector(Args &&...)
    {
        throw Exception("Functions 'like' and 'match' don't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename Type, bool FindAny, bool FindAnyIndex, bool MultiSearchDistance>
struct MultiMatchAnyImpl
{
    static_assert(static_cast<int>(FindAny) + static_cast<int>(FindAnyIndex) == 1);
    using ResultType = Type;
    static constexpr bool is_using_hyperscan = true;

    static void vector_constant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<Type> & res)
    {
        vector_constant(haystack_data, haystack_offsets, needles, res, std::nullopt);
    }

    static void vector_constant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<Type> & res,
        [[maybe_unused]] std::optional<UInt32> edit_distance)
    {
        (void)FindAny;
        (void)FindAnyIndex;
#if USE_HYPERSCAN
        const auto & hyperscan_regex = MultiRegexps::get<FindAnyIndex, MultiSearchDistance>(needles, edit_distance);
        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(hyperscan_regex->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception("Could not clone scratch space for hyperscan", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        MultiRegexps::ScratchPtr smart_scratch(scratch);

        auto on_match = []([[maybe_unused]] unsigned int id,
                           unsigned long long /* from */,
                           unsigned long long /* to */,
                           unsigned int /* flags */,
                           void * context) -> int
        {
            if constexpr (FindAnyIndex)
                *reinterpret_cast<Type *>(context) = id;
            else if constexpr (FindAny)
                *reinterpret_cast<Type *>(context) = 1;
            return 0;
        };
        const size_t haystack_offsets_size = haystack_offsets.size();
        UInt64 offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i)
        {
            UInt64 length = haystack_offsets[i] - offset - 1;
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception("Too long string to search", ErrorCodes::TOO_MANY_BYTES);
            res[i] = 0;
            err = hs_scan(
                hyperscan_regex->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + offset,
                length,
                0,
                smart_scratch.get(),
                on_match,
                &res[i]);
            if (err != HS_SUCCESS)
                throw Exception("Failed to scan with hyperscan", ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT);
            offset = haystack_offsets[i];
        }
#else
        /// Fallback if do not use hyperscan
        if constexpr (MultiSearchDistance)
            throw Exception(
                "Edit distance multi-search is not implemented when hyperscan is off (is it Intel processor?)",
                ErrorCodes::NOT_IMPLEMENTED);
        PaddedPODArray<UInt8> accum(res.size());
        memset(res.data(), 0, res.size() * sizeof(res.front()));
        memset(accum.data(), 0, accum.size());
        for (size_t j = 0; j < needles.size(); ++j)
        {
            MatchImpl<false, false>::vector_constant(haystack_data, haystack_offsets, needles[j].toString(), accum);
            for (size_t i = 0; i < res.size(); ++i)
            {
                if constexpr (FindAny)
                    res[i] |= accum[i];
                else if (FindAnyIndex && accum[i])
                    res[i] = j + 1;
            }
        }
#endif // USE_HYPERSCAN
    }
};


struct ExtractImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        const auto & regexp = Regexps::get<false, false>(pattern);

        unsigned capture = regexp->getNumberOfSubpatterns() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            unsigned count
                = regexp->match(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset - 1, matches, capture + 1);
            if (count > capture && matches[capture].offset != std::string::npos)
            {
                const auto & match = matches[capture];
                res_data.resize(res_offset + match.length + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + match.offset], match.length);
                res_offset += match.length;
            }
            else
            {
                res_data.resize(res_offset + 1);
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};


/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' could contain substitutions, for example: '\2-\3-\1'
  */
template <bool replace_one = false>
struct ReplaceRegexpImpl
{
    /// Sequence of instructions, describing how to get resulting string.
    /// Each element is either:
    /// - substitution (in that case first element of pair is their number and second element is empty)
    /// - string that need to be inserted (in that case, first element of pair is that string and second element is -1)
    using Instructions = std::vector<std::pair<int, std::string>>;

    static const size_t max_captures = 10;


    static Instructions createInstructions(const std::string & s, int num_captures)
    {
        Instructions instructions;

        String now = "";
        for (size_t i = 0; i < s.size(); ++i)
        {
            if (s[i] == '\\' && i + 1 < s.size())
            {
                if (isNumericASCII(s[i + 1])) /// Substitution
                {
                    if (!now.empty())
                    {
                        instructions.emplace_back(-1, now);
                        now = "";
                    }
                    instructions.emplace_back(s[i + 1] - '0', String());
                }
                else
                    now += s[i + 1]; /// Escaping
                ++i;
            }
            else
                now += s[i]; /// Plain character
        }

        if (!now.empty())
        {
            instructions.emplace_back(-1, now);
            now = "";
        }

        for (const auto & it : instructions)
            if (it.first >= num_captures)
                throw Exception(
                    "Invalid replace instruction in replacement string. Id: " + toString(it.first) + ", but regexp has only "
                        + toString(num_captures - 1) + " subpatterns",
                    ErrorCodes::BAD_ARGUMENTS);

        return instructions;
    }


    static void processString(
        const re2_st::StringPiece & input,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        re2_st::RE2 & searcher,
        int num_captures,
        const Instructions & instructions)
    {
        re2_st::StringPiece matches[max_captures];

        size_t start_pos = 0;
        while (start_pos < static_cast<size_t>(input.length()))
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(input, start_pos, input.length(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                const auto & match = matches[0];
                size_t bytes_to_copy = (match.data() - input.data()) - start_pos;

                /// Copy prefix before matched regexp without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                start_pos += bytes_to_copy + match.length();

                /// Do substitution instructions
                for (const auto & it : instructions)
                {
                    if (it.first >= 0)
                    {
                        res_data.resize(res_data.size() + matches[it.first].length());
                        memcpy(&res_data[res_offset], matches[it.first].data(), matches[it.first].length());
                        res_offset += matches[it.first].length();
                    }
                    else
                    {
                        res_data.resize(res_data.size() + it.second.size());
                        memcpy(&res_data[res_offset], it.second.data(), it.second.size());
                        res_offset += it.second.size();
                    }
                }

                if (replace_one || match.length() == 0) /// Stop after match of zero length, to avoid infinite loop.
                    can_finish_current_string = true;
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + input.length() - start_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, input.length() - start_pos);
                res_offset += input.length() - start_pos;
                start_pos = input.length();
            }
        }

        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        ++res_offset;
    }


    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        re2_st::RE2 searcher(needle);
        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, static_cast<int>(max_captures));

        Instructions instructions = createInstructions(replacement, num_captures);

        /// Cannot perform search for whole block. Will process each string separately.
        for (size_t i = 0; i < size; ++i)
        {
            int from = i > 0 ? offsets[i - 1] : 0;
            re2_st::StringPiece input(reinterpret_cast<const char *>(data.data() + from), offsets[i] - from - 1);

            processString(input, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vector_fixed(
        const ColumnString::Chars & data,
        size_t n,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        size_t size = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(size);

        re2_st::RE2 searcher(needle);
        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, static_cast<int>(max_captures));

        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < size; ++i)
        {
            int from = i * n;
            re2_st::StringPiece input(reinterpret_cast<const char *>(data.data() + from), n);

            processString(input, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }
};


/** Replace one or all occurencies of substring 'needle' to 'replacement'. 'needle' and 'replacement' are constants.
  */
template <bool replace_one = false>
struct ReplaceStringImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        /// The current index in the array of strings.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy the data without changing
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);

            /// Determine which index it belongs to.
            while (i < offsets.size() && begin + offsets[i] <= match)
            {
                res_offsets[i] = res_offset + ((begin + offsets[i]) - pos);
                ++i;
            }
            res_offset += (match - pos);

            /// If you have reached the end, it's time to stop
            if (i == offsets.size())
                break;

            /// Is it true that this string no longer needs to perform transformations.
            bool can_finish_current_string = false;

            /// We check that the entry does not go through the boundaries of strings.
            if (match + needle.size() < begin + offsets[i])
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle.size();
                if (replace_one)
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + (begin + offsets[i] - pos));
                memcpy(&res_data[res_offset], pos, (begin + offsets[i] - pos));
                res_offset += (begin + offsets[i] - pos);
                res_offsets[i] = res_offset;
                pos = begin + offsets[i];
                ++i;
            }
        }
    }

    /// Note: this function converts fixed-length strings to variable-length strings
    ///       and each variable-length string should ends with zero byte.
    static void vector_fixed(
        const ColumnString::Chars & data,
        size_t n,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        size_t count = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(count);

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

#define COPY_REST_OF_CURRENT_STRING() \
    do \
    { \
        const size_t len = begin + n * (i + 1) - pos; \
        res_data.resize(res_data.size() + len + 1); \
        memcpy(&res_data[res_offset], pos, len); \
        res_offset += len; \
        res_data[res_offset++] = 0; \
        res_offsets[i] = res_offset; \
        pos = begin + n * (i + 1); \
        ++i; \
    } while (false)

            /// Copy skipped strings without any changes but
            /// add zero byte to the end of each string.
            while (i < count && begin + n * (i + 1) <= match)
            {
                COPY_REST_OF_CURRENT_STRING();
            }

            /// If you have reached the end, it's time to stop
            if (i == count)
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
                pos = match + needle.size();
                if (replace_one || pos == begin + n * (i + 1))
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

    static void constant(const std::string & data, const std::string & needle, const std::string & replacement, std::string & res_data)
    {
        res_data = "";
        int replace_cnt = 0;
        for (size_t i = 0; i < data.size(); ++i)
        {
            bool match = true;
            if (i + needle.size() > data.size() || (replace_one && replace_cnt > 0))
                match = false;
            for (size_t j = 0; match && j < needle.size(); ++j)
                if (data[i + j] != needle[j])
                    match = false;
            if (match)
            {
                ++replace_cnt;
                res_data += replacement;
                i = i + needle.size() - 1;
            }
            else
                res_data += data[i];
        }
    }
};


template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringReplace>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[2]))
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column_src = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;
        const ColumnPtr column_replacement = block.getByPosition(arguments[2]).column;

        if (!column_needle->isColumnConst() || !column_replacement->isColumnConst())
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be constants.", ErrorCodes::ILLEGAL_COLUMN);

        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();
        const IColumn * c2 = block.getByPosition(arguments[2]).column.get();
        const ColumnConst * c1_const = typeid_cast<const ColumnConst *>(c1);
        const ColumnConst * c2_const = typeid_cast<const ColumnConst *>(c2);
        String needle = c1_const->getValue<String>();
        String replacement = c2_const->getValue<String>();

        if (needle.size() == 0)
            throw Exception("Length of the second argument of function replace must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), needle, replacement, col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector_fixed(col_fixed->getChars(), col_fixed->getN(), needle, replacement, col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct NameMatch
{
    static constexpr auto name = "match";
};
struct NameLike
{
    static constexpr auto name = "like";
};
struct NameNotLike
{
    static constexpr auto name = "notLike";
};
struct NameMultiMatchAny
{
    static constexpr auto name = "multiMatchAny";
};
struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multiMatchAnyIndex";
};
struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multiFuzzyMatchAny";
};
struct NameMultiFuzzyMatchAnyIndex
{
    static constexpr auto name = "multiFuzzyMatchAnyIndex";
};
struct NameExtract
{
    static constexpr auto name = "extract";
};
struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};
struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};
struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};
struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};


using FunctionMatch = FunctionsStringSearch<MatchImpl<false>, NameMatch>;

using FunctionMultiMatchAny = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt8, true, false, false>,
    NameMultiMatchAny,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt64, false, true, false>,
    NameMultiMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<UInt8, true, false, true>,
    NameMultiFuzzyMatchAny,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiFuzzyMatchAnyIndex = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<UInt64, false, true, true>,
    NameMultiFuzzyMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

using FunctionLike = FunctionsStringSearch<MatchImpl<true>, NameLike>;
using FunctionNotLike = FunctionsStringSearch<MatchImpl<true, true>, NameNotLike>;
using FunctionExtract = FunctionsStringSearchToString<ExtractImpl, NameExtract>;
using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<true>, NameReplaceOne>;
using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;
using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<true>, NameReplaceRegexpOne>;
using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<false>, NameReplaceRegexpAll>;

void registerFunctionsStringRegex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMatch>();
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionExtract>();

    factory.registerFunction<FunctionReplaceOne>();
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerFunction<FunctionReplaceRegexpOne>();
    factory.registerFunction<FunctionReplaceRegexpAll>();

    factory.registerFunction<FunctionMultiMatchAny>();
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::CaseInsensitive);
}
}
