#include <Functions/FunctionReverseSplit.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/StringSearcher.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/MemorySanitizer.h>
#include <IO/WriteHelpers.h>
#include <string_view>
#include <cstddef>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Helper class to efficiently find separator occurrences
template <typename Searcher>
class TokenSplitter
{
public:
    TokenSplitter(const std::string_view & input_haystack, const Searcher & input_searcher, size_t separator_size)
        : haystack(input_haystack), searcher(input_searcher), sep_size(separator_size)
    {
        current_pos = 0;
        findNextSeparator();
    }

    bool hasNext() const { return current_pos <= haystack.size(); }

    std::string_view nextToken()
    {
        if (current_pos > haystack.size())
            return {};

        size_t start = current_pos;
        size_t end = (next_separator_pos == std::string::npos) ? haystack.size() : next_separator_pos;

        current_pos = (next_separator_pos == std::string::npos) ? haystack.size() + 1 : next_separator_pos + sep_size;
        findNextSeparator();

        return std::string_view(haystack.data() + start, end - start);
    }

    size_t countTokens() const
    {
        size_t count = 0;
        size_t pos = 0;
        while (pos <= haystack.size())
        {
            count++;
            const UInt8 * found = searcher.search(reinterpret_cast<const UInt8*>(haystack.data() + pos), haystack.size() - pos);
            if (found == reinterpret_cast<const UInt8*>(haystack.data() + haystack.size()))
                break;
            pos = found - reinterpret_cast<const UInt8*>(haystack.data()) + sep_size;
        }
        return count;
    }

private:
    void findNextSeparator()
    {
        if (current_pos > haystack.size())
        {
            next_separator_pos = std::string::npos;
            return;
        }

        const UInt8 * found = searcher.search(reinterpret_cast<const UInt8*>(haystack.data() + current_pos), haystack.size() - current_pos);
        next_separator_pos = (found == reinterpret_cast<const UInt8*>(haystack.data() + haystack.size()))
            ? std::string::npos
            : found - reinterpret_cast<const UInt8*>(haystack.data());
    }

    const std::string_view & haystack;
    const Searcher & searcher;
    const size_t sep_size;
    size_t current_pos = 0;
    size_t next_separator_pos = 0;
};

/// Count occurrences of a character in a string
static size_t countCharacterOccurrences(const std::string_view & str, char ch)
{
    size_t count = 0;
    for (size_t i = 0; i < str.size(); ++i)
    {
        if (str.data()[i] == ch)
            ++count;
    }
    return count;
}

}

DataTypePtr FunctionReverseSplit::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    size_t number_of_arguments = arguments.size();

    if (number_of_arguments < 1 || number_of_arguments > 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
            getName(), number_of_arguments);

    if (!isString(arguments[0].type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function {} must be String, got {}",
            getName(), arguments[0].type->getName());

    if (number_of_arguments == 2)
    {
        if (!isString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be String, got {}",
                getName(), arguments[1].type->getName());
    }

    return std::make_shared<DataTypeString>();
}

ColumnPtr FunctionReverseSplit::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const ColumnPtr & column_haystack = arguments[0].column;
    const ColumnString * col_haystack = checkAndGetColumn<ColumnString>(column_haystack.get());
    if (!col_haystack)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be String", getName());

    String default_separator = ".";
    const std::string_view * separators = nullptr;
    std::vector<std::string_view> separator_buffer;

    if (arguments.size() == 2)
    {
        const ColumnPtr & column_separator = arguments[1].column;
        if (const ColumnConst * col_const_separator = checkAndGetColumn<ColumnConst>(column_separator.get()))
        {
            default_separator = col_const_separator->getValue<String>();
        }
        else if (const ColumnString * col_separator = checkAndGetColumn<ColumnString>(column_separator.get()))
        {
            separator_buffer.reserve(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
                separator_buffer.emplace_back(col_separator->getDataAt(i));
            separators = separator_buffer.data();
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Second argument for function {} must be constant String or ColumnString", getName());
        }
    }

    auto col_res = ColumnString::create();
    ColumnString::Chars & res_data = col_res->getChars();
    ColumnString::Offsets & res_offsets = col_res->getOffsets();
    res_offsets.resize(input_rows_count);

    /// Pre-calculate total size to avoid multiple reallocations
    size_t total_size_estimate = 0;
    {
        size_t res_chars_approx = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const std::string_view haystack = col_haystack->getDataAt(i);
            const std::string_view separator = separators ? separators[i] : std::string_view(default_separator.data(), default_separator.size());

            if (separator.size() == 0)
            {
                /// Empty separator - reverse the entire string
                res_chars_approx += haystack.size() + 1;
                continue;
            }

            /// For performance, use a simple count for single-character separators
            if (separator.size() == 1)
            {
                size_t sep_count = countCharacterOccurrences(haystack, separator.data()[0]);
                size_t token_count = sep_count + 1;
                res_chars_approx += haystack.size() + (token_count - 1) * separator.size() + 1;
            }
            else
            {
                /// For multi-character separators, use StringSearcher for accurate counting
                ASCIICaseSensitiveStringSearcher searcher(reinterpret_cast<const UInt8*>(separator.data()), separator.size());
                TokenSplitter<decltype(searcher)> splitter(haystack, searcher, separator.size());
                size_t token_count = splitter.countTokens();
                res_chars_approx += haystack.size() + (token_count - 1) * separator.size() + 1;
            }
        }
        total_size_estimate = res_chars_approx;
    }

    res_data.reserve(total_size_estimate + input_rows_count); /// +1 for null terminator per row

    /// Main processing loop - vectorized execution
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const std::string_view haystack = col_haystack->getDataAt(i);
        const std::string_view separator = separators ? separators[i] : std::string_view(default_separator.data(), default_separator.size());

        /// Handle empty input
        if (haystack.size() == 0)
        {
            res_data.push_back(0);
            res_offsets[i] = res_data.size();
            continue;
        }

        /// Handle empty separator - reverse entire string
        if (separator.size() == 0)
        {
            for (size_t j = 0; j < haystack.size(); ++j)
                res_data.push_back(haystack.data()[haystack.size() - 1 - j]);
            res_data.push_back(0);
            res_offsets[i] = res_data.size();
            continue;
        }

        /// For single-character separators, use optimized path
        if (separator.size() == 1)
        {
            const char sep_char = separator.data()[0];
            std::vector<size_t> token_starts;
            token_starts.reserve(haystack.size() / 2 + 2); /// Estimate

            size_t pos = 0;
            while (pos <= haystack.size())
            {
                token_starts.push_back(pos);
                if (pos == haystack.size())
                    break;

                /// Find next separator
                size_t next_pos = pos;
                while (next_pos < haystack.size() && haystack.data()[next_pos] != sep_char)
                    ++next_pos;

                if (next_pos < haystack.size())
                    pos = next_pos + 1;
                else
                    pos = haystack.size() + 1;
            }

            /// Write tokens in reverse order
            for (int64_t j = static_cast<int64_t>(token_starts.size()) - 1; j >= 0; --j)
            {
                size_t start = token_starts[j];
                size_t end = (j + 1 < static_cast<int64_t>(token_starts.size()))
                    ? token_starts[j + 1] - 1
                    : haystack.size();

                if (j < static_cast<int64_t>(token_starts.size()) - 1)
                    res_data.push_back(sep_char);

                if (end > start)
                    res_data.insert(res_data.end(), haystack.data() + start, haystack.data() + end);
            }
        }
        else
        {
            /// For multi-character separators, use StringSearcher
            ASCIICaseSensitiveStringSearcher searcher(reinterpret_cast<const UInt8*>(separator.data()), separator.size());
            TokenSplitter<decltype(searcher)> splitter(haystack, searcher, separator.size());

            /// Collect tokens in reverse order
            std::vector<std::string_view> tokens;
            tokens.reserve(8); /// Initial capacity

            while (splitter.hasNext())
            {
                std::string_view token = splitter.nextToken();
                tokens.push_back(token);
            }

            /// Write tokens in reverse order
            for (size_t j = 0; j < tokens.size(); ++j)
            {
                if (j > 0)
                    res_data.insert(res_data.end(), separator.data(), separator.data() + separator.size());

                const std::string_view & token = tokens[tokens.size() - 1 - j];
                if (token.size() > 0)
                    res_data.insert(res_data.end(), token.data(), token.data() + token.size());
            }
        }

        res_data.push_back(0); /// Null terminator
        res_offsets[i] = res_data.size();

        /// Safety check to prevent excessive memory usage
        if (res_data.size() > total_size_estimate * 2)
        {
            /// Re-estimate and reserve more space if needed
            size_t additional_reserve = (res_data.size() - total_size_estimate) * 2;
            res_data.reserve(res_data.size() + additional_reserve);
            total_size_estimate = res_data.size() + additional_reserve;
        }
    }

    return col_res;
}

REGISTER_FUNCTION(ReverseSplit)
{
    FunctionDocumentation::Description description = R"(
Splits a string into an array of substrings separated by a string separator, starting from the end.
This function processes the string from right to left, which is useful for parsing domain names,
file paths, or other hierarchical data where the rightmost parts are more significant.

Examples:
- reverseSplit('www.google.com') returns 'com.google.www'
- reverseSplit('a/b/c', '/') returns 'c/b/a'
- reverseSplit('x::y::z', '::') returns 'z::y::x'
)";

    FunctionDocumentation::Syntax syntax = "reverseSplit(string[, separator])";
    
    FunctionDocumentation::Arguments arguments = {
        {"string", "The input string to split and reverse.", {"String"}},
        {"separator", "The separator string. If not provided, splits by '.' (dot). Default: '.'", {"String"}}
    };
    
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a string with substrings ordered from right to left of the original string, joined by the same separator.",
        {"String"}
    };
    
    FunctionDocumentation::Examples examples = {
        {"Basic domain splitting", "SELECT reverseSplit('www.google.com')", "'com.google.www'"},
        {"Path splitting", "SELECT reverseSplit('a/b/c', '/')", "'c/b/a'"},
        {"Custom separator", "SELECT reverseSplit('x::y::z', '::')", "'z::y::x'"},
        {"Edge case with dots", "SELECT reverseSplit('.a.b.', '.')", "'.b.a.'"},
        {"Single element", "SELECT reverseSplit('single')", "'single'"},
        {"Empty separator", "SELECT reverseSplit('abcde', '')", "'edcba'"}
    };
    
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReverseSplit>(documentation);
}

}
