#include <Functions/FunctionReverseBySeparator.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/StringSearcher.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/PODArray.h>
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

}

DataTypePtr FunctionReverseBySeparator::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
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

ColumnPtr FunctionReverseBySeparator::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const ColumnPtr & column_haystack = arguments[0].column;
    const ColumnString * col_haystack = checkAndGetColumn<ColumnString>(column_haystack.get());
    if (!col_haystack)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be String", getName());

    String separator_str = ".";
    std::string_view separator;

    if (arguments.size() == 2)
    {
        const ColumnPtr & column_separator = arguments[1].column;
        if (const ColumnConst * col_const_separator = checkAndGetColumn<ColumnConst>(column_separator.get()))
        {
            separator_str = col_const_separator->getValue<String>();
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Second argument for function {} must be constant String", getName());
        }
    }
    
    separator = std::string_view(separator_str.data(), separator_str.size());

    auto col_res = ColumnString::create();
    ColumnString::Chars & res_data = col_res->getChars();
    ColumnString::Offsets & res_offsets = col_res->getOffsets();
    res_offsets.resize(input_rows_count);

    size_t total_size_estimate = 0;
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        total_size_estimate += col_haystack->getDataAt(i).size();
    }
    
    res_data.reserve(total_size_estimate);

    /// Main processing loop - vectorized execution
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const std::string_view haystack = col_haystack->getDataAt(i);
        // separator is already defined above as a string_view

        /// Handle empty input
        if (haystack.size() == 0)
        {
            res_offsets[i] = res_data.size();
            continue;
        }

        /// Handle empty separator - reverse entire string
        if (separator.size() == 0)
        {
            for (size_t j = 0; j < haystack.size(); ++j)
                res_data.push_back(haystack.data()[haystack.size() - 1 - j]);
            res_offsets[i] = res_data.size();
            continue;
        }

        /// For single-character separators, use optimized path
        if (separator.size() == 1)
        {
            const char sep_char = separator.data()[0];
            PODArray<size_t, 4096, Allocator<false>, 0, 0> token_starts;
            token_starts.reserve(haystack.size() / 2 + 2); /// Estimate

            size_t pos = 0;
            while (pos < haystack.size())
            {
                token_starts.push_back(pos);

                /// Find next separator
                size_t next_pos = pos;
                while (next_pos < haystack.size() && haystack.data()[next_pos] != sep_char)
                    ++next_pos;

                if (next_pos < haystack.size())
                    pos = next_pos + 1;
                else
                    break; /// No more separators, we're done
            }
            
            /// Handle the case where the string ends with a separator
            if (pos == haystack.size())
                token_starts.push_back(pos);

            /// Write tokens in reverse order
            for (int64_t j = static_cast<int64_t>(token_starts.size()) - 1; j >= 0; --j)
            {
                size_t start = token_starts[j];
                size_t end = (j + 1 < static_cast<int64_t>(token_starts.size()))
                    ? token_starts[j + 1] - 1
                    : haystack.size();

                /// Add separator before token (except for the first token we write)
                if (j < static_cast<int64_t>(token_starts.size()) - 1)
                    res_data.push_back(sep_char);

                /// Add the token content
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

        res_offsets[i] = res_data.size();
    }

    return col_res;
}

REGISTER_FUNCTION(ReverseBySeparator)
{
    FunctionDocumentation::Description description = R"(
Reverses the order of substrings in a string separated by a specified separator.
This function splits the string by the separator, reverses the order of the resulting parts,
and joins them back using the same separator. It is useful for parsing domain names,
file paths, or other hierarchical data where you need to reverse the order of components.

Examples:
- reverseBySeparator('www.google.com') returns 'com.google.www'
- reverseBySeparator('a/b/c', '/') returns 'c/b/a'
- reverseBySeparator('x::y::z', '::') returns 'z::y::x'
)";

    FunctionDocumentation::Syntax syntax = "reverseBySeparator(string[, separator])";
    
    FunctionDocumentation::Arguments arguments = {
        {"string", "The input string to reverse the order of its parts.", {"String"}},
        {"separator", "The separator string used to identify parts. If not provided, uses '.' (dot). Default: '.'", {"String"}}
    };
    
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a string with substrings ordered from right to left of the original string, joined by the same separator.",
        {"String"}
    };
    
    FunctionDocumentation::Examples examples = {
        {"Basic domain reversal", "SELECT reverseBySeparator('www.google.com')", "'com.google.www'"},
        {"Path reversal", "SELECT reverseBySeparator('a/b/c', '/')", "'c/b/a'"},
        {"Custom separator", "SELECT reverseBySeparator('x::y::z', '::')", "'z::y::x'"},
        {"Edge case with dots", "SELECT reverseBySeparator('.a.b.', '.')", "'.b.a.'"},
        {"Single element", "SELECT reverseBySeparator('single')", "'single'"},
        {"Empty separator", "SELECT reverseBySeparator('abcde', '')", "'edcba'"}
    };
    
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReverseBySeparator>(documentation);
}

}
