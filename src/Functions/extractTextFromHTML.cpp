#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <common/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

ALWAYS_INLINE bool startsWith(const char * s, const char * end, const char * prefix)
{
    return s + strlen(prefix) < end && 0 == memcmp(s, prefix, strlen(prefix));
}

ALWAYS_INLINE bool checkAndSkip(const char * __restrict & s, const char * end, const char * prefix)
{
    if (startsWith(s, end, prefix))
    {
        s += strlen(prefix);
        return true;
    }
    return false;
}

bool processComment(const char * __restrict & src, const char * end)
{
    if (!checkAndSkip(src, end, "<!--"))
        return false;

    while (true)
    {
        const char * gt = find_first_symbols<'>'>(src, end);
        if (gt >= end)
            break;

        if (gt > src + strlen("--") && gt[-1] == '-' && gt[-2] == '-')
        {
            src = gt + 1;
            break;
        }

        src = gt + 1;
    }

    return true;
}

bool processCDATA(const char * __restrict & src, const char * end, char * __restrict & dst, bool & pending_whitespace)
{
    if (!checkAndSkip(src, end, "<![CDATA["))
        return false;

    if (dst && pending_whitespace && src < end)
    {
        pending_whitespace = false;
        *dst = ' ';
        ++dst;
    }

    const char * gt = src;
    while (true)
    {
        gt = find_first_symbols<'>'>(gt, end);
        if (gt >= end)
            break;

        if (gt[-1] == ']' && gt[-2] == ']')
        {
            if (dst)
            {
                size_t bytes_to_copy = gt - src - strlen("]]");
                memcpy(dst, src, bytes_to_copy);
                dst += bytes_to_copy;
            }
            src = gt + 1;
            break;
        }

        ++gt;
    }

    return true;
}

bool processElementAndSkipContent(const char * __restrict & src, const char * end, const char * tag_name)
{
    auto old_src = src;

    if (!(src < end && *src == '<'))
        return false;
    ++src;

    if (!checkAndSkip(src, end, tag_name))
    {
        src = old_src;
        return false;
    }

    if (src >= end)
        return false;

    if (!(isWhitespaceASCII(*src) || *src == '>'))
    {
        src = old_src;
        return false;
    }

    const char * gt = find_first_symbols<'>'>(src, end);
    if (gt >= end)
        return false;

    src = gt + 1;

    while (true)
    {
        const char * lt = find_first_symbols<'<'>(src, end);
        src = lt;
        if (src + 1 >= end)
            break;

        ++src;

        /// Skip comments and CDATA
        if (*src == '!')
        {
            --src;
            bool pending_whitespace = false;
            char * dst = nullptr;
            processComment(src, end) || processCDATA(src, end, dst, pending_whitespace);
            continue;
        }

        if (*src != '/')
            continue;
        ++src;

        if (checkAndSkip(src, end, tag_name))
        {
            while (src < end && isWhitespaceASCII(*src))
                ++src;

            if (src >= end)
                break;

            if (*src == '>')
            {
                ++src;
                break;
            }
        }
    }

    return true;
}

bool skipTag(const char * __restrict & src, const char * end)
{
    if (src < end && *src == '<')
    {
        src = find_first_symbols<'>'>(src, end);
        if (src < end)
            ++src;

        return true;
    }

    return false;
}

void copyText(const char * __restrict & src, const char * end, char * __restrict & dst, bool & pending_whitespace)
{
    while (src < end && isWhitespaceASCII(*src))
    {
        pending_whitespace = true;
        ++src;
    }

    if (pending_whitespace && src < end)
    {
        pending_whitespace = false;
        *dst = ' ';
        ++dst;
    }

    const char * lt = find_first_symbols<'<'>(src, end);

    while (true)
    {
        const char * ws = find_first_symbols<' ', '\t', '\n', '\r', '\f', '\v'>(src, lt);
        size_t bytes_to_copy = ws - src;
        memcpy(dst, src, bytes_to_copy);
        dst += bytes_to_copy;

        src = ws;
        while (src < lt && isWhitespaceASCII(*src))
        {
            pending_whitespace = true;
            ++src;
        }

        if (src < lt)
        {
            *dst = ' ';
            ++dst;
        }
        else
        {
            break;
        }
    }

    src = lt;
}

size_t extract(const char * __restrict src, size_t size, char * __restrict dst)
{
    /** There are the following rules:
      * - comments are removed with all their content;
      * - elements 'script' and 'style' are removed with all their content;
      * - for other elements tags are removed but content is processed as text;
      * - CDATA should be copied verbatim;
      */

    char * dst_begin = dst;
    const char * end = src + size;
    bool pending_whitespace = false;

    while (src < end)
    {
        copyText(src, end, dst, pending_whitespace);

        processComment(src, end)
            || processCDATA(src, end, dst, pending_whitespace)
            || processElementAndSkipContent(src, end, "script")
            || processElementAndSkipContent(src, end, "style")
            || skipTag(src, end);
    }

    return dst - dst_begin;
}

}


class FunctionExtractTextFromHTML : public IFunction
{
public:
    static constexpr auto name = "extractTextFromHTML";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionExtractTextFromHTML>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows) const override
    {
        const ColumnString * src = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!src)
             throw Exception("First argument for function " + getName() + " must be string.", ErrorCodes::ILLEGAL_COLUMN);

        const ColumnString::Chars & src_chars = src->getChars();
        const ColumnString::Offsets & src_offsets = src->getOffsets();

        auto res = ColumnString::create();

        ColumnString::Chars & res_chars = res->getChars();
        ColumnString::Offsets & res_offsets = res->getOffsets();

        res_chars.resize(src_chars.size());
        res_offsets.resize(src_offsets.size());

        ColumnString::Offset src_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < rows; ++i)
        {
            auto next_src_offset = src_offsets[i];

            res_offset += extract(
                reinterpret_cast<const char *>(&src_chars[src_offset]),
                next_src_offset - src_offset - 1,
                reinterpret_cast<char *>(&res_chars[res_offset]));

            res_chars[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            src_offset = next_src_offset;
        }

        res_chars.resize(res_offset);
        return res;
    }
};

void registerFunctionExtractTextFromHTML(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractTextFromHTML>();
}

}
