#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>


/** A function to extract text from HTML or XHTML.
  * It does not necessarily 100% conforms to any of the HTML, XML or XHTML standards,
  * but the implementation is reasonably accurate and it is fast.
  *
  * The rules are the following:
  *
  * 1. Comments are skipped. Example: <!-- test -->
  * Comment must end with -->. Nested comments are not possible.
  * Note: constructions like <!--> <!---> are not valid comments in HTML but will be skipped by other rules.
  *
  * 2. CDATA is pasted verbatim.
  * Note: CDATA is XML/XHTML specific. But we still process it for "best-effort" approach.
  *
  * 3. 'script' and 'style' elements are removed with all their content.
  * Note: it's assumed that closing tag cannot appear inside content.
  * For example, in JS string literal is has to be escaped as "<\/script>".
  * Note: comments and CDATA is possible inside script or style - then closing tags are not searched inside CDATA.
  * Example: <script><![CDATA[</script>]]></script>
  * But still searched inside comments. Sometimes it becomes complicated:
  * <script>var x = "<!--"; </script> var y = "-->"; alert(x + y);</script>
  * Note: script and style can be the names of XML namespaces - then they are not treat like usual script or style.
  * Example: <script:a>Hello</script:a>.
  * Note: whitespaces are possible after closing tag name: </script > but not before: < / script>.
  *
  * 4. Other tags or tag-like elements are skipped without inner content.
  * Example: <a>.</a>
  * Note: it's expected that this HTML is illegal: <a test=">"></a>
  * Note: it will also skip something like tags: <>, <!>, etc.
  * Note: tag without end will be skipped to the end of input: <hello
  * >
  * 5. HTML and XML entities are not decoded.
  * It should be processed by separate function.
  *
  * 6. Whitespaces in text are collapsed or inserted by specific rules.
  * Whitespaces at beginning and at the end are removed.
  * Consecutive whitespaces are collapsed.
  * But if text is separated by other elements and there is no whitespace, it is inserted.
  * It may be unnatural, examples: Hello<b>world</b>, Hello<!-- -->world
  * - in HTML there will be no whitespace, but the function will insert it.
  * But also consider: Hello<p>world</p>, Hello<br>world.
  * This behaviour is reasonable for data analysis, e.g. convert HTML to a bag of words.
  *
  * 7. Also note that correct handling of whitespaces would require
  * support of <pre></pre> and CSS display and white-space properties.
  *
  * Usage example:
  *
  * SELECT extractTextFromHTML(html) FROM url('https://yandex.ru/', RawBLOB, 'html String')
  *
  * - ClickHouse has embedded web browser.
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

inline bool startsWith(const char * s, const char * end, const char * prefix)
{
    return s + strlen(prefix) < end && 0 == memcmp(s, prefix, strlen(prefix));
}

inline bool checkAndSkip(const char * __restrict & s, const char * end, const char * prefix)
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

bool processCDATA(const char * __restrict & src, const char * end, char * __restrict & dst)
{
    if (!checkAndSkip(src, end, "<![CDATA["))
        return false;

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
    const auto * old_src = src;

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

        /// Skip CDATA
        if (*src == '!')
        {
            --src;
            char * dst = nullptr;
            if (processCDATA(src, end, dst))
                continue;
            ++src;
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

void copyText(const char * __restrict & src, const char * end, char * __restrict & dst, bool needs_whitespace)
{
    while (src < end && isWhitespaceASCII(*src))
        ++src;

    const char * lt = find_first_symbols<'<'>(src, end);

    if (needs_whitespace && src < lt)
    {
        *dst = ' ';
        ++dst;
    }

    while (true)
    {
        const char * ws = find_first_symbols<' ', '\t', '\n', '\r', '\f', '\v'>(src, lt);
        size_t bytes_to_copy = ws - src;
        memcpy(dst, src, bytes_to_copy);
        dst += bytes_to_copy;

        src = ws;
        while (src < lt && isWhitespaceASCII(*src))
            ++src;

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

    const char * end = src + size;
    char * dst_begin = dst;

    while (src < end)
    {
        bool needs_whitespace = dst != dst_begin && dst[-1] != ' ';
        copyText(src, end, dst, needs_whitespace);

        processComment(src, end)
            || processCDATA(src, end, dst)
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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionExtractTextFromHTML>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

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
