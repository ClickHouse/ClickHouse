#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/HTMLSelector.h>
#include <Functions/IFunction.h>
#include <Functions/extractTextFromHTML.h>
#include <Common/StringUtils.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Collapses whitespace and trims it at both sides.
void appendCollapsedWhitespace(std::string_view text, ColumnString::Chars & chars)
{
    bool written = false;
    bool need_space = false;
    for (char c : text)
    {
        if (isWhitespaceASCII(c))
        {
            need_space = written;
            continue;
        }
        if (need_space)
            chars.push_back(' ');
        need_space = false;
        chars.push_back(static_cast<UInt8>(c));
        written = true;
    }
}

void appendMatch(const HTML::Match & match, std::string_view document, ColumnString::Chars & chars)
{
    switch (match.output)
    {
        case HTML::Output::OuterHTML:
        {
            chars.insert(document.data() + match.begin, document.data() + match.end);
            break;
        }
        case HTML::Output::Attribute:
        {
            chars.insert(match.attribute.data(), match.attribute.data() + match.attribute.size());
            break;
        }
        case HTML::Output::Text:
        {
            std::string_view content = document.substr(match.content_begin, match.content_end - match.content_begin);
            if (match.raw_text)
            {
                /// The content of `script` or `style` is returned as is, for example, to extract JSON-LD.
                chars.insert(content.data(), content.data() + content.size());
            }
            else if (match.escapable_raw_text)
            {
                /// The content of `title` or `textarea` is text, but not HTML.
                appendCollapsedWhitespace(content, chars);
            }
            else
            {
                size_t old_size = chars.size();
                chars.resize(old_size + content.size());
                size_t size = extractTextFromHTML(content.data(), content.size(), reinterpret_cast<char *>(chars.data() + old_size));
                chars.resize(old_size + size);
            }
            break;
        }
    }
}


/// extractHTMLOne(html, selector) - like `document.querySelector`, returns the first match or an empty string.
/// extractHTMLAll(html, selector) - like `document.querySelectorAll`, returns an array of all matches.
template <bool all>
class FunctionExtractHTML : public IFunction
{
public:
    static constexpr auto name = all ? "extractHTMLAll" : "extractHTMLOne";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionExtractHTML>(); }
    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"html", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"selector", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };
        validateFunctionArguments(*this, arguments, args);

        if constexpr (all)
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        else
            return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr html_holder = arguments[0].column->convertToFullColumnIfConst();
        const ColumnString * html_column = checkAndGetColumn<ColumnString>(html_holder.get());
        if (!html_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of the first argument of function {}",
                arguments[0].column->getName(), getName());

        const ColumnString * selector_column = nullptr;
        std::optional<HTML::Selector> selector;
        std::optional<HTML::Matcher> matcher;
        String current_selector;

        if (const auto * selector_const = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
        {
            selector.emplace(selector_const->getDataAt(0));
            matcher.emplace(*selector);
        }
        else
        {
            selector_column = checkAndGetColumn<ColumnString>(arguments[1].column.get());
            if (!selector_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of the second argument of function {}",
                    arguments[1].column->getName(), getName());
        }

        auto res_strings = ColumnString::create();
        ColumnString::Chars & res_chars = res_strings->getChars();
        ColumnString::Offsets & res_string_offsets = res_strings->getOffsets();

        auto res_array_offsets = ColumnArray::ColumnOffsets::create();
        ColumnArray::Offsets & res_offsets = res_array_offsets->getData();

        if constexpr (all)
            res_offsets.resize(input_rows_count);
        else
            res_string_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (selector_column)
            {
                std::string_view selector_text = selector_column->getDataAt(i);
                if (!selector || selector_text != current_selector)
                {
                    /// The matcher references the selector.
                    matcher.reset();
                    selector.emplace(selector_text);
                    matcher.emplace(*selector);
                    current_selector = selector_text;
                }
            }

            std::string_view document = html_column->getDataAt(i);
            const auto & matches = matcher->find(document, !all);

            if constexpr (all)
            {
                for (const auto & match : matches)
                {
                    appendMatch(match, document, res_chars);
                    res_string_offsets.push_back(res_chars.size());
                }
                res_offsets[i] = res_string_offsets.size();
            }
            else
            {
                if (!matches.empty())
                    appendMatch(matches.front(), document, res_chars);
                res_string_offsets[i] = res_chars.size();
            }
        }

        if constexpr (all)
            return ColumnArray::create(std::move(res_strings), std::move(res_array_offsets));
        else
            return res_strings;
    }
};

}

REGISTER_FUNCTION(ExtractHTML)
{
    FunctionDocumentation::Description common_description = R"(

The selector supports a subset of CSS selectors, compatible with `document.querySelector` in browsers:
- type selectors and the universal selector: `div`, `*`;
- identifiers and classes: `#main`, `.item`;
- attribute selectors: `[href]`, `[lang=en]`, `[class~=item]`, `[lang|=en]`, `[href^=https]`, `[href$=".pdf"]`, `[href*=example]`, with an optional `i` flag for case-insensitive comparison: `[type=TEXT i]`;
- combinators: descendant (`div p`), child (`ul > li`), next sibling (`h2 + p`), subsequent sibling (`h2 ~ p`);
- pseudo-classes `:first-child`, `:nth-child(an+b)` (also `odd` and `even`), `:not(...)` with a list of compound selectors, `:root`;
- selector lists: `h1, h2`.

The result is selected by an optional pseudo-element at the end of the selector (like in Scrapy):
- by default, the matched element is returned with its tags and content (like `outerHTML`);
- `::text` returns the text content of the element, extracted by the rules of the `extractTextFromHTML` function, except that the content of `script` and `style` elements is returned as is (useful to extract JSON-LD), and the content of `title` and `textarea` with collapsed whitespace;
- `::attr(name)` returns the value of the attribute; elements without this attribute are skipped.

The HTML is parsed approximately in a single pass, but taking into account the most important rules of HTML parsers:
void elements (`br`, `img`, ...), self-closing tags (`<x/>`), comments, `CDATA`, raw text elements (`script`, `style`, `textarea`, `title`),
elements without end tags (`p`, `li`, `td`, `tr`, `option`, ...), and unmatched end tags.
Tag and attribute names are case-insensitive. Missing `html`, `head` and `body` elements are not inserted.
HTML entities are not decoded, use the `decodeHTMLComponent` function for that.
)";

    {
        FunctionDocumentation::Description description = R"(
Returns the first fragment of an HTML document matching a CSS selector, similarly to `document.querySelector` in JavaScript.
Returns an empty string if nothing matches.
)" + common_description;
        FunctionDocumentation::Syntax syntax = "extractHTMLOne(html, selector)";
        FunctionDocumentation::Arguments arguments = {
            {"html", "HTML document or fragment.", {"String"}},
            {"selector", "CSS selector.", {"String"}},
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the first matched fragment, or an empty string.", {"String"}};
        FunctionDocumentation::Examples examples = {
            {"Element", "SELECT extractHTMLOne('<ul><li>One</li><li class=\"x\">Two</li></ul>', 'li.x')", "<li class=\"x\">Two</li>"},
            {"Text", "SELECT extractHTMLOne('<html><head><title>Hello</title></head><body><h1>Title <b>here</b></h1></body></html>', 'h1::text')", "Title here"},
            {"Attribute", R"(SELECT extractHTMLOne('<meta property="og:image" content="https://example.com/a.png">', 'meta[property="og:image"]::attr(content)'))", "https://example.com/a.png"},
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionExtractHTML<false>>(documentation);
    }

    {
        FunctionDocumentation::Description description = R"(
Returns all fragments of an HTML document matching a CSS selector, in the document order, similarly to `document.querySelectorAll` in JavaScript.
If matched elements are nested, both are returned.
)" + common_description;
        FunctionDocumentation::Syntax syntax = "extractHTMLAll(html, selector)";
        FunctionDocumentation::Arguments arguments = {
            {"html", "HTML document or fragment.", {"String"}},
            {"selector", "CSS selector.", {"String"}},
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the array of matched fragments.", {"Array(String)"}};
        FunctionDocumentation::Examples examples = {
            {"Elements", "SELECT extractHTMLAll('<ul><li>One<li>Two</ul>', 'li')", "['<li>One','<li>Two']"},
            {"Links", R"(SELECT extractHTMLAll('<a href="/a">A</a> <a name="x">X</a> <a href="/b">B</a>', 'a::attr(href)'))", "['/a','/b']"},
            {"Table cells", "SELECT extractHTMLAll('<table><tr><td>1<td>2<tr><td>3<td>4</table>', 'tr > td:nth-child(2)::text')", "['2','4']"},
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionExtractHTML<true>>(documentation);
    }
}

}
