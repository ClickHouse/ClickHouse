#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <cstring>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** resolveRelativeURL(relative, base) - resolves a relative URL (e.g. taken from an HTML page) against a base URL.
  *
  * The function prefers implementation simplicity over full compliance with RFC 3986:
  * - if the relative URL has a scheme ('ftp://example.com/img'), it is returned as is;
  * - a protocol-relative URL ('//example.com/img') takes only the scheme from the base URL;
  * - an absolute path ('/img') takes the scheme and the authority from the base URL;
  * - '?query' replaces the query string and the fragment of the base URL;
  * - '#fragment' replaces the fragment of the base URL;
  * - otherwise ('img'), the relative URL replaces everything after the last slash of the base URL's path.
  * Leading './' and '../' segments of the relative URL are collapsed against the base URL's directory,
  * while dot segments in the middle of the path are kept as is.
  * If the relative URL is empty, the base URL is returned. If the base URL is empty, the relative URL is returned.
  * The function does not know about HTML's 'base' tag.
  */
class FunctionResolveRelativeURL final : public IFunction
{
public:
    static constexpr auto name = "resolveRelativeURL";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionResolveRelativeURL>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * relative_column = arguments[0].column.get();
        const IColumn * base_column = arguments[1].column.get();

        /// Each of the two arguments is either a full column or a constant.
        /// (If both are constants, the default implementation for constants executes the function on a single row.)

        const ColumnString * relative_vector = checkAndGetColumn<ColumnString>(relative_column);
        const ColumnConst * relative_const = checkAndGetColumnConst<ColumnString>(relative_column);
        if (!relative_vector && !relative_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                relative_column->getName(), getName());

        const ColumnString * base_vector = checkAndGetColumn<ColumnString>(base_column);
        const ColumnConst * base_const = checkAndGetColumnConst<ColumnString>(base_column);
        if (!base_vector && !base_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second argument of function {}",
                base_column->getName(), getName());

        std::string_view relative_const_value;
        if (relative_const)
            relative_const_value = relative_const->getDataColumn().getDataAt(0);

        std::string_view base_const_value;
        if (base_const)
            base_const_value = base_const->getDataColumn().getDataAt(0);

        auto col_res = ColumnString::create();
        ColumnString::Chars & res_data = col_res->getChars();
        ColumnString::Offsets & res_offsets = col_res->getOffsets();

        res_data.reserve(
            (relative_vector ? relative_vector->getChars().size() : relative_const_value.size() * input_rows_count)
            + (base_vector ? base_vector->getChars().size() : base_const_value.size() * input_rows_count));
        res_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view relative = relative_vector ? relative_vector->getDataAt(i) : relative_const_value;
            std::string_view base = base_vector ? base_vector->getDataAt(i) : base_const_value;

            resolve(relative, base, res_data);
            res_offsets[i] = res_data.size();
        }

        return col_res;
    }

private:
    /// Appends a string to the result column data.
    static void append(ColumnString::Chars & res_data, std::string_view str)
    {
        if (str.empty())
            return;

        size_t old_size = res_data.size();
        res_data.resize(old_size + str.size());
        memcpy(res_data.data() + old_size, str.data(), str.size());
    }

    /// Returns the length of the scheme if the URL starts with "<scheme>://", otherwise 0.
    static size_t schemeLength(std::string_view url)
    {
        if (url.empty() || !isAlphaASCII(url[0]))
            return 0;

        size_t pos = 1;
        while (pos < url.size() && (isAlphaNumericASCII(url[pos]) || url[pos] == '+' || url[pos] == '-' || url[pos] == '.'))
            ++pos;

        if (pos + 2 < url.size() && url[pos] == ':' && url[pos + 1] == '/' && url[pos + 2] == '/')
            return pos;

        return 0;
    }

    /// Returns the position right after the authority (user info, host and port) of the URL:
    /// the position of the first '/', '?' or '#' after the "scheme://" prefix, or the end of the URL.
    static size_t authorityEnd(std::string_view url)
    {
        size_t scheme_length = schemeLength(url);

        size_t authority_start = 0;
        if (scheme_length > 0)
            authority_start = scheme_length + 3;
        else if (url.starts_with("//"))
            authority_start = 2;

        size_t pos = url.find_first_of("/?#", authority_start);
        return pos == std::string_view::npos ? url.size() : pos;
    }

    /// Resolves the relative URL against the base URL and appends the result to res_data.
    static void resolve(std::string_view relative, std::string_view base, ColumnString::Chars & res_data)
    {
        static constexpr size_t npos = std::string_view::npos;

        /// An empty relative URL points to the base URL itself.
        if (relative.empty())
        {
            append(res_data, base);
            return;
        }

        /// If the relative URL has a scheme, it is already absolute.
        /// If the base URL is empty, there is nothing to resolve against.
        if (schemeLength(relative) > 0 || base.empty())
        {
            append(res_data, relative);
            return;
        }

        /// A protocol-relative URL: take the scheme from the base URL.
        /// If the base URL has no scheme, there is nothing to add.
        if (relative.starts_with("//"))
        {
            std::string_view scheme = base.substr(0, schemeLength(base));
            if (!scheme.empty())
            {
                append(res_data, scheme);
                append(res_data, ":");
            }
            append(res_data, relative);
            return;
        }

        /// An absolute path: take the scheme and the authority from the base URL.
        if (relative[0] == '/')
        {
            append(res_data, base.substr(0, authorityEnd(base)));
            append(res_data, relative);
            return;
        }

        /// A query string: replace the query string and the fragment of the base URL.
        if (relative[0] == '?')
        {
            size_t pos = base.find_first_of("?#");
            append(res_data, base.substr(0, pos == npos ? base.size() : pos));
            append(res_data, relative);
            return;
        }

        /// A fragment: replace the fragment of the base URL.
        if (relative[0] == '#')
        {
            size_t pos = base.find('#');
            append(res_data, base.substr(0, pos == npos ? base.size() : pos));
            append(res_data, relative);
            return;
        }

        /// A relative path: replace everything after the last slash of the base URL's path.
        /// The query string and the fragment of the base URL are dropped.

        size_t authority_end = authorityEnd(base);

        size_t path_end = base.find_first_of("?#", authority_end);
        if (path_end == npos)
            path_end = base.size();

        std::string_view path = base.substr(authority_end, path_end - authority_end);
        size_t last_slash = path.rfind('/');

        /// The position right after the last slash of the base URL's directory,
        /// or the end of the authority if the path has no slashes (then a slash is added below).
        size_t directory_end = last_slash == npos ? authority_end : authority_end + last_slash + 1;

        /// Collapse leading '.' and '..' segments of the relative URL against the base URL's directory.
        /// Extra '..' segments that would escape past the root, as well as dot segments
        /// in the middle of the relative URL, are kept as is.
        while (true)
        {
            if (relative == ".")
            {
                relative = {};
            }
            else if (relative.starts_with("./"))
            {
                relative.remove_prefix(2);
            }
            else if (relative == ".." || relative.starts_with("../"))
            {
                /// The directory is already at the root of the base URL.
                if (directory_end <= authority_end + 1)
                    break;

                size_t prev_slash = base.rfind('/', directory_end - 2);
                if (prev_slash == npos || prev_slash < authority_end)
                    break;

                directory_end = prev_slash + 1;
                relative.remove_prefix(std::min<size_t>(3, relative.size()));
            }
            else
                break;
        }

        append(res_data, base.substr(0, directory_end));
        if (directory_end == authority_end)
            append(res_data, "/");
        append(res_data, relative);
    }
};

REGISTER_FUNCTION(ResolveRelativeURL)
{
    /// resolveRelativeURL documentation
    FunctionDocumentation::Description description_resolveRelativeURL = R"(
Resolves a relative URL (e.g. taken from an HTML page) against a base URL, similarly to how a browser resolves links.

The function prefers implementation simplicity over full compliance with RFC 3986:
- If `relative` has a scheme (`http://`, `ftp://`, ...), it is returned as is.
- If `relative` is protocol-relative (`//example.com/path`), only the scheme is taken from `base`.
- If `relative` starts with `/`, the scheme and the authority (host) are taken from `base`.
- If `relative` starts with `?` or `#`, it replaces the query string or the fragment of `base` respectively.
- Otherwise, `relative` replaces everything after the last slash of the path of `base`.

Leading `./` and `../` segments of `relative` are collapsed against the directory of `base`,
while dot segments in the middle of the path are kept as is.
If `relative` is empty, `base` is returned. If `base` is empty, `relative` is returned.
The function does not take HTML's `base` tag into account.
    )";
    FunctionDocumentation::Syntax syntax_resolveRelativeURL = "resolveRelativeURL(relative, base)";
    FunctionDocumentation::Arguments arguments_resolveRelativeURL = {
        {"relative", "Relative URL.", {"String"}},
        {"base", "Base URL against which the relative URL is resolved.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_resolveRelativeURL = {"Returns the resolved absolute URL.", {"String"}};
    FunctionDocumentation::Examples examples_resolveRelativeURL = {
    {
        "Usage example",
        R"(
SELECT
    resolveRelativeURL('image.gif', 'https://clickhouse.com/blog/') AS relative,
    resolveRelativeURL('/image.gif', 'https://clickhouse.com/blog/') AS absolute_path,
    resolveRelativeURL('//example.com/image.gif', 'https://clickhouse.com/blog/') AS protocol_relative,
    resolveRelativeURL('ftp://example.com/image.gif', 'https://clickhouse.com/blog/') AS already_absolute
FORMAT Vertical;
        )",
        R"(
Row 1:
──────
relative:          https://clickhouse.com/blog/image.gif
absolute_path:     https://clickhouse.com/image.gif
protocol_relative: https://example.com/image.gif
already_absolute:  ftp://example.com/image.gif
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_resolveRelativeURL = {26, 7};
    FunctionDocumentation::Category category_resolveRelativeURL = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_resolveRelativeURL = {description_resolveRelativeURL, syntax_resolveRelativeURL, arguments_resolveRelativeURL, {}, returned_value_resolveRelativeURL, examples_resolveRelativeURL, introduced_in_resolveRelativeURL, category_resolveRelativeURL};

    factory.registerFunction<FunctionResolveRelativeURL>(documentation_resolveRelativeURL);
}

}
