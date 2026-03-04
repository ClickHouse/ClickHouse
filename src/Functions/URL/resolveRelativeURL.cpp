#include <Columns/ColumnString.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <base/find_symbols.h>
#include <Functions/URL/protocol.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/FunctionDocumentation.h>
#include <string_view>

namespace DB
{
using std::string_view;


namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

class FunctionResolveRelativeURL : public IFunction
{
public:
    static constexpr auto name = "resolveRelativeURL";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionResolveRelativeURL>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"relative_url", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"base_url", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        validateFunctionArguments(getName(), arguments, mandatory_args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr relative_url_column = arguments[0].column;
        // Both arguments can be constants, so there could be four separate implementations
        // if we want to optimize the constants cases whenever possible. For now, just keep
        // it simple (with default implementations for constants set to true).
        relative_url_column = relative_url_column->convertToFullColumnIfConst();

        ColumnPtr base_url_column = arguments[1].column;
        base_url_column = base_url_column->convertToFullColumnIfConst();

        const ColumnString * relative_url = checkAndGetColumn<ColumnString>(relative_url_column.get());

        if (!relative_url)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
        }

        const auto * base_url = checkAndGetColumn<ColumnString>(base_url_column.get());

        if (!base_url)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", arguments[1].column->getName(), getName());
        }

        auto col_res = ColumnString::create();

        ColumnString::Chars & vec_res = col_res->getChars();
        ColumnString::Offsets & offsets_res = col_res->getOffsets();
        vector(relative_url->getChars(), relative_url->getOffsets(), base_url->getChars(), base_url->getOffsets(), vec_res, offsets_res, input_rows_count);
        return col_res;
    }

    static void vector(
        const ColumnString::Chars & relative_url_data,
        const ColumnString::Offsets & relative_url_offsets,
        const ColumnString::Chars & base_url_data,
        const ColumnString::Offsets & base_url_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(relative_url_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset relative_url_prev_offset = 0;
        ColumnString::Offset base_url_prev_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset relative_url_offset = relative_url_offsets[i];
            ColumnString::Offset base_url_offset = base_url_offsets[i];

            const char * relative_url_begin = reinterpret_cast<const char *>(&relative_url_data[relative_url_prev_offset]);
            const char * relative_url_end = reinterpret_cast<const char *>(&relative_url_data[relative_url_offset]);

            const char * base_url_begin = reinterpret_cast<const char *>(&base_url_data[base_url_prev_offset]);
            const char * base_url_end = reinterpret_cast<const char *>(&base_url_data[base_url_offset]);

            size_t res_url_len = 0;
            resolveURL(relative_url_begin, relative_url_end, base_url_begin, base_url_end,
                res_data, res_offset, res_url_len);

            res_offset += res_url_len;
            res_offsets[i] = res_offset;

            relative_url_prev_offset = relative_url_offset;
            base_url_prev_offset = base_url_offset;
        }
    }

    /// Helper to remove "." and ".." segments from a path (RFC 3986 Section 5.2.4)
    static std::string removeDotSegements(std::string_view path) {
        if (path.empty()) return "";

        std::string result;
        size_t i = 0;
        while (i < path.size()) {
            if (path.substr(i, 3) == "../" || path.substr(i, 2) == "./") {
                i += (path[i+1] == '.' ? 3 : 2);
            } else if (path.substr(i, 3) == "/./") {
                i += 2;
            } else if (path.substr(i, 2) == "/." && i + 2 == path.size()) {
                path = "/"; i = 0;
            } else if (path.substr(i, 4) == "/../" || (path.substr(i, 3) == "/.." && i + 3 == path.size())) {
                if (path.substr(i, 3) == "/.." && i + 3 == path.size()) { path = "/"; i = 0; } else { i += 3; }
                size_t last_slash = result.find_last_of('/');
                if (last_slash != std::string::npos) result.erase(last_slash);
            } else if (path == "." || path == "..") {
                break;
            } else {
                size_t next_slash = path.find('/', i + (path[i] == '/' ? 1 : 0));
                size_t len = (next_slash == std::string_view::npos) ? path.size() - i : next_slash - i;
                result.append(path.substr(i, len));
                i += len;
            }
        }
        return result;
    }

    /// Helper to resolve relative URL, write result to data array at given offset
    static void resolveURL(
        const char* rel_beg, const char* rel_end,
        const char* base_beg, const char* base_end,
        ColumnString::Chars& result_data, size_t result_offset, size_t& result_length
    ) {
        using sv = std::string_view;
        sv rel(rel_beg, rel_end - rel_beg);
        sv base(base_beg, base_end - base_beg);

        // 1. URL components parser
        auto get_parts = [](sv u, sv& scheme, sv& auth, sv& path, sv& query, sv& frag) {
            size_t f = u.find('#'); if (f != sv::npos) { frag = u.substr(f); u = u.substr(0, f); }
            size_t q = u.find('?'); if (q != sv::npos) { query = u.substr(q); u = u.substr(0, q); }
            size_t s = u.find(':');
            size_t sl = u.find('/');
            if (s != sv::npos && (sl == sv::npos || s < sl)) { scheme = u.substr(0, s + 1); u = u.substr(s + 1); }
            if (u.starts_with("//")) {
                size_t a_end = u.find('/', 2);
                auth = u.substr(0, a_end);
                path = (a_end == sv::npos) ? "" : u.substr(a_end);
            } else { path = u; }
        };

        sv base_url_scheme;
        sv base_url_auth;
        sv base_url_path;
        sv base_url_query;
        sv base_url_frag;
        get_parts(base, base_url_scheme, base_url_auth, base_url_path, base_url_query, base_url_frag);

        sv relative_url_scheme;
        sv relative_url_auth;
        sv relative_url_path;
        sv relative_url_query;
        sv relative_url_frag;
        get_parts(rel, relative_url_scheme, relative_url_auth, relative_url_path, relative_url_query, relative_url_frag);

        // 2. Resolution Logic, per RFC 3986 Section 5.2.2 (strict)
        std::string target_url_scheme;
        std::string target_url_auth;
        std::string target_url_path;
        std::string target_url_query;
        std::string target_url_frag;

        if (!relative_url_scheme.empty()) {
            target_url_scheme = relative_url_scheme; target_url_auth = relative_url_auth;
            target_url_path = removeDotSegements(relative_url_path); target_url_query = relative_url_query;
        } else {
            target_url_scheme = base_url_scheme;
            if (!relative_url_auth.empty()) {
                target_url_auth = relative_url_auth; target_url_path = removeDotSegements(relative_url_path); target_url_query = relative_url_query;
            } else {
                target_url_auth = base_url_auth;
                if (relative_url_path.empty()) {
                    target_url_path = base_url_path;
                    target_url_query = (!relative_url_query.empty()) ? relative_url_query : base_url_query;
                } else {
                    if (relative_url_path.starts_with('/')) {
                        target_url_path = removeDotSegements(relative_url_path);
                    } else {
                        // Merge paths (5.2.3)
                        std::string merged;
                        if (!base_url_auth.empty() && base_url_path.empty()) merged = "/";
                        else {
                            size_t last = base_url_path.find_last_of('/');
                            if (last != std::string::npos) merged = std::string(base_url_path.substr(0, last + 1));
                        }
                        merged.append(relative_url_path);
                        target_url_path = removeDotSegements(merged);
                    }
                    target_url_query = relative_url_query;
                }
            }
        }
        target_url_frag = relative_url_frag;

        // 3. Recombine and Output to result
        std::string res = target_url_scheme + target_url_auth + target_url_path + target_url_query + target_url_frag;
        result_data.resize(result_offset + res.size());
        std::memcpy(&result_data[result_offset], res.data(), res.size());
        result_length = res.size();
    }

};

REGISTER_FUNCTION(ResolveRelativeURL)
{
    factory.registerFunction<FunctionResolveRelativeURL>(
        FunctionDocumentation{
            .description=R"(
Resolves a relative URL to its absolute form based on a given base URL.
This is particularly useful to resolve relative URLs from HTML pages.
For details, see Internet Standard RFC 3986 section 5.2-5.4. This
is a strict implementation of the RFC - some results may differ from
implementations that favor backward compatibility.
)",
            .syntax=R"(resolveRelativeURL(relative_url, base_url))",
            .arguments={{"relative_url", "The relative URL to be resolved.", {"String"}},
                {"base_url", "The base URL to be used for resolution.", {"String"}}},
            .returned_value= {"The resolved absolute URL.", {"String"}},
            .examples={{"Usage example",R"(
SELECT
    resolveRelativeURL('image.gif', 'http://click.com/blog/') AS absolute_url1,
    resolveRelativeURL('/image.gif', 'http://click.com/blog/') AS absolute_url2;
)",
            R"(
┌─absolute_url1───────────────────┬─absolute_url2──────────────┐
│ http://click.com/blog/image.gif │ http://click.com/image.gif │
└─────────────────────────────────┴────────────────────────────┘
)"}},
            .introduced_in={1,1},
            .category=FunctionDocumentation::Category::URL,
        }
    );
};

}
