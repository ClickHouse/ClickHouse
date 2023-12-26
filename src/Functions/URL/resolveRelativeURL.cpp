#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <base/find_symbols.h>
#include <Functions/URL/protocol.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

bool isAbsoluteURL(const char * url_begin, const char * url_end)
{
    Pos pos = find_first_symbols<':'>(url_begin, url_end);
    if (pos == url_end)
    {
        return false;
    }
    return  pos + 2 < url_end && pos[1] == '/' && pos[2] == '/';
}

Pos getDomainNameBeginPos(const char * url_begin, const char * url_end)
{
    Pos pos = find_first_symbols<':'>(url_begin, url_end);
    if (pos == url_end || pos + 2 >= url_end)
    {
        return url_end;
    }
    bool has_subsequent_slash = pos[1] == '/' && pos[2] == '/';
    if (has_subsequent_slash)
    {
        return pos+3;
    }
    return url_end;
}

Pos getDomainNameEndPos(const char * url_begin, const char * url_end)
{
    Pos pos = find_first_symbols<':'>(url_begin, url_end);
    if (pos == url_end || pos + 2 >= url_end)
    {
        return url_end;
    }
    bool has_subsequent_slash = pos[1] == '/' && pos[2] == '/';
    if (has_subsequent_slash)
    {
        pos = find_first_symbols<'/'>(pos + 3, url_end);
        return pos;
    }
    return url_end;
}

class FunctionResolveRelativeURL : public IFunction
{
public:
    static constexpr auto name = "resolveRelativeURL";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionResolveRelativeURL>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isString(arguments[1]) && !isArray(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr relative_url_column = arguments[0].column;
        const ColumnPtr base_url_column = arguments[1].column;

        const ColumnString * relative_url = checkAndGetColumn<ColumnString>(relative_url_column.get());

        const auto * base_url = checkAndGetColumnConstData<ColumnString>(base_url_column.get());

        if (!relative_url)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
        }

        if (!base_url)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[1].column->getName(), getName());
        }

        auto col_res = ColumnString::create();

        ColumnString::Chars & vec_res = col_res->getChars();
        ColumnString::Offsets & offsets_res = col_res->getOffsets();
        vector(relative_url->getChars(), relative_url->getOffsets(), base_url->getChars(), base_url->getOffsets(), vec_res, offsets_res);
        return col_res;
    }

    static void vector(
        const ColumnString::Chars & relative_url_data,
        const ColumnString::Offsets & relative_url_offsets,
        const ColumnString::Chars & base_url_data,
        const ColumnString::Offsets & base_url_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(relative_url_data.size() / 5);
        res_offsets.resize(relative_url_offsets.size());

        ColumnString::Offset relative_url_prev_offset = 0;
        ColumnString::Offset base_url_prev_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < relative_url_offsets.size(); ++i)
        {
            ColumnString::Offset relative_url_offset = relative_url_offsets[i];
            ColumnString::Offset base_url_offset = base_url_offsets[i];


            const char * relative_url_begin = reinterpret_cast<const char *>(&relative_url_data[relative_url_prev_offset]);
            const char * relative_url_end = reinterpret_cast<const char *>(&relative_url_data[relative_url_offset]);
            size_t relative_size = relative_url_offset - relative_url_prev_offset - 1;
            const char * relative_url_ptr = relative_url_begin;

            const char * base_url_begin = reinterpret_cast<const char *>(&base_url_data[base_url_prev_offset]);
            const char * base_url_end = reinterpret_cast<const char *>(&base_url_data[base_url_offset]);
            size_t base_size = base_url_end - base_url_begin - 1;


            /// Relative url is an absolute url or relative url is not an absolute url, add relative url to the result directly
            if (isAbsoluteURL(relative_url_begin, relative_url_end)
                || !isAbsoluteURL(base_url_begin, base_url_end))
            {
                res_data.resize(res_offset + relative_size);
                memcpy(&res_data[res_offset], relative_url_begin, relative_size);
                res_offset += relative_size;
            }
            else if (*relative_url_ptr == '/')
            {
                relative_url_ptr++;
                Pos domain_pos;
                /// Relative url begins with double or more slashes
                if (relative_url_ptr != relative_url_end && *relative_url_ptr == '/')
                {
                    relative_url_ptr++;
                    /// Relative url begins with three or more slashes
                    if (*relative_url_ptr == '/')
                    {
                        domain_pos = getDomainNameEndPos(base_url_begin, base_url_end);
                        if (domain_pos == base_url_end)
                            continue;
                    }
                    /// Relative url begins with double slashes
                    else
                    {
                        relative_url_ptr = const_cast<char *>(find_first_not_symbols<'/'>(relative_url_ptr, relative_url_end));
                        /// Relative url only contains double slashes
                        if (*relative_url_ptr == '\0' || relative_url_ptr == relative_url_end)
                        {
                            domain_pos = base_url_end - 1;
                        }
                        else
                        {
                            domain_pos = getDomainNameBeginPos(base_url_begin, base_url_end);
                            if (domain_pos == base_url_end)
                                continue;
                        }
                    }

                }
                /// Relative url begin with single slash
                else
                {
                    domain_pos = getDomainNameEndPos(base_url_begin, base_url_end);
                    if (domain_pos == base_url_end)
                        continue;
                    relative_url_ptr--;
                }

                base_size = domain_pos - base_url_begin;
                relative_size = relative_url_end - relative_url_ptr - 1;
                res_data.resize(res_offset + base_size + relative_size + 1);
                memcpy(&res_data[res_offset], base_url_begin, base_size);
                res_offset += base_size;
                memcpy(&res_data[res_offset], relative_url_ptr, relative_size);
                res_offset += relative_size;
            }
            else
            {
                Pos base_url_last_slash;
                Pos base_url_domain_name_end = getDomainNameEndPos(base_url_begin, base_url_end);
                base_url_last_slash = find_last_symbols_or_null<'/'>(base_url_domain_name_end, base_url_end);
                if (base_url_last_slash == nullptr)
                {
                    base_url_last_slash = base_url_end - 1;
                }
                base_size = base_url_last_slash - base_url_begin;
                res_data.resize(res_offset + base_size + relative_size + 2);
                memcpy(&res_data[res_offset], base_url_begin, base_size);
                res_offset += base_size;
                memcpy(&res_data[res_offset], "/", 1);
                res_offset++;
                memcpy(&res_data[res_offset], relative_url_ptr, relative_size);
                res_offset += relative_size;
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;
            relative_url_prev_offset = relative_url_offset;
            base_url_prev_offset = base_url_offset;
        }
    }
};

REGISTER_FUNCTION(ResolveRelativeURL)
{
    factory.registerFunction<FunctionResolveRelativeURL>(
        FunctionDocumentation{
            .description=R"(
                resolves a relative URL to its absolute form based on a given base URL.
                This is particularly useful to resolve relative URLs from HTML pages.]
                 )",
            .examples={{"", "SELECT explain FROM (EXPLAIN AST SELECT * FROM system.numbers) WHERE explain LIKE '%Asterisk%'", ""}}
        }
    );
}

}

