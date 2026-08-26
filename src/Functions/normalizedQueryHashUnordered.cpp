#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryNormalization.h>


/// normalizedQueryHash that does not care about the order inside expression lists.

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool implicit_select;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SYNTAX_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int TOO_SLOW_PARSING;
}

namespace
{

enum class ErrorHandling : uint8_t
{
    Exception,
    Null
};

bool isParsingError(int code)
{
    return code == ErrorCodes::SYNTAX_ERROR || code == ErrorCodes::TOO_DEEP_RECURSION || code == ErrorCodes::TOO_SLOW_PARSING;
}

class FunctionNormalizedQueryHashUnordered final : public IFunction
{
public:
    FunctionNormalizedQueryHashUnordered(ContextPtr context, String name_, ErrorHandling error_handling_)
        : name(std::move(name_)), error_handling(error_handling_)
    {
        const Settings & settings = context->getSettingsRef();
        max_query_size = settings[Setting::max_query_size];
        max_parser_depth = settings[Setting::max_parser_depth];
        max_parser_backtracks = settings[Setting::max_parser_backtracks];
        implicit_select = settings[Setting::implicit_select];
        allow_settings_after_format_in_insert = settings[Setting::allow_settings_after_format_in_insert];
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    /// the adaptor wraps it in Nullable itself, otherwise a Dynamic argument would make the result Dynamic
    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeUInt64>(); }

    /// same as normalizedQueryHash: a hash must stay stable, the Variant adaptor would compute it per variant
    bool useDefaultImplementationForVariant() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, args);

        DataTypePtr result_type = std::make_shared<DataTypeUInt64>();
        if (error_handling == ErrorHandling::Null)
            return std::make_shared<DataTypeNullable>(result_type);
        return result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr col_query = arguments[0].column;
        const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get());
        if (!col_query_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_query->getName(), getName());

        ColumnUInt8::MutablePtr col_null_map;
        if (error_handling == ErrorHandling::Null)
            col_null_map = ColumnUInt8::create(input_rows_count, false);

        auto col_res = ColumnUInt64::create(input_rows_count, 0);
        auto & res_data = col_res->getData();

        const ColumnString::Chars & data = col_query_string->getChars();
        const ColumnString::Offsets & offsets = col_query_string->getOffsets();

        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = begin + offsets[i] - prev_offset;
            prev_offset = offsets[i];

            ParserQuery parser(end, allow_settings_after_format_in_insert, implicit_select);
            ASTPtr ast;
            try
            {
                ast = parseQuery(parser, begin, end, /*query_description*/ {}, max_query_size, max_parser_depth, max_parser_backtracks);
            }
            catch (const Exception & e)
            {
                /// anything else, a memory limit for one, is not this row being unparseable
                if (error_handling == ErrorHandling::Exception || !isParsingError(e.code()))
                    throw;

                col_null_map->getData()[i] = 1;
                continue;
            }

            res_data[i] = unorderedQueryHash(*ast);
        }

        if (error_handling == ErrorHandling::Null)
            return ColumnNullable::create(std::move(col_res), std::move(col_null_map));
        return col_res;
    }

private:
    String name;
    ErrorHandling error_handling;

    size_t max_query_size;
    size_t max_parser_depth;
    size_t max_parser_backtracks;
    bool implicit_select;
    bool allow_settings_after_format_in_insert;
};

}

REGISTER_FUNCTION(normalizedQueryHashUnordered)
{
    FunctionDocumentation::Description description = R"(
Like [`normalizedQueryHash`](#normalizedQueryHash), it returns identical 64 bit hash values for similar queries without the values of literals,
but the query is parsed first and every expression list is sorted, so the order inside a list does not reach the hash either.
`SELECT a, b FROM t` and `SELECT b, a FROM t` get the same hash.

The rule is applied to every expression list, including the ones whose order does change the result, such as `ORDER BY` and the arguments of a
function: `SELECT a - b` and `SELECT b - a` also get the same hash. The function is therefore lossy on purpose - use it to group a workload by
shape, for example over `system.query_log`, and never to decide that two queries may be substituted for each other.

The argument is parsed as ClickHouse SQL under the current session's [`max_query_size`](/operations/settings/settings#max_query_size),
[`max_parser_depth`](/operations/settings/settings#max_parser_depth), [`max_parser_backtracks`](/operations/settings/settings#max_parser_backtracks)
[`implicit_select`](/operations/settings/settings#implicit_select) and
[`allow_settings_after_format_in_insert`](/operations/settings/settings#allow_settings_after_format_in_insert), not under the settings the query
originally ran with.

Throws in case of a parsing error.
    )";
    FunctionDocumentation::Syntax syntax = "normalizedQueryHashUnordered(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a 64 bit hash value.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t WHERE x = 1') = normalizedQueryHashUnordered('SELECT b, a FROM t WHERE x = 2') AS res;
        )",
        R"(
┌─res─┐
│   1 │
└─────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction(
        "normalizedQueryHashUnordered",
        [](ContextPtr context)
        { return std::make_shared<FunctionNormalizedQueryHashUnordered>(context, "normalizedQueryHashUnordered", ErrorHandling::Exception); },
        documentation);
}

REGISTER_FUNCTION(normalizedQueryHashUnorderedOrNull)
{
    FunctionDocumentation::Description description = R"(
Like [`normalizedQueryHashUnordered`](#normalizedQueryHashUnordered), but returns `NULL` instead of throwing in case of a parsing error.

This is the variant to use over `system.query_log`, which also stores queries that failed to parse and queries truncated by
the [`log_queries_cut_to_length`](/operations/settings/settings#log_queries_cut_to_length) setting.

`query_log.query` holds the text as it was submitted, so a row gives `NULL` whenever that text is not ClickHouse SQL for the current session:
a query written in another dialect, or one that needed a higher `max_parser_depth` than this session allows. To hash the ClickHouse SQL of such
queries instead, turn on [`log_formatted_queries`](/operations/settings/settings#log_formatted_queries) and pass `formatted_query`.
    )";
    FunctionDocumentation::Syntax syntax = "normalizedQueryHashUnorderedOrNull(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a 64 bit hash value, or `NULL` if the query cannot be parsed.", {"Nullable(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHashUnorderedOrNull('SELECT * FROM') AS res;
        )",
        R"(
┌──res─┐
│ ᴺᵁᴸᴸ │
└──────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction(
        "normalizedQueryHashUnorderedOrNull",
        [](ContextPtr context)
        { return std::make_shared<FunctionNormalizedQueryHashUnordered>(context, "normalizedQueryHashUnorderedOrNull", ErrorHandling::Null); },
        documentation);
}

}
