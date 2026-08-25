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


/// `normalizedQueryHash` that does not care about the order of a SELECT list. See `canonicalQueryHash`.

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool implicit_select;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

enum class ErrorHandling : uint8_t
{
    Exception,
    Null
};

class FunctionNormalizedQueryHashCanonical final : public IFunction
{
public:
    FunctionNormalizedQueryHashCanonical(ContextPtr context, String name_, ErrorHandling error_handling_)
        : name(std::move(name_)), error_handling(error_handling_)
    {
        const Settings & settings = context->getSettingsRef();
        max_query_size = settings[Setting::max_query_size];
        max_parser_depth = settings[Setting::max_parser_depth];
        max_parser_backtracks = settings[Setting::max_parser_backtracks];
        implicit_select = settings[Setting::implicit_select];
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

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

            ParserQuery parser(end, false, implicit_select);
            ASTPtr ast;
            try
            {
                ast = parseQuery(parser, begin, end, /*query_description*/ {}, max_query_size, max_parser_depth, max_parser_backtracks);
            }
            catch (...)
            {
                if (error_handling == ErrorHandling::Exception)
                    throw;

                col_null_map->getData()[i] = 1;
                continue;
            }

            res_data[i] = canonicalQueryHash(*ast);
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
};

}

REGISTER_FUNCTION(normalizedQueryHashCanonical)
{
    FunctionDocumentation::Description description = R"(
Like [`normalizedQueryHash`](#normalizedQueryHash), it returns identical 64 bit hash values for similar queries without the values of literals,
but it is computed over the parsed query, so it also ignores the order of elements in lists where the order does not change what the query does:
the `SELECT` expression list, `GROUP BY` keys and the operands of `and` and `or`.

`SELECT a, b FROM t` and `SELECT b, a FROM t` therefore get the same hash, even though the two queries return their columns in a different order.
Use this to group a workload by shape; do not use it to decide that two queries may be substituted for each other.

Throws in case of a parsing error.
    )";
    FunctionDocumentation::Syntax syntax = "normalizedQueryHashCanonical(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a 64 bit hash value.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHashCanonical('SELECT a, b FROM t WHERE x = 1') = normalizedQueryHashCanonical('SELECT b, a FROM t WHERE x = 2') AS res;
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
        "normalizedQueryHashCanonical",
        [](ContextPtr context)
        { return std::make_shared<FunctionNormalizedQueryHashCanonical>(context, "normalizedQueryHashCanonical", ErrorHandling::Exception); },
        documentation);
}

REGISTER_FUNCTION(normalizedQueryHashCanonicalOrNull)
{
    FunctionDocumentation::Description description = R"(
Like [`normalizedQueryHashCanonical`](#normalizedQueryHashCanonical), but returns `NULL` instead of throwing in case of a parsing error.

This is the variant to use over `system.query_log`, which also stores queries that failed to parse and queries truncated by
the [`log_queries_cut_to_length`](/operations/settings/settings#log_queries_cut_to_length) setting.
    )";
    FunctionDocumentation::Syntax syntax = "normalizedQueryHashCanonicalOrNull(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a 64 bit hash value, or `NULL` if the query cannot be parsed.", {"Nullable(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHashCanonicalOrNull('this is not a query') AS res;
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
        "normalizedQueryHashCanonicalOrNull",
        [](ContextPtr context)
        { return std::make_shared<FunctionNormalizedQueryHashCanonical>(context, "normalizedQueryHashCanonicalOrNull", ErrorHandling::Null); },
        documentation);
}

}
