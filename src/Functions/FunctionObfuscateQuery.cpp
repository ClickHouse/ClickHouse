#include <Functions/FunctionObfuscateQuery.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Parsers/obfuscateQueries.h>
#include <IO/WriteBufferFromString.h>
#include <string_view>
#include <Functions/FunctionHelpers.h>
#include <Common/SipHash.h>
#include <Common/randomSeed.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

// Keep namespace DB open for the function implementations below.

UInt64 FunctionObfuscateQuery::hashStringToUInt64(std::string_view s)
{
    SipHash h;
    h.update(s);
    return h.get64();
}


DataTypePtr FunctionObfuscateQuery::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} requires at least 1 argument", getName());

    if (arguments.size() > 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} requires at most 2 arguments", getName());

    if (!isString(arguments[0].type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "First argument of function {} must be String", getName());

    if (mode == Mode::WithProvidedSeed && arguments.size() == 2)
    {
        if (!isInteger(arguments[1].type) && !isString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Second argument of function {} must be an integer type or String", getName());
    }

    return std::make_shared<DataTypeString>();
}

ColumnPtr FunctionObfuscateQuery::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    const ColumnPtr col_query = arguments[0].column;

    auto col_res = ColumnString::create();

    /// Two main paths: column of strings, or const column.
    if (const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get()))
    {
    // use getDataAt(i) directly; no need to fetch raw buffers here

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view src = col_query_string->getDataAt(i);
                auto extractSeedFromArg = [&](const ColumnPtr & col, size_t row) -> UInt64 {
                    // Handle constant columns first (covers literal seeds of any integer type).
                    if (const ColumnConst * col_c_any = typeid_cast<const ColumnConst *>(col.get()))
                    {
                        const IColumn & data_col = col_c_any->getDataColumn();
                        if (const ColumnString * s = checkAndGetColumn<ColumnString>(&data_col))
                            return hashStringToUInt64(s->getDataAt(0));
                        // For numeric consts, ColumnConst provides getUInt
                        return col_c_any->getUInt(0);
                    }

                    // Non-const string column
                    if (const ColumnString * col_s = checkAndGetColumn<ColumnString>(col.get()))
                        return hashStringToUInt64(col_s->getDataAt(row));

                    // Non-const numeric columns: use generic getUInt
                    return col->getUInt(row);
                };

                UInt64 seed_value = 0;
                if (mode == Mode::WithProvidedSeed && arguments.size() >= 2)
                {
                    seed_value = extractSeedFromArg(arguments[1].column, i);
                }
                else if (mode == Mode::WithTag && arguments.size() >= 2)
                {
                    UInt64 tag_hash = extractSeedFromArg(arguments[1].column, i);
                    seed_value = randomSeed() ^ tag_hash ^ static_cast<UInt64>(i + 1);
                }
                else
                {
                    seed_value = randomSeed() ^ hashStringToUInt64(src) ^ static_cast<UInt64>(i + 1);
                }

            WordMap obfuscate_map;
            WordSet used_nouns;

            SipHash hash_func;
            hash_func.update(seed_value);

            WriteBufferFromOwnString wb;
            obfuscateQueries(src, wb, obfuscate_map, used_nouns, hash_func, [](std::string_view){ return false; });
            auto & out = wb.str();

            col_res->insertData(out.data(), out.size());
        }
    }
    else if (const ColumnConst * col_query_const = checkAndGetColumnConst<ColumnString>(col_query.get()))
    {
        const String & const_query = col_query_const->getValue<String>();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto extractSeedFromArg = [&](const ColumnPtr & col, size_t row) -> UInt64 {
                if (const ColumnConst * col_c_any = typeid_cast<const ColumnConst *>(col.get()))
                {
                    const IColumn & data_col = col_c_any->getDataColumn();
                    if (const ColumnString * s = checkAndGetColumn<ColumnString>(&data_col))
                        return hashStringToUInt64(s->getDataAt(0));
                    return col_c_any->getUInt(0);
                }

                if (const ColumnString * col_s = checkAndGetColumn<ColumnString>(col.get()))
                    return hashStringToUInt64(col_s->getDataAt(row));

                return col->getUInt(row);
            };

            UInt64 seed_value = 0;
            if (mode == Mode::WithProvidedSeed && arguments.size() >= 2)
            {
                seed_value = extractSeedFromArg(arguments[1].column, i);
            }
            else if (mode == Mode::WithTag && arguments.size() >= 2)
            {
                UInt64 tag_hash = extractSeedFromArg(arguments[1].column, i);
                seed_value = randomSeed() ^ tag_hash ^ static_cast<UInt64>(i + 1);
            }
            else
            {
                seed_value = randomSeed() ^ hashStringToUInt64(std::string_view(const_query)) ^ static_cast<UInt64>(i + 1);
            }

                WordMap obfuscate_map;
                WordSet used_nouns;

                SipHash hash_func;
                hash_func.update(seed_value);

                WriteBufferFromOwnString wb;
                obfuscateQueries(std::string_view(const_query), wb, obfuscate_map, used_nouns, hash_func, [](std::string_view){ return false; });
            auto & out = wb.str();

            col_res->insertData(out.data(), out.size());
        }
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_query->getName(), getName());
    }

    return col_res;
}

namespace
{

class FunctionObfuscateQueryAdaptor : public IFunction
{
private:
    FunctionObfuscateQuery::Mode getMode(size_t num_args) const
    {
        if (num_args == 2)
        {
            // Check if second argument is seed (obfuscateQueryWithSeed) or tag (obfuscateQuery with tag)
            // We need to look at the function name
            if (name == "obfuscateQueryWithSeed")
                return FunctionObfuscateQuery::Mode::WithProvidedSeed;
            else
                return FunctionObfuscateQuery::Mode::WithTag;
        }
        return FunctionObfuscateQuery::Mode::WithRandomSeed;
    }

public:
    explicit FunctionObfuscateQueryAdaptor(String name_) : name(std::move(name_)) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override
    {
        // Only deterministic for obfuscateQueryWithSeed with constant seed
        return name == "obfuscateQueryWithSeed";
    }

    bool isDeterministic() const override
    {
        // obfuscateQueryWithSeed is deterministic when provided with constant seed
        return name == "obfuscateQueryWithSeed";
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        // Only obfuscateQueryWithSeed is deterministic in scope of query.
        return name == "obfuscateQueryWithSeed";
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least 1 argument", getName());

        if (arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at most 2 arguments", getName());

        // First argument must be string
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "First argument of function {} must be String", getName());

        // For obfuscateQueryWithSeed, second argument must be integer or String
        if (name == "obfuscateQueryWithSeed" && arguments.size() == 2)
        {
            if (!isInteger(arguments[1].type) && !isString(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Second argument of function {} must be an integer type or String", getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        FunctionObfuscateQuery::Mode mode = getMode(arguments.size());
        FunctionObfuscateQuery impl(name, mode);
        return impl.executeImpl(arguments, nullptr, input_rows_count);
    }

private:
    String name;
};

}

REGISTER_FUNCTION(obfuscateQuery)
{
    // Inline description and syntax into the documentation initializer to avoid unused-local warnings

    FunctionDocumentation::Arguments obfuscate_query_arguments = {
        {"query", "The SQL query to obfuscate.", {"String"}},
        {"tag", "Optional. A value to prevent common subexpression elimination when the same function call is used multiple times.", {}}
    };

    FunctionDocumentation::ReturnedValue obfuscate_query_returned_value = {
        "The obfuscated query with identifiers and literals replaced while preserving the original query structure.",
        {"String"}
    };

    FunctionDocumentation::Examples obfuscate_query_examples = {
        {"Basic usage",
         "SELECT obfuscateQuery('SELECT name, age FROM users WHERE age > 30')",
         "SELECT fruit, number FROM table WHERE number > 12"},
        {"With tag to prevent common subexpression elimination",
         "SELECT obfuscateQuery('SELECT * FROM t', 1), obfuscateQuery('SELECT * FROM t', 2)",
         "SELECT a FROM b, SELECT c FROM d"},
        {"Different rows produce different results",
         "SELECT obfuscateQuery('SELECT 1') AS a, obfuscateQuery('SELECT 1') AS b",
         "A B"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation::Parameters parameters = {};

    FunctionDocumentation obfuscate_query_documentation = {
        R"(
Obfuscates a SQL query by replacing identifiers with random words and literals with random values while preserving query structure.

This function is useful for anonymizing queries before logging or sharing them for debugging purposes.
Different rows will produce different obfuscated results even for the same input query, which helps
maintain privacy when working with multiple queries.

The optional `tag` parameter prevents common subexpression elimination when the same function call
is used multiple times in a query. This ensures that each invocation produces a different obfuscated result.

Features:
- Replaces table names, column names, and aliases with random words
- Replaces numeric and string literals with random values
- Preserves the overall query structure and SQL syntax
- Produces different results for different rows
)",
        "obfuscateQuery(query[, tag])",
        obfuscate_query_arguments,
        parameters,
        obfuscate_query_returned_value,
        obfuscate_query_examples,
        introduced_in,
        category
    };

    factory.registerFunction("obfuscateQuery",
        [](ContextPtr){ return std::make_shared<FunctionObfuscateQueryAdaptor>("obfuscateQuery"); },
        obfuscate_query_documentation);

    // Register obfuscateQueryWithSeed with its own documentation
    FunctionDocumentation::Arguments obfuscate_query_with_seed_arguments = {
        {"query", "The SQL query to obfuscate.", {"String"}},
        {"seed", "The seed for obfuscation. The same seed produces deterministic results.", {"Integer", "String"}}
    };

    FunctionDocumentation::ReturnedValue obfuscate_query_with_seed_returned_value = {
        "The obfuscated query, deterministically generated based on the provided seed.",
        {"String"}
    };

    FunctionDocumentation::Examples obfuscate_query_with_seed_examples = {
        {"Deterministic obfuscation with integer seed",
         "SELECT obfuscateQueryWithSeed('SELECT name FROM users', 42)",
         "SELECT fruit FROM table"},
        {"Deterministic obfuscation with string seed",
         "SELECT obfuscateQueryWithSeed('SELECT id, value FROM data', 'myseed')",
         "SELECT a, b FROM c"},
        {"Same seed produces same result",
         "SELECT obfuscateQueryWithSeed('SELECT 1', 100) = obfuscateQueryWithSeed('SELECT 1', 100)",
         "true"}
    };

    FunctionDocumentation obfuscate_query_with_seed_documentation = {
        R"(
Obfuscates a SQL query using a specified seed for deterministic results.

Unlike `obfuscateQuery()`, this function produces deterministic results when given the same seed.
This is useful when you need consistent obfuscation across multiple runs or when you want to
reproduce the same obfuscated query for testing or debugging purposes.

Features:
- Deterministic obfuscation based on the provided seed
- Same seed always produces the same obfuscated result
- Different seeds produce different results
- Preserves query structure like obfuscateQuery()

Use cases:
- Reproducible test cases
- Consistent anonymization across multiple runs
- Debugging with consistent obfuscated queries
)",
        "obfuscateQueryWithSeed(query, seed)",
        obfuscate_query_with_seed_arguments,
        parameters,
        obfuscate_query_with_seed_returned_value,
        obfuscate_query_with_seed_examples,
        introduced_in,
        category
    };

    factory.registerFunction("obfuscateQueryWithSeed",
        [](ContextPtr){ return std::make_shared<FunctionObfuscateQueryAdaptor>("obfuscateQueryWithSeed"); },
        obfuscate_query_with_seed_documentation);
}

}
