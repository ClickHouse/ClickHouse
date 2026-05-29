#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/obfuscateQueries.h>
#include <IO/WriteBufferFromString.h>
#include <Functions/FunctionHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Formats/FormatFactory.h>
#include <Compression/CompressionFactory.h>
#include <Databases/DatabaseFactory.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Core/Settings.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Poco/String.h>
#include <boost/algorithm/string/split.hpp>
#include <string_view>
#include <optional>
#include <unordered_set>


extern const char * auto_time_zones[];

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

const KnownIdentifierFunc & getKnownIdentifierFunc()
{
    /// Built once per process: the registered names are populated at server startup
    /// and do not change afterwards. Mirrors the logic of `clickhouse-format --obfuscate`
    /// in programs/format/Format.cpp so that names of functions, aggregate functions,
    /// table functions, formats, storages, data types, settings, codecs, etc. are kept
    /// as-is instead of being replaced by random words.
    static const KnownIdentifierFunc func = []() -> KnownIdentifierFunc
    {
        auto names = std::make_shared<std::unordered_set<std::string>>();

        auto insert = [&](const auto & range) { names->insert(range.begin(), range.end()); };
        insert(StorageFactory::instance().getAllRegisteredNames());
        insert(DataTypeFactory::instance().getAllRegisteredNames());
        insert(Settings().getAllRegisteredNames());
        insert(MergeTreeSettings().getAllRegisteredNames());
        insert(MergeTreeIndexFactory::instance().getAllRegisteredNames());
        insert(CompressionCodecFactory::instance().getAllRegisteredNames());
        insert(DatabaseFactory::instance().getAllRegisteredNames());
        insert(DictionaryFactory::instance().getAllRegisteredNames());
        insert(DictionarySourceFactory::instance().getAllRegisteredNames());

        for (const auto * it = auto_time_zones; *it; ++it)
        {
            std::vector<std::string> split;
            boost::split(split, std::string(*it), [](char c) { return c == '/'; });
            for (const auto & word : split)
                if (!word.empty())
                    names->insert(word);
        }

        auto names_lowercase = std::make_shared<std::unordered_set<std::string>>();
        for (const auto & name : *names)
            names_lowercase->insert(Poco::toLower(name));

        return [names, names_lowercase](std::string_view name) -> bool
        {
            std::string what(name);
            if (FunctionFactory::instance().has(what)
                || AggregateFunctionFactory::instance().isAggregateFunctionName(what)
                || TableFunctionFactory::instance().isTableFunctionName(what)
                || FormatFactory::instance().isOutputFormat(what)
                || FormatFactory::instance().isInputFormat(what)
                || names->contains(what))
                return true;

            return names_lowercase->contains(Poco::toLower(what));
        };
    }();

    return func;
}

class ObfuscateQueryFunction
{
public:
    enum class Mode
    {
        WithRandomSeed,
        WithTag,
        WithProvidedSeed
    };

    explicit ObfuscateQueryFunction(String function_name_, Mode mode_) : function_name(std::move(function_name_)), mode(mode_) {}

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

private:
    String function_name;
    Mode mode;

    static UInt64 hashStringToUInt64(std::string_view s);
    static std::optional<UInt64> extractConstSeedFromArg(const ColumnPtr & col);
    static UInt64 extractSeedFromArg(const ColumnPtr & col, size_t row);
};

UInt64 ObfuscateQueryFunction::hashStringToUInt64(std::string_view s)
{
    SipHash h;
    h.update(s);
    return h.get64();
}

std::optional<UInt64> ObfuscateQueryFunction::extractConstSeedFromArg(const ColumnPtr & col)
{
    if (const ColumnConst * col_const = typeid_cast<const ColumnConst *>(col.get()))
    {
        const IColumn & data_col = col_const->getDataColumn();
        if (const ColumnString * string_col = checkAndGetColumn<ColumnString>(&data_col))
            return hashStringToUInt64(string_col->getDataAt(0));

        return col_const->getUInt(0);
    }

    return std::nullopt;
}

UInt64 ObfuscateQueryFunction::extractSeedFromArg(const ColumnPtr & col, size_t row)
{
    if (const ColumnString * string_col = checkAndGetColumn<ColumnString>(col.get()))
        return hashStringToUInt64(string_col->getDataAt(row));

    return col->getUInt(row);
}


ColumnPtr ObfuscateQueryFunction::execute(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    const ColumnPtr col_query = arguments[0].column;

    auto col_res = ColumnString::create();
    const std::optional<UInt64> const_seed_hash = (arguments.size() >= 2) ? extractConstSeedFromArg(arguments[1].column) : std::nullopt;
    const KnownIdentifierFunc & known_identifier_func = getKnownIdentifierFunc();

    /// Two main paths: column of strings, or a const column.
    if (const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get()))
    {
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view src = col_query_string->getDataAt(i);

            UInt64 seed_value = 0;
            if (mode == Mode::WithProvidedSeed && arguments.size() >= 2)
            {
                seed_value = const_seed_hash ? *const_seed_hash : extractSeedFromArg(arguments[1].column, i);
            }
            else
            {
                seed_value = thread_local_rng();
            }

            WordMap obfuscate_map;
            WordSet used_nouns;

            SipHash hash_func;
            hash_func.update(seed_value);

            WriteBufferFromOwnString wb;
            obfuscateQueries(src, wb, obfuscate_map, used_nouns, hash_func, known_identifier_func);
            auto & out = wb.str();

            col_res->insertData(out.data(), out.size());
        }
    }
    else if (const ColumnConst * col_query_const = checkAndGetColumnConst<ColumnString>(col_query.get()))
    {
        const String & const_query = col_query_const->getValue<String>();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt64 seed_value = 0;
            if (mode == Mode::WithProvidedSeed && arguments.size() >= 2)
            {
                seed_value = const_seed_hash ? *const_seed_hash : extractSeedFromArg(arguments[1].column, i);
            }
            else
            {
                seed_value = thread_local_rng();
            }

            WordMap obfuscate_map;
            WordSet used_nouns;

            SipHash hash_func;
            hash_func.update(seed_value);

            WriteBufferFromOwnString wb;
            obfuscateQueries(std::string_view(const_query), wb, obfuscate_map, used_nouns, hash_func, known_identifier_func);
            auto & out = wb.str();

            col_res->insertData(out.data(), out.size());
        }
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_query->getName(), function_name);
    }

    return col_res;
}

class ObfuscateQueryFunctionAdaptor : public IFunction
{
private:
    ObfuscateQueryFunction::Mode getMode(size_t num_args) const
    {
        if (name == "obfuscateQueryWithSeed")
        {
            if (num_args != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires exactly 2 arguments", getName());
            return ObfuscateQueryFunction::Mode::WithProvidedSeed;
        }

        if (num_args == 2)
            return ObfuscateQueryFunction::Mode::WithTag;

        return ObfuscateQueryFunction::Mode::WithRandomSeed;
    }

public:
    explicit ObfuscateQueryFunctionAdaptor(String name_) : name(std::move(name_)) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override
    {
        // Only deterministic for obfuscateQueryWithSeed with constant seed
        return name == "obfuscateQueryWithSeed";
    }

    bool useDefaultImplementationForLowCardinalityColumns() const override
    {
        return true;
    }

    bool canBeExecutedOnLowCardinalityDictionary() const override
    {
        // `obfuscateQuery` relies on per-row randomness, so dictionary execution
        // would collapse equal values and produce undesired behavior.
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
        if (name == "obfuscateQueryWithSeed")
        {
            if (arguments.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires exactly 2 arguments", getName());
        }
        else
        {
            if (arguments.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires at least 1 argument", getName());

            if (arguments.size() > 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires at most 2 arguments", getName());
        }

        // First argument must be string
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "First argument of function {} must be String", getName());

        // For both `obfuscateQuery` and `obfuscateQueryWithSeed`, second argument must be integer or String.
        if (arguments.size() == 2)
        {
            if (!isInteger(arguments[1].type) && !isString(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Second argument of function {} must be an integer type or String", getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ObfuscateQueryFunction::Mode mode = getMode(arguments.size());
        ObfuscateQueryFunction impl(name, mode);
        return impl.execute(arguments, input_rows_count);
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
        [](ContextPtr){ return std::make_shared<ObfuscateQueryFunctionAdaptor>("obfuscateQuery"); },
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
        [](ContextPtr){ return std::make_shared<ObfuscateQueryFunctionAdaptor>("obfuscateQueryWithSeed"); },
        obfuscate_query_with_seed_documentation);
}

}
