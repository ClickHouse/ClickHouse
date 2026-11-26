#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/queryNormalization.h>


/** The function returns 64bit hash value that is identical for similar queries.
  * See also 'normalizeQuery'. This function is only slightly more efficient.
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

class FunctionNormalizedQueryHash final : public IFunction
{
private:
    bool keep_names;
    bool unordered;

    void process(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt64> & res_data,
        size_t input_rows_count) const
    {
        res_data.resize(input_rows_count);

        ColumnString::Offset prev_src_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset curr_src_offset = offsets[i];
            const char * begin = reinterpret_cast<const char *>(&data[prev_src_offset]);
            const char * end = reinterpret_cast<const char *>(&data[curr_src_offset]);
            res_data[i] = unordered ? normalizedQueryHashUnordered(begin, end) : normalizedQueryHash(begin, end, keep_names);
            prev_src_offset = curr_src_offset;
        }
    }
public:
    explicit FunctionNormalizedQueryHash(bool keep_names_, bool unordered_ = false) : keep_names(keep_names_), unordered(unordered_) {}

    String getName() const override
    {
        if (unordered)
            return "normalizedQueryHashUnordered";
        return keep_names ? "normalizedQueryHashKeepNames" : "normalizedQueryHash";
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Disable default Variant implementation for compatibility.
    /// Hash values must remain stable, so we don't want the Variant adaptor to change hash computation.
    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();
            typename ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);
            process(col->getChars(), col->getOffsets(), vec_res, input_rows_count);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

}


REGISTER_FUNCTION(NormalizedQueryHash)
{
    FunctionDocumentation::Description normalizedQueryHash_description = R"(
Returns identical 64 bit hash values without the values of literals for similar queries.
Can be helpful in analyzing query logs.
    )";
    FunctionDocumentation::Syntax normalizedQueryHash_syntax = "normalizedQueryHash(x)";
    FunctionDocumentation::Arguments normalizedQueryHash_arguments = {
        {"x", "Sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue normalizedQueryHash_returned_value = {"Returns a 64 bit hash value.", {"UInt64"}};
    FunctionDocumentation::Examples normalizedQueryHash_examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHash('SELECT 1 AS `xyz`') != normalizedQueryHash('SELECT 1 AS `abc`') AS res
        )",
        R"(
┌─res─┐
│   1 │
└─────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn normalizedQueryHash_introduced_in = {20, 8};
    FunctionDocumentation::Category normalizedQueryHash_category = FunctionDocumentation::Category::Other;
    FunctionDocumentation normalizedQueryHash_documentation = {normalizedQueryHash_description, normalizedQueryHash_syntax, normalizedQueryHash_arguments, {}, normalizedQueryHash_returned_value, normalizedQueryHash_examples, normalizedQueryHash_introduced_in, normalizedQueryHash_category};

    FunctionDocumentation::Description normalizedQueryHashKeepNames_description = R"(
Like [`normalizedQueryHash`](#normalizedQueryHash) it returns identical 64 bit hash values without the values of literals for similar queries, but it does not replace complex aliases (containing whitespace, more than two digits or at least 36 bytes long such as UUIDs) with a placeholder before hashing.
Can be helpful in analyzing query logs.
    )";
    FunctionDocumentation::Syntax normalizedQueryHashKeepNames_syntax = "normalizedQueryHashKeepNames(x)";
    FunctionDocumentation::Arguments normalizedQueryHashKeepNames_arguments = {
        {"x", "Sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue normalizedQueryHashKeepNames_returned_value = {"Returns a 64 bit hash value.", {"UInt64"}};
    FunctionDocumentation::Examples normalizedQueryHashKeepNames_examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHash('SELECT 1 AS `xyz123`') != normalizedQueryHash('SELECT 1 AS `abc123`') AS normalizedQueryHash;
SELECT normalizedQueryHashKeepNames('SELECT 1 AS `xyz123`') != normalizedQueryHashKeepNames('SELECT 1 AS `abc123`') AS normalizedQueryHashKeepNames;
        )",
        R"(
┌─normalizedQueryHash─┐
│                   0 │
└─────────────────────┘
┌─normalizedQueryHashKeepNames─┐
│                            1 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn normalizedQueryHashKeepNames_introduced_in = {21, 2};
    FunctionDocumentation::Category normalizedQueryHashKeepNames_category = FunctionDocumentation::Category::Other;
    FunctionDocumentation normalizedQueryHashKeepNames_documentation = {normalizedQueryHashKeepNames_description, normalizedQueryHashKeepNames_syntax, normalizedQueryHashKeepNames_arguments, {}, normalizedQueryHashKeepNames_returned_value, normalizedQueryHashKeepNames_examples, normalizedQueryHashKeepNames_introduced_in, normalizedQueryHashKeepNames_category};

    factory.registerFunction("normalizedQueryHashKeepNames", [](ContextPtr){ return std::make_shared<FunctionNormalizedQueryHash>(true); }, normalizedQueryHashKeepNames_documentation);
    factory.registerFunction("normalizedQueryHash", [](ContextPtr){ return std::make_shared<FunctionNormalizedQueryHash>(false); }, normalizedQueryHash_documentation);

    FunctionDocumentation::Description normalizedQueryHashUnordered_description = R"(
Like [`normalizedQueryHash`](#normalizedQueryHash) it returns identical 64 bit hash values without the values of literals for similar queries,
but it combines the hashes of the tokens with a sum, so the order of the tokens does not matter at all.
`SELECT a, b FROM t` and `SELECT b, a FROM t` get the same hash.

The query is not parsed, only split into tokens, so the function is lossy on purpose: any reordering of the same tokens gets the same hash,
including ones that change the meaning (`a - b` and `b - a`) or that are not valid SQL at all (`SELECT 1` and `1 SELECT`).
Use it to group a workload by shape, for example over `system.query_log`, and never to decide that two queries may be substituted for each other.
It never throws: text that does not tokenize cleanly is hashed as well.
    )";
    FunctionDocumentation::Syntax normalizedQueryHashUnordered_syntax = "normalizedQueryHashUnordered(x)";
    FunctionDocumentation::Arguments normalizedQueryHashUnordered_arguments = {
        {"x", "Sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue normalizedQueryHashUnordered_returned_value = {"Returns a 64 bit hash value.", {"UInt64"}};
    FunctionDocumentation::Examples normalizedQueryHashUnordered_examples = {
    {
        "Usage example",
        R"(
SELECT normalizedQueryHashUnordered('SELECT a, b FROM t WHERE x = 1') = normalizedQueryHashUnordered('SELECT b, a FROM t WHERE x = 2') AS res
        )",
        R"(
┌─res─┐
│   1 │
└─────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn normalizedQueryHashUnordered_introduced_in = {26, 10};
    FunctionDocumentation::Category normalizedQueryHashUnordered_category = FunctionDocumentation::Category::Other;
    FunctionDocumentation normalizedQueryHashUnordered_documentation = {normalizedQueryHashUnordered_description, normalizedQueryHashUnordered_syntax, normalizedQueryHashUnordered_arguments, {}, normalizedQueryHashUnordered_returned_value, normalizedQueryHashUnordered_examples, normalizedQueryHashUnordered_introduced_in, normalizedQueryHashUnordered_category};

    factory.registerFunction("normalizedQueryHashUnordered", [](ContextPtr){ return std::make_shared<FunctionNormalizedQueryHash>(false, true); }, normalizedQueryHashUnordered_documentation);
}

}
