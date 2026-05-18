#include <Common/Exception.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/sparseGramsImpl.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <bool is_utf8>
class SparseGramsHashes : public IFunction
{
public:
    static constexpr auto name = is_utf8 ? "sparseGramsHashesUTF8" : "sparseGramsHashes";
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    /// Disable default Variant implementation for compatibility.
    /// Hash values must remain stable, so we don't want the Variant adaptor to change hash computation.
    bool useDefaultImplementationForVariant() const override { return false; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<SparseGramsHashes>(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & args) const override
    {
        SparseGramsImpl<is_utf8>::checkArguments(*this, args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        SparseGramsImpl<is_utf8> impl;
        impl.init(arguments, false);

        CRC32CHasher hasher;

        auto col_res = ColumnUInt32::create();
        auto & res_data = col_res->getData();

        auto col_res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = col_res_offsets->getData();

        auto string_arg = arguments[impl.strings_argument_position].column.get();

        if (const auto * col_string = checkAndGetColumn<ColumnString>(string_arg))
        {
            const auto & src_data = col_string->getChars();
            const auto & src_offsets = col_string->getOffsets();

            res_offsets_data.reserve(input_rows_count);
            res_data.reserve(src_data.size());

            ColumnString::Offset current_src_offset = 0;
            Pos start{};
            Pos end{};

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                start = reinterpret_cast<Pos>(&src_data[current_src_offset]);
                current_src_offset = src_offsets[i];
                end = reinterpret_cast<Pos>(&src_data[current_src_offset]);
                impl.set(start, end);
                while (impl.get(start, end))
                    res_data.push_back(static_cast<UInt32>(hasher(start, end - start)));

                res_offsets_data.push_back(res_data.size());
            }

            return ColumnArray::create(std::move(col_res), std::move(col_res_offsets));
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);
    }
};

REGISTER_FUNCTION(SparseGramsHashes)
{
    FunctionDocumentation::Arguments arguments_sparse = {
        {"s", "An input string.", {"String"}},
        {"min_ngram_length", "Optional. The minimum length of extracted ngram. The default and minimal value is 3.", {"UInt*"}},
        {"max_ngram_length", "Optional. The maximum length of extracted ngram. The default value is 100. Should be not less than `min_ngram_length`.", {"UInt*"}},
        {"min_cutoff_length", "Optional. If specified, only n-grams with length greater or equal than `min_cutoff_length` are returned. The default value is the same as `min_ngram_length`. Should be not less than `min_ngram_length` and not greater than `max_ngram_length`.", {"UInt*"}}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;

    FunctionDocumentation::Description description_hashes = R"(
Finds hashes of all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses `CRC32` as a hash function.
)";
    FunctionDocumentation::Syntax syntax_hashes = "sparseGramsHashes(s[, min_ngram_length, max_ngram_length])";
    FunctionDocumentation::ReturnedValue returned_value_hashes = {"Returns an array of selected substrings CRC32 hashes.", {"Array(UInt32)"}};
    FunctionDocumentation::Examples examples_hashes = {
    {
        "Usage example",
        "SELECT sparseGramsHashes('alice', 3)",
        R"(
┌─sparseGramsHashes('alice', 3)──────────────────────┐
│ [1481062250,2450405249,4012725991,1918774096]      │
└────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_hashes = {description_hashes, syntax_hashes, arguments_sparse, {}, returned_value_hashes, examples_hashes, introduced_in, category};

    FunctionDocumentation::Description description_hashes_utf8 = R"(
Finds hashes of all substrings of a given UTF-8 string that have a length of at least `n`, where the hashes of the (n-1)-grams at the borders of the substring are strictly greater than those of any (n-1)-gram inside the substring.
Expects UTF-8 string, throws an exception in case of invalid UTF-8 sequence.
Uses `CRC32` as a hash function.
)";
    FunctionDocumentation::Syntax syntax_hashes_utf8 = "sparseGramsHashesUTF8(s[, min_ngram_length, max_ngram_length])";
    FunctionDocumentation::ReturnedValue returned_value_hashes_utf8 = {"Returns an array of selected UTF-8 substrings CRC32 hashes.", {"Array(UInt32)"}};
    FunctionDocumentation::Examples examples_hashes_utf8 = {
    {
        "Usage example",
        "SELECT sparseGramsHashesUTF8('алиса', 3)",
        R"(
┌─sparseGramsHashesUTF8('алиса', 3)─┐
│ [4178533925,3855635300,561830861] │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_hashes_utf8 = {description_hashes_utf8, syntax_hashes_utf8, arguments_sparse, {}, returned_value_hashes_utf8, examples_hashes_utf8, introduced_in, category};

    factory.registerFunction<SparseGramsHashes<false>>(documentation_hashes);
    factory.registerFunction<SparseGramsHashes<true>>(documentation_hashes_utf8);
}

}
