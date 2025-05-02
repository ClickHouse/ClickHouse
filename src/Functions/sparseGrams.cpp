#include <optional>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Common/Exception.h>

#include <zlib.h>
#include <Poco/UTF8Encoding.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Functions that finds all substrings win minimal length n
  * such their border (n-1)-grams' hashes are more than hashes of every (n-1)-grams' in substring.
  * As a hash function use zlib crc32, which is crc32-ieee with 0xffffffff as initial value
  *
  * sparseGrams(s)
  */
namespace
{

using Pos = const char *;

template <bool is_utf8>
class SparseGramsImpl
{
private:
    Pos pos;
    Pos end;
    std::vector<UInt32> ngram_hashes;
    std::vector<size_t> utf8_offsets;
    size_t left;
    size_t right;
    UInt64 min_ngram_length = 3;
    UInt64 max_ngram_length = 100;

    void buildNgramHashes()
    {
        if constexpr (is_utf8)
        {
            Poco::UTF8Encoding encoder{};
            size_t byte_offset = 0;
            while (pos + byte_offset < end)
            {
                utf8_offsets.push_back(byte_offset);
                auto len = encoder.sequenceLength(reinterpret_cast<const unsigned char *>(pos + byte_offset), end - pos - byte_offset);
                if (len < 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect utf8 symbol");
                byte_offset += len;
            }
            if (pos + byte_offset != end)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect utf8 symbol");

            utf8_offsets.push_back(byte_offset);

            if (utf8_offsets.size() >= min_ngram_length)
                ngram_hashes.reserve(utf8_offsets.size() - min_ngram_length + 1);
            for (size_t i = 0; i + min_ngram_length - 1 < utf8_offsets.size(); ++i)
                ngram_hashes.push_back(crc32_z(
                    0UL,
                    reinterpret_cast<const unsigned char *>(pos + utf8_offsets[i]),
                    utf8_offsets[i + min_ngram_length - 1] - utf8_offsets[i]));
        }
        else
        {
            if (pos + min_ngram_length <= end)
                ngram_hashes.reserve(end - pos - min_ngram_length + 1);
            for (size_t i = 0; pos + i + min_ngram_length - 2 < end; ++i)
                ngram_hashes.push_back(crc32_z(0L, reinterpret_cast<const unsigned char *>(pos + i), min_ngram_length - 1));
        }
    }

    std::optional<std::pair<size_t, size_t>> getNextIndices()
    {
        chassert(right > left);
        while (left < ngram_hashes.size())
        {
            while (right < ngram_hashes.size() && right <= left + max_ngram_length - min_ngram_length + 1)
            {
                if (right > left + 1)
                {
                    if (ngram_hashes[left] < ngram_hashes[right - 1])
                        break;

                    if (ngram_hashes[right] < ngram_hashes[right - 1])
                    {
                        ++right;
                        continue;
                    }
                }

                return {{left, right++}};
            }
            ++left;
            right = left + 1;
        }

        return std::nullopt;
    }

public:
    static constexpr auto name = is_utf8 ? "sparseGramsUTF8" : "sparseGrams";
    static constexpr auto strings_argument_position = 0uz;
    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }
    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"s", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        FunctionArgumentDescriptors optional_args{
            {"min_ngram_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), isColumnConst, "const Number"},
            {"max_ngram_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), isColumnConst, "const Number"},
        };

        validateFunctionArguments(func, arguments, mandatory_args, optional_args);
    }

    void init(const ColumnsWithTypeAndName & arguments, bool /*max_substrings_includes_remaining_string*/)
    {
        if (arguments.size() > 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, must be from 1 to 3",
                name,
                arguments.size());

        if (arguments.size() >= 2)
            min_ngram_length = arguments[1].column->getUInt(0);

        if (min_ngram_length < 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'min_ngram_length' must be greater or equal to 3");

        if (arguments.size() == 3)
            max_ngram_length = arguments[2].column->getUInt(0);

        if (max_ngram_length < min_ngram_length)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'max_ngram_length' must be greater or equal to 'min_ngram_length'");
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        left = 0;
        right = 1;

        ngram_hashes.clear();
        if constexpr (is_utf8)
            utf8_offsets.clear();

        buildNgramHashes();
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        auto result = getNextIndices();
        if (!result)
            return false;

        auto [iter_left, iter_right] = *result;

        if constexpr (is_utf8)
        {
            token_begin = pos + utf8_offsets[iter_left];
            token_end = pos + utf8_offsets[iter_right + min_ngram_length - 1];
        }
        else
        {
            token_begin = pos + iter_left;
            token_end = pos + iter_right + min_ngram_length - 1;
        }
        return true;
    }
};

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
                end = reinterpret_cast<Pos>(&src_data[current_src_offset]) - 1;
                impl.set(start, end);
                while (impl.get(start, end))
                    res_data.push_back(crc32_z(0UL, reinterpret_cast<const unsigned char *>(start), end - start));

                res_offsets_data.push_back(res_data.size());
            }

            return ColumnArray::create(std::move(col_res), std::move(col_res_offsets));
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);
    }
};

using FunctionSparseGrams = FunctionTokens<SparseGramsImpl<false>>;
using FunctionSparseGramsUTF8 = FunctionTokens<SparseGramsImpl<true>>;

}

REGISTER_FUNCTION(SparseGrams)
{
    const FunctionDocumentation description = {
        .description=R"(Finds all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.)",
        .arguments={
            {"s", "An input string"},
            {"min_ngram_length", "The minimum length of extracted ngram. The default and minimal value is 3"},
            {"max_ngram_length", "The maximum length of extracted ngram. The default value is 100. Should be not less than 'min_ngram_length'"},
        },
        .returned_value{"An array of selected substrings"},
        .category{"String"}
    };
    const FunctionDocumentation hashes_description{
        .description = R"(Finds hashes of all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.)",
        .arguments = description.arguments,
        .returned_value = "An array of selected substrings hashes",
        .category = description.category};

    factory.registerFunction<FunctionSparseGrams>(description);
    factory.registerFunction<FunctionSparseGramsUTF8>(description);

    factory.registerFunction<SparseGramsHashes<false>>(hashes_description);
    factory.registerFunction<SparseGramsHashes<true>>(hashes_description);
}

}
