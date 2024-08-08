#include <optional>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>

#include "base/types.h"
#include <base/StringRef.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Functions that finds all substrings such their crc32-hash is more than crc32-hash of every bigram in substring.
  *
  * sparseGrams(s)
  */
namespace
{

template<bool is_utf8>
struct SparseGramsName;

template<>
struct SparseGramsName<true>
{
    static constexpr const char* value = "sparseGramsUTF8";
    static constexpr const char* value_hashes = "sparseGramsHashesUTF8";
};

template<>
struct SparseGramsName<false>
{
    static constexpr const char* value = "sparseGrams";
    static constexpr const char* value_hashes = "sparseGramsHashes";
};

using Pos = const char *;

template<bool is_utf8>
class SparseGramsImpl
{
private:
    Pos pos;
    Pos end;
    CRC32Hash hasher;
    std::vector<size_t> bigram_hashes;
    size_t left;
    size_t right;
    size_t minimal_length = 3;
    std::vector<size_t> utf8_offsets;

    void BuildBigramHashes()
    {
        if (pos == end)
        {
            utf8_offsets.push_back(0);
            return;
        }

        if constexpr (is_utf8)
        {
            size_t byte_offset = 0;
            while (pos + byte_offset != end)
            {
                utf8_offsets.push_back(byte_offset);
                auto elem = static_cast<unsigned char>(pos[byte_offset]);
                if (elem < 0x7f)
                {
                    byte_offset++;
                }
                else if (elem < 0x7ff)
                {
                    byte_offset += 2;
                }
                else if (elem < 0xffff)
                {
                    byte_offset += 3;
                }
                else if (elem < 0x10ffff)
                {
                    byte_offset += 4;
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect utf8 symbol");
                }
            }
            utf8_offsets.push_back(byte_offset);
            for (size_t i = 0; i < utf8_offsets.size() - 2; ++i)
            {
                bigram_hashes.push_back(calcHash(utf8_offsets[i], utf8_offsets[i + 2]));
            }
        }
        else
        {
            size_t i = 0;
            for (;; ++i)
            {
                utf8_offsets.push_back(i);
                if (pos + i + 1 == end)
                {
                    break;
                }
                bigram_hashes.push_back(calcHash(i, i + 2));
            }
            utf8_offsets.push_back(i + 1);
        }
    }

public:

    /// Calculates CRC32-hash from substring [it_left, it_right)
    unsigned calcHash(size_t it_left, size_t it_right)
    {
        auto substr_ref = StringRef(pos + it_left, it_right - it_left);
        return hasher(substr_ref);
    }

    std::optional<std::pair<size_t, size_t>> getNextIndices()
    {
        while (pos + utf8_offsets[left] != end)
        {
            while (right < utf8_offsets.size()
                && pos + utf8_offsets[right] - 1 != end
                && right - left < minimal_length)
            {
                right++;
            }

            if (right - left < minimal_length)
            {
                return std::nullopt;
            }

            size_t max_substr_bigram_hash = 0;
            for (size_t i = left; i < right - 1; ++i)
            {
                max_substr_bigram_hash = std::max(max_substr_bigram_hash, bigram_hashes[i]);
            }

            while (right - 1 < utf8_offsets.size()
                && pos + utf8_offsets[right - 1] != end
                && calcHash(
                    utf8_offsets[left],
                    utf8_offsets[right]) <= max_substr_bigram_hash)
            {
                max_substr_bigram_hash = std::max(max_substr_bigram_hash, bigram_hashes[right - 1]);
                right++;
            }

            if (right - 1 < utf8_offsets.size() && pos + utf8_offsets[right - 1] != end)
            {
                auto left_iter = utf8_offsets[left];
                auto right_iter = utf8_offsets[right];

                right++;
                if (right - 1 < utf8_offsets.size() && pos + utf8_offsets[right - 1] == end)
                {
                    left++;
                    right = left;
                }
                return std::pair{left_iter, right_iter};
            }
            left++;
            right = left;
        }

        return std::nullopt;
    }

    static constexpr auto name = SparseGramsName<is_utf8>::value;

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        checkArgumentsWithOptionalMaxSubstrings(func, arguments);
    }

    void init(const ColumnsWithTypeAndName & arguments, bool /*max_substrings_includes_remaining_string*/)
    {
        if (arguments.size() < 2)
        {
            return;
        }

        const auto * col = checkAndGetColumnConstIntOrUInt(arguments[1].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant int.", arguments[1].column->getName(), name);

        minimal_length = col->getValue<Int32>();
    }

    void setMinimalLenght(size_t length)
    {
        minimal_length = length;
    }

    static constexpr auto strings_argument_position = 0uz;

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        left = 0;
        right = 0;

        if constexpr (is_utf8)
        {
            utf8_offsets.clear();
        }

        BuildBigramHashes();
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        auto result = getNextIndices();
        if (!result)
        {
            return false;
        }
        auto [iter_left, iter_right] = *result;
        token_begin = pos + iter_left;
        token_end = pos + iter_right;
        return true;
    }
};

template <bool is_utf8>
class SparseGramsHashesImpl : public IFunction
{
public:
    static constexpr auto name = SparseGramsName<is_utf8>::value_hashes;

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<SparseGramsHashesImpl>(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        impl.init(arguments, false);

        auto col_res_nested = ColumnUInt64::create();
        auto & res_nested_data = col_res_nested->getData();

        auto col_res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = col_res_offsets->getData();
        res_offsets_data.reserve(input_rows_count);

        const auto & src = arguments[0];
        const auto & src_column = *src.column;

        if (const auto * col_non_const = typeid_cast<const ColumnString *>(&src_column))
        {

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view input_str = col_non_const->getDataAt(i).toView();
                std::vector<UInt64> integers = getHashes(input_str);
                res_nested_data.insert(integers.begin(), integers.end());
                res_offsets_data.push_back(integers.size());
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        return ColumnArray::create(std::move(col_res_nested), std::move(col_res_offsets));
    }

private:
    std::vector<size_t> getHashes(std::string_view input_str) const
    {
        impl.set(input_str.data(), input_str.data() + input_str.size());

        std::vector<size_t> result;
        while (true)
        {

            auto maybe_substr = impl.getNextIndices();
            if (!maybe_substr)
            {
                break;
            }
            auto [it_left, it_right] = *maybe_substr;
            result.push_back(impl.calcHash(it_left, it_right));
        }
        return result;
    }

    mutable SparseGramsImpl<is_utf8> impl;
};

using FunctionSparseGrams = FunctionTokens<SparseGramsImpl<false>>;
using FunctionSparseGramsUTF8 = FunctionTokens<SparseGramsImpl<true>>;

}

REGISTER_FUNCTION(SparseGrams)
{
    factory.registerFunction<FunctionSparseGrams>();
    factory.registerFunction<FunctionSparseGramsUTF8>();

    factory.registerFunction<SparseGramsHashesImpl<false>>();
    factory.registerFunction<SparseGramsHashesImpl<true>>();
}

}
