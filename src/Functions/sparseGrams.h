#pragma once

#include <optional>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Common/UTF8Helpers.h>
#include <Common/Exception.h>
#include <base/types.h>
#include <Common/HashTable/Hash.h>

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

struct CRC32CHasher
{
    size_t operator()(const char* data, size_t length) const
    {
        return updateWeakHash32(reinterpret_cast<const UInt8*>(data), length, 0);
    }
};

using Pos = const char *;

template <bool is_utf8>
class SparseGramsImpl
{
private:
    struct SubString
    {
        size_t left_index;
        size_t right_index;
        size_t symbols_between;
    };

    CRC32CHasher hasher;

    Pos pos;
    Pos end;
    UInt64 min_ngram_length = 3;
    UInt64 max_ngram_length = 100;
    std::optional<UInt64> min_cutoff_length;

    /// Current batch of answers. The size of result can not be greater than `convex_hull`.
    /// The size of `convex_hull` should not be large, see comment to `convex_hull` for more details.
    std::vector<SubString> result;
    size_t iter_result = 0;

    struct PositionAndHash
    {
        size_t position;
        size_t left_ngram_position;
        size_t symbol_index;
        size_t hash;
    };

    class NGramSymbolIterator
    {
    public:
        NGramSymbolIterator() = default;

        NGramSymbolIterator(Pos data_, Pos end_, size_t n_)
            : data(data_), end(end_), n(n_)
        {
        }

        bool increment()
        {
            if (isEnd())
                return false;

            right_iterator = getNextPosition(right_iterator);

            if (++num_increments >= n)
                left_iterator = getNextPosition(left_iterator);

            return true;
        }

        bool isEnd() const
        {
            return data + right_iterator >= end;
        }

        std::pair<size_t, size_t> getNGramPositions() const
        {
            return {left_iterator, right_iterator};
        }

        size_t getRightSymbol() const
        {
            return num_increments;
        }

        size_t getNextPosition(size_t iterator) const
        {
            if constexpr (is_utf8)
                return iterator + UTF8::seqLength(data[iterator]);
            else
                return iterator + 1;
        }

    private:

        Pos data;
        Pos end;
        size_t n;
        size_t right_iterator = 0;
        size_t left_iterator = 0;
        size_t num_increments = 0;
    };

    /// The convex hull contains the maximum values ​​of the suffixes that start from the current right iterator.
    /// For example, if we have n-gram hashes like [1,5,2,4,1,3] and current right position is 4 (the last one)
    /// than our convex hull will consists of elements:
    /// [{position:1, hash:5}, {position:3, hash:4}, {position:4,hash:1}]
    /// Assuming that hashes are uniformly distributed, the expected size of convex_hull is N^{1/3},
    /// where N is the length of the string.
    /// Proof: https://math.stackexchange.com/questions/3469295/expected-number-of-vertices-in-a-convex-hull
    std::vector<PositionAndHash> convex_hull;
    NGramSymbolIterator symbol_iterator;

    /// Get the next batch of answers. Returns false if there can be no more answers.
    bool consume()
    {
        if (symbol_iterator.isEnd())
            return false;

        auto [ngram_left_position, right_position] = symbol_iterator.getNGramPositions();
        size_t right_symbol_index = symbol_iterator.getRightSymbol();
        size_t next_right_position = symbol_iterator.getNextPosition(right_position);
        size_t right_border_ngram_hash = hasher(pos + ngram_left_position, next_right_position - ngram_left_position);

        while (!convex_hull.empty() && convex_hull.back().hash < right_border_ngram_hash)
        {
            size_t possible_left_position = convex_hull.back().left_ngram_position;
            size_t possible_left_symbol_index = convex_hull.back().symbol_index;
            size_t length = right_symbol_index - possible_left_symbol_index + 2;
            if (length > max_ngram_length)
            {
                /// If the current length is greater than the current right position, it will be greater at future right positions, so we can just delete them all.
                convex_hull.clear();
                break;
            }
            result.push_back({
                .left_index = possible_left_position,
                .right_index = next_right_position,
                .symbols_between = length
            });
            convex_hull.pop_back();
        }

        if (!convex_hull.empty())
        {
            size_t possible_left_position = convex_hull.back().left_ngram_position;
            size_t possible_left_symbol_index = convex_hull.back().symbol_index;
            size_t length = right_symbol_index - possible_left_symbol_index + 2;
            if (length <= max_ngram_length)
                result.push_back({
                    .left_index = possible_left_position,
                    .right_index = next_right_position,
                    .symbols_between = length
                });
        }

        /// there should not be identical hashes in the convex hull. If there are, then we leave only the last one
        while (!convex_hull.empty() && convex_hull.back().hash == right_border_ngram_hash)
            convex_hull.pop_back();

        convex_hull.push_back(PositionAndHash{
            .position = right_position,
            .left_ngram_position = ngram_left_position,
            .symbol_index = right_symbol_index,
            .hash = right_border_ngram_hash
        });
        symbol_iterator.increment();
        return true;
    }

    std::optional<SubString> getNextIndices()
    {
        if (result.size() <= iter_result)
        {
            result.clear();
            iter_result = 0;

            if (!consume())
                return std::nullopt;

            return getNextIndices();
        }

        return result[iter_result++];
    }

public:
    static constexpr auto name = is_utf8 ? "sparseGramsUTF8" : "sparseGrams";
    static constexpr auto strings_argument_position = 0uz;
    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }
    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1}; }

    SparseGramsImpl() = default;
    explicit SparseGramsImpl(UInt64 min_ngram_length_, UInt64 max_ngram_length_, std::optional<UInt64> min_cutoff_length_)
        : min_ngram_length(min_ngram_length_)
        , max_ngram_length(max_ngram_length_)
        , min_cutoff_length(min_cutoff_length_)
    {
    }

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
        result.clear();
        convex_hull.clear();
        iter_result = 0;

        pos = pos_;
        end = end_;

        symbol_iterator = NGramSymbolIterator(pos, end, min_ngram_length - 1);
        for (size_t i = 0; i < min_ngram_length - 2; ++i)
            if (!symbol_iterator.increment())
                return;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        while (true)
        {
            auto cur_result = getNextIndices();
            if (!cur_result)
                return false;

            auto iter_left = cur_result->left_index;
            auto iter_right = cur_result->right_index;
            auto length = cur_result->symbols_between;
            if (min_cutoff_length && *min_cutoff_length > length)
            {
                continue;
            }
            token_begin = pos + iter_left;
            token_end = pos + iter_right;
            return true;
        }
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
                    res_data.push_back(hasher(start, end - start));

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
