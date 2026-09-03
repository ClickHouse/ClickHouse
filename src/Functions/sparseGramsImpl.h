#pragma once

#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>
#include <Functions/FunctionHelpers.h>
#include <base/types.h>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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

    Pos pos = nullptr;
    Pos end = nullptr;
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
        Pos data = nullptr;
        Pos end = nullptr;
        size_t n = 0;
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
        , min_cutoff_length(std::move(min_cutoff_length_))
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
            {"min_cutoff_length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), isColumnConst, "const Number"},
        };

        validateFunctionArguments(func, arguments, mandatory_args, optional_args);
    }

    void init(const ColumnsWithTypeAndName & arguments, bool /*max_substrings_includes_remaining_string*/)
    {
        if (arguments.size() > 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, must be from 1 to 4",
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

        if (arguments.size() == 4)
            min_cutoff_length = arguments[3].column->getUInt(0);

        if (min_cutoff_length && *min_cutoff_length < min_ngram_length)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'min_cutoff_length' must be greater or equal to 'min_ngram_length'");

        if (min_cutoff_length && *min_cutoff_length > max_ngram_length)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'min_cutoff_length' must be less or equal to 'max_ngram_length'");
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
                continue;

            token_begin = pos + iter_left;
            token_end = pos + iter_right;
            return true;
        }
    }
};

}
