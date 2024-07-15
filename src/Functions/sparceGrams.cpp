#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include "base/types.h"
#include <base/StringRef.h>

namespace DB
{

/** Functions that finds all substrings such their crc32-hash is more than crc32-hash of every bigram in substring.
  *
  * sparseGrams(s)
  */
namespace
{

using Pos = const char *;

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

    /// Calculates CRC32-hash from substring [it_left, it_right)
    unsigned CalcHash(size_t it_left, size_t it_right) 
    {
        auto substr_ref = StringRef(pos + it_left, it_right - it_left);
        return hasher(substr_ref);
    }

    void BuildBigramHashes()
    {
        if (pos == end) {
            return;
        }

        for (size_t i = 0;; ++i) {
            if (pos + i + 1 == end) {
                break;
            }
            bigram_hashes.push_back(CalcHash(i, i + 2));
        }
    }

public:
    static constexpr auto name = "sparseGrams";

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

    static constexpr auto strings_argument_position = 0uz;

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        left = 0;
        right = 0;

        BuildBigramHashes();
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        while (pos + left != end) {
            while (pos + right - 1 != end && right - left < minimal_length) {
                right++;
            }

            if (right - left < minimal_length) {
                return false;
            }

            size_t max_substr_bigram_hash = 0;
            for (size_t i = left; i < right - 1; ++i) {
                max_substr_bigram_hash = std::max(max_substr_bigram_hash, bigram_hashes[i]);   
            }

            while (pos + right - 1 != end && CalcHash(left, right) <= max_substr_bigram_hash) {
                max_substr_bigram_hash = std::max(max_substr_bigram_hash, bigram_hashes[right - 1]);
                right++;
            }

            if (pos + right - 1 != end) {
                token_begin = pos + left;
                token_end = pos + right;
                right++;
                if (pos + right - 1 == end) {
                    left++;
                    right = left;
                }
                return true;
            }
            left++;
            right = left;
        }

        return false;
    }
};

using FunctionSparseGrams = FunctionTokens<SparseGramsImpl>;

}

REGISTER_FUNCTION(SparseGrams)
{
    factory.registerFunction<FunctionSparseGrams>();
}

}
