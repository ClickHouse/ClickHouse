#include <stdexcept>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include "Common/Exception.h"
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

template<bool is_utf8>
struct SparseGramsName;

template<>
struct SparseGramsName<true>
{
    static constexpr const char* value = "sparseGramsUTF8";
};

template<>
struct SparseGramsName<false>
{
    static constexpr const char* value = "sparseGrams";
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

    /// Calculates CRC32-hash from substring [it_left, it_right)
    unsigned CalcHash(size_t it_left, size_t it_right)
    {
        auto substr_ref = StringRef(pos + it_left, it_right - it_left);
        return hasher(substr_ref);
    }

    void BuildBigramHashes()
    {
        if (pos == end) {
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
                if (elem < 0x7f) {
                    byte_offset++;
                } else if (elem < 0x7ff) {
                    byte_offset += 2;
                } else if (elem < 0xffff) {
                    byte_offset += 3;
                } else if (elem < 0x10ffff) {
                    byte_offset += 4;
                } else {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect utf8 symbol");
                }
            }   
            utf8_offsets.push_back(byte_offset);
            for (size_t i = 0; i < utf8_offsets.size() - 2; ++i)
            {
                bigram_hashes.push_back(CalcHash(utf8_offsets[i], utf8_offsets[i + 2]));
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
                bigram_hashes.push_back(CalcHash(i, i + 2));
            }
            utf8_offsets.push_back(i + 1);
        }
    }

public:
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
        for (auto elem : utf8_offsets)
        {
            std::cerr << elem << ' ';
        }
        std::cerr << '\n';
        while (pos + utf8_offsets[left] != end) {
            while (right < utf8_offsets.size() && 
                pos + utf8_offsets[right] - 1 != end &&
                (right - left < minimal_length)) 
            {
                right++;
            }

            if (right - left < minimal_length) 
            {
                return false;
            }

            size_t max_substr_bigram_hash = 0;
            for (size_t i = left; i < right - 1; ++i) 
            {
                max_substr_bigram_hash = std::max(max_substr_bigram_hash, bigram_hashes[i]);
            }

            while (right - 1 < utf8_offsets.size() &&
                pos + utf8_offsets[right - 1] != end && 
                CalcHash(utf8_offsets[left], utf8_offsets[right]) <= max_substr_bigram_hash) 
            {
                max_substr_bigram_hash = std::max(max_substr_bigram_hash, bigram_hashes[right - 1]);
                right++;
            }

            if (right - 1 < utf8_offsets.size() && pos + utf8_offsets[right - 1] != end) {
                token_begin = pos + utf8_offsets[left];
                token_end = pos + utf8_offsets[right];

                right++;
                if (right - 1 < utf8_offsets.size() && pos + utf8_offsets[right - 1] == end) {
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

using FunctionSparseGrams = FunctionTokens<SparseGramsImpl<false>>;
using FunctionSparseGramsUTF8 = FunctionTokens<SparseGramsImpl<true>>;

}

REGISTER_FUNCTION(SparseGrams)
{
    factory.registerFunction<FunctionSparseGrams>();
    factory.registerFunction<FunctionSparseGramsUTF8>();
}

}
