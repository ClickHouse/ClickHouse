#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A, typename B>
struct BitShiftLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = true;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BitShiftLeft is not implemented for big integers as second argument");
        else if (b < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The number of shift positions needs to be a non-negative value");
        else if (static_cast<UInt256>(b) > 8 * sizeof(A))
            return static_cast<Result>(0);
        else if constexpr (is_big_int_v<A>)
            return static_cast<Result>(a) << static_cast<UInt32>(b);
        else
            return static_cast<Result>(a) << static_cast<Result>(b);
    }

    /// For String
    static ALWAYS_INLINE NO_SANITIZE_UNDEFINED void apply(const UInt8 * pos [[maybe_unused]], const UInt8 * end [[maybe_unused]], const B & b [[maybe_unused]], ColumnString::Chars & out_vec, ColumnString::Offsets & out_offsets)
    {
        if constexpr (is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BitShiftLeft is not implemented for big integers as second argument");
        else
        {
            const UInt8 word_size = 8 * sizeof(*pos);
            size_t n = end - pos;
            const UInt128 bit_limit = static_cast<UInt128>(word_size) * n;
            if (b < 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The number of shift positions needs to be a non-negative value");

            if (b == bit_limit || static_cast<decltype(bit_limit)>(b) > bit_limit)
            {
                // insert default value
                out_vec.push_back(0);
                out_offsets.push_back(out_offsets.back() + 1);
                return;
            }

            size_t shift_left_bits = b % word_size;
            size_t shift_left_bytes = b / word_size;

            const UInt8 * begin = pos;

            const size_t old_size = out_vec.size();
            size_t length;
            if (shift_left_bits)
                length = end + shift_left_bytes - begin + 1; /// Moving to the left here will make a redundant byte to store the overflowing bits in the front
            else
                length = end + shift_left_bytes - begin;

            const size_t new_size = old_size + length + 1;
            out_vec.resize(new_size);
            out_vec[old_size + length] = 0;

            UInt8 * op_pointer = const_cast<UInt8 *>(begin);
            UInt8 * out = out_vec.data() + old_size;

            UInt8 previous = 0;
            while (op_pointer < end)
            {
                if (shift_left_bits)
                {
                    /// The left b bit of the right byte is moved to the right b bit of this byte
                    *out = static_cast<UInt8>(static_cast<UInt8>(*(op_pointer) >> (8 - shift_left_bits)) | previous);
                    previous = *op_pointer << shift_left_bits;
                }
                else
                {
                    *out = *op_pointer;
                }
                op_pointer++;
                out++;
            }
            if (shift_left_bits)
            {
                *out = *(op_pointer - 1) << shift_left_bits;
                out++;
            }

            for (size_t i = 0; i < shift_left_bytes; ++i)
                *(out + i) = 0;

            out_offsets.push_back(new_size);
        }
    }

    /// For FixedString
    static ALWAYS_INLINE NO_SANITIZE_UNDEFINED void apply(const UInt8 * pos [[maybe_unused]], const UInt8 * end [[maybe_unused]], const B & b [[maybe_unused]], ColumnFixedString::Chars & out_vec)
    {
        if constexpr (is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BitShiftLeft is not implemented for big integers as second argument");
        else
        {
            const UInt8 word_size = 8;
            size_t n = end - pos;
            const UInt128 bit_limit = static_cast<UInt128>(word_size) * n;
            if (b < 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The number of shift positions needs to be a non-negative value");

            if (b == bit_limit || static_cast<decltype(bit_limit)>(b) > bit_limit)
            {
                // insert default value
                out_vec.resize_fill(out_vec.size() + n);
                return;
            }

            size_t shift_left_bytes = b / word_size;
            size_t shift_left_bits = b % word_size;

            const UInt8 * begin = pos;

            const size_t old_size = out_vec.size();
            const size_t new_size = old_size + n;

            out_vec.resize(new_size);

            UInt8 * op_pointer = const_cast<UInt8 *>(begin + shift_left_bytes);
            UInt8 * out = out_vec.data() + old_size;

            while (op_pointer < end)
            {
                *out = *op_pointer << shift_left_bits;
                if (op_pointer + 1 < end)
                {
                    /// The left b bit of the right byte is moved to the right b bit of this byte
                    *out = static_cast<UInt8>(static_cast<UInt8>(*(op_pointer + 1) >> (8 - shift_left_bits)) | *out);
                }
                op_pointer++;
                out++;
            }

            for (size_t i = 0; i < shift_left_bytes; ++i)
                *(out + i) = 0;
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitShiftLeftImpl expected an integral type");
        return b.CreateShl(left, right);
    }
#endif
};

struct NameBitShiftLeft { static constexpr auto name = "bitShiftLeft"; };
using FunctionBitShiftLeft = BinaryArithmeticOverloadResolver<BitShiftLeftImpl, NameBitShiftLeft, true, false>;

}

REGISTER_FUNCTION(BitShiftLeft)
{
    FunctionDocumentation::Description description = R"(
Shifts the binary representation of a value to the left by a specified number of bit positions.

A `FixedString` or a `String` is treated as a single multibyte value.

Bits of a `FixedString` value are lost as they are shifted out.
On the contrary, a `String` value is extended with additional bytes, so no bits are lost.
)";
    FunctionDocumentation::Syntax syntax = "bitShiftLeft(a, N)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "A value to shift.", {"(U)Int*", "String", "FixedString"}},
        {"N", "The number of positions to shift.", {"UInt8/16/32/64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the shifted value with type equal to that of `a`."};
    FunctionDocumentation::Examples examples = {{"Usage example with binary encoding",
        R"(
SELECT 99 AS a, bin(a), bitShiftLeft(a, 2) AS a_shifted, bin(a_shifted);
        )",
        R"(
┌──a─┬─bin(99)──┬─a_shifted─┬─bin(bitShiftLeft(99, 2))─┐
│ 99 │ 01100011 │       140 │ 10001100                 │
└────┴──────────┴───────────┴──────────────────────────┘
        )"},
        {"Usage example with hexadecimal encoding", R"(
SELECT 'abc' AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
        )",
        R"(
┌─a───┬─hex('abc')─┬─a_shifted─┬─hex(bitShiftLeft('abc', 4))─┐
│ abc │ 616263     │ &0        │ 06162630                    │
└─────┴────────────┴───────────┴─────────────────────────────┘
        )"},
{"Usage example with Fixed String encoding", R"(
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
        )",
R"(
┌─a───┬─hex(toFixedString('abc', 3))─┬─a_shifted─┬─hex(bitShiftLeft(toFixedString('abc', 3), 4))─┐
│ abc │ 616263                       │ &0        │ 162630                                        │
└─────┴──────────────────────────────┴───────────┴───────────────────────────────────────────────┘
        )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitShiftLeft>(documentation);
}

}
