#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
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
    static inline NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<B>)
            throw Exception("BitShiftLeft is not implemented for big integers as second argument", ErrorCodes::NOT_IMPLEMENTED);
        else if constexpr (is_big_int_v<A>)
            return static_cast<Result>(a) << static_cast<UInt32>(b);
        else
            return static_cast<Result>(a) << static_cast<Result>(b);
    }

    /// For String
    static ALWAYS_INLINE NO_SANITIZE_UNDEFINED void apply(const UInt8 * pos [[maybe_unused]], const UInt8 * end [[maybe_unused]], const B & b [[maybe_unused]], ColumnString::Chars & out_vec, ColumnString::Offsets & out_offsets)
    {
        if constexpr (is_big_int_v<B>)
            throw Exception("BitShiftLeft is not implemented for big integers as second argument", ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            UInt8 word_size = 8;
            /// To prevent overflow
            if (static_cast<double>(b) >= (static_cast<double>(end - pos) * word_size) || b < 0)
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
            throw Exception("BitShiftLeft is not implemented for big integers as second argument", ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            UInt8 word_size = 8;
            size_t n = end - pos;
            /// To prevent overflow
            if (static_cast<double>(b) >= (static_cast<double>(n) * word_size) || b < 0)
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

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitShiftLeftImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateShl(left, right);
    }
#endif
};

struct NameBitShiftLeft { static constexpr auto name = "bitShiftLeft"; };
using FunctionBitShiftLeft = BinaryArithmeticOverloadResolver<BitShiftLeftImpl, NameBitShiftLeft, true, false>;

}

REGISTER_FUNCTION(BitShiftLeft)
{
    factory.registerFunction<FunctionBitShiftLeft>();
}

}
