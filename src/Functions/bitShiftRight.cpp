#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <base/hex.h>

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
struct BitShiftRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = true;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BitShiftRight is not implemented for big integers as second argument");
        else if (b < 0 || static_cast<UInt256>(b) > 8 * sizeof(A))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The number of shift positions needs to be a non-negative value and less or equal to the bit width of the value to shift");
        else if constexpr (is_big_int_v<A>)
            return static_cast<Result>(a) >> static_cast<UInt32>(b);
        else
            return static_cast<Result>(a) >> static_cast<Result>(b);
    }

    static NO_SANITIZE_UNDEFINED void bitShiftRightForBytes(const UInt8 * op_pointer, const UInt8 * begin, UInt8 * out, const size_t shift_right_bits)
    {
        while (op_pointer > begin)
        {
            op_pointer--;
            out--;
            *out = *op_pointer >> shift_right_bits;
            if (op_pointer - 1 >= begin)
            {
                /// The right b bit of the left byte is moved to the left b bit of this byte
                *out = static_cast<UInt8>(static_cast<UInt8>(*(op_pointer - 1) << (8 - shift_right_bits)) | *out);
            }
        }
    }

    /// For String
    static ALWAYS_INLINE NO_SANITIZE_UNDEFINED void apply(const UInt8 * pos [[maybe_unused]], const UInt8 * end [[maybe_unused]], const B & b [[maybe_unused]], ColumnString::Chars & out_vec, ColumnString::Offsets & out_offsets)
    {
        if constexpr (is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BitShiftRight is not implemented for big integers as second argument");
        else
        {
            const UInt8 word_size = 8;
            size_t n = end - pos;
            const UInt128 bit_limit = static_cast<UInt128>(word_size) * n;
            if (b < 0 || static_cast<decltype(bit_limit)>(b) > bit_limit)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The number of shift positions needs to be a non-negative value and less or equal to the bit width of the value to shift");
            if (b == bit_limit)
            {
                /// insert default value
                out_vec.push_back(0);
                out_offsets.push_back(out_offsets.back() + 1);
                return;
            }

            size_t shift_right_bytes = b / word_size;
            size_t shift_right_bits = b % word_size;

            const UInt8 * begin = pos;
            const UInt8 * shift_right_end = end - shift_right_bytes;

            const size_t old_size = out_vec.size();
            size_t length = shift_right_end - begin;
            const size_t new_size = old_size + length + 1;
            out_vec.resize(new_size);
            out_vec[old_size + length] = 0;

            /// We start from the byte on the right and shift right shift_right_bits bit by byte
            UInt8 * op_pointer = const_cast<UInt8 *>(shift_right_end);
            UInt8 * out = out_vec.data() + old_size + length;
            bitShiftRightForBytes(op_pointer, begin, out, shift_right_bits);
            out_offsets.push_back(new_size);
        }
    }

    /// For FixedString
    static ALWAYS_INLINE NO_SANITIZE_UNDEFINED void apply(const UInt8 * pos [[maybe_unused]], const UInt8 * end [[maybe_unused]], const B & b [[maybe_unused]], ColumnFixedString::Chars & out_vec)
    {
        if constexpr (is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BitShiftRight is not implemented for big integers as second argument");
        else
        {
            const UInt8 word_size = 8;
            size_t n = end - pos;
            const UInt128 bit_limit = static_cast<UInt128>(word_size) * n;
            if (b < 0 || static_cast<decltype(bit_limit)>(b) > bit_limit)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The number of shift positions needs to be a non-negative value and less or equal to the bit width of the value to shift");
            if (b == bit_limit)
            {
                // insert default value
                out_vec.resize_fill(out_vec.size() + n);
                return;
            }

            size_t shift_right_bytes = b / word_size;
            size_t shift_right_bits = b % word_size;

            const UInt8 * begin = pos;
            const UInt8 * shift_right_end = end - shift_right_bytes;

            const size_t old_size = out_vec.size();
            const size_t new_size = old_size + n;

            /// Fill 0 to the left
            out_vec.resize_fill(out_vec.size() + old_size + shift_right_bytes);
            out_vec.resize(new_size);

            /// We start from the byte on the right and shift right shift_right_bits bit by byte
            UInt8 * op_pointer = const_cast<UInt8 *>(shift_right_end);
            UInt8 * out = out_vec.data() + new_size;
            bitShiftRightForBytes(op_pointer, begin, out, shift_right_bits);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitShiftRightImpl expected an integral type");
        return is_signed ? b.CreateAShr(left, right) : b.CreateLShr(left, right);
    }
#endif
};


struct NameBitShiftRight { static constexpr auto name = "bitShiftRight"; };
using FunctionBitShiftRight = BinaryArithmeticOverloadResolver<BitShiftRightImpl, NameBitShiftRight, true, false>;

}

REGISTER_FUNCTION(BitShiftRight)
{
    factory.registerFunction<FunctionBitShiftRight>();
}

}
