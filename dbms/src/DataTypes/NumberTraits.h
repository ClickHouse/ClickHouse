#pragma once

#include <boost/mpl/bool.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/if.hpp>
#include <boost/mpl/and.hpp>
#include <boost/mpl/or.hpp>
#include <boost/mpl/not.hpp>
#include <boost/mpl/greater.hpp>
#include <boost/mpl/min_max.hpp>
#include <boost/mpl/equal_to.hpp>
#include <boost/mpl/comparison.hpp>

#include <Core/Types.h>
#include <tuple>
#include <type_traits>

namespace DB
{

/** Allows get the result type of the functions +, -, *, /, %, div (integer division).
  * The rules are different from those used in C++.
  */

namespace NumberTraits
{

using Unsigned = boost::mpl::false_;
using Signed = boost::mpl::true_ ;

using Integer = boost::mpl::false_;
using Floating = boost::mpl::true_;

using HasNull = boost::mpl::true_;
using HasNoNull = boost::mpl::false_;

using Bits0 = boost::mpl::int_<0>;
using Bits8 = boost::mpl::int_<8>;
using Bits16 = boost::mpl::int_<16>;
using Bits32 = boost::mpl::int_<32>;
using Bits64 = boost::mpl::int_<64>;
using BitsTooMany = boost::mpl::int_<1024>;

struct Error {};

template <typename T> struct Next;

template <> struct Next<Bits0> { using Type = Bits0; };
template <> struct Next<Bits8> { using Type = Bits16; };
template <> struct Next<Bits16> { using Type = Bits32; };
template <> struct Next<Bits32> { using Type = Bits64; };
template <> struct Next<Bits64> { using Type = Bits64; };

template <typename T> struct ExactNext { using Type = typename Next<T>::Type; };
template <> struct ExactNext<Bits64> { using Type = BitsTooMany; };

template <typename T> struct Traits;

template <typename T>
struct Traits<Nullable<T>>
{
    using Sign = typename Traits<T>::Sign;
    using Floatness = typename Traits<T>::Floatness;
    using Bits = typename Traits<T>::Bits;
    using Nullity = HasNull;
};

template <> struct Traits<void>        { using Sign = Unsigned;        using Floatness = Integer;      using Bits = Bits0;     using Nullity = HasNoNull; };
template <> struct Traits<Null> : Traits<Nullable<void>> {};
template <> struct Traits<UInt8>       { using Sign = Unsigned;        using Floatness = Integer;      using Bits = Bits8;     using Nullity = HasNoNull; };
template <> struct Traits<UInt16>      { using Sign = Unsigned;        using Floatness = Integer;      using Bits = Bits16;    using Nullity = HasNoNull; };
template <> struct Traits<UInt32>      { using Sign = Unsigned;        using Floatness = Integer;      using Bits = Bits32;    using Nullity = HasNoNull; };
template <> struct Traits<UInt64>      { using Sign = Unsigned;        using Floatness = Integer;      using Bits = Bits64;    using Nullity = HasNoNull; };
template <> struct Traits<Int8>        { using Sign = Signed;          using Floatness = Integer;      using Bits = Bits8;     using Nullity = HasNoNull; };
template <> struct Traits<Int16>       { using Sign = Signed;          using Floatness = Integer;      using Bits = Bits16;    using Nullity = HasNoNull; };
template <> struct Traits<Int32>       { using Sign = Signed;          using Floatness = Integer;      using Bits = Bits32;    using Nullity = HasNoNull; };
template <> struct Traits<Int64>       { using Sign = Signed;          using Floatness = Integer;      using Bits = Bits64;    using Nullity = HasNoNull; };
template <> struct Traits<Float32>     { using Sign = Signed;          using Floatness = Floating;     using Bits = Bits32;    using Nullity = HasNoNull; };
template <> struct Traits<Float64>     { using Sign = Signed;          using Floatness = Floating;     using Bits = Bits64;    using Nullity = HasNoNull; };

template <typename Sign, typename Floatness, typename Bits, typename Nullity> struct Construct;

template <typename Sign, typename Floatness, typename Bits>
struct Construct<Sign, Floatness, Bits, HasNull>
{
    using Type = Nullable<typename Construct<Sign, Floatness, Bits, HasNoNull>::Type>;
};

template <> struct Construct<Unsigned, Integer, Bits0, HasNull>  { using Type = Null; };
template <> struct Construct<Unsigned, Floating, Bits0, HasNull> { using Type = Null; };
template <> struct Construct<Signed, Integer, Bits0, HasNull>    { using Type = Null; };
template <> struct Construct<Signed, Floating, Bits0, HasNull>   { using Type = Null; };

template <typename Sign, typename Floatness>
struct Construct<Sign, Floatness, BitsTooMany, HasNull>
{
    using Type = Error;
};

template <typename Sign, typename Floatness>
struct Construct<Sign, Floatness, BitsTooMany, HasNoNull>
{
    using Type = Error;
};

template <> struct Construct<Unsigned, Integer, Bits0, HasNoNull>   { using Type = void; };
template <> struct Construct<Unsigned, Floating, Bits0, HasNoNull>  { using Type = void; };
template <> struct Construct<Signed, Integer, Bits0, HasNoNull>     { using Type = void; };
template <> struct Construct<Signed, Floating, Bits0, HasNoNull>    { using Type = void; };
template <> struct Construct<Unsigned, Integer, Bits8, HasNoNull>   { using Type = UInt8; };
template <> struct Construct<Unsigned, Integer, Bits16, HasNoNull>  { using Type = UInt16; };
template <> struct Construct<Unsigned, Integer, Bits32, HasNoNull>  { using Type = UInt32; };
template <> struct Construct<Unsigned, Integer, Bits64, HasNoNull>  { using Type = UInt64; };
template <> struct Construct<Unsigned, Floating, Bits8, HasNoNull>  { using Type = Float32; };
template <> struct Construct<Unsigned, Floating, Bits16, HasNoNull> { using Type = Float32; };
template <> struct Construct<Unsigned, Floating, Bits32, HasNoNull> { using Type = Float32; };
template <> struct Construct<Unsigned, Floating, Bits64, HasNoNull> { using Type = Float64; };
template <> struct Construct<Signed, Integer, Bits8, HasNoNull>     { using Type = Int8; };
template <> struct Construct<Signed, Integer, Bits16, HasNoNull>    { using Type = Int16; };
template <> struct Construct<Signed, Integer, Bits32, HasNoNull>    { using Type = Int32; };
template <> struct Construct<Signed, Integer, Bits64, HasNoNull>    { using Type = Int64; };
template <> struct Construct<Signed, Floating, Bits8, HasNoNull>    { using Type = Float32; };
template <> struct Construct<Signed, Floating, Bits16, HasNoNull>   { using Type = Float32; };
template <> struct Construct<Signed, Floating, Bits32, HasNoNull>   { using Type = Float32; };
template <> struct Construct<Signed, Floating, Bits64, HasNoNull>   { using Type = Float64; };

template <typename T>
inline bool isErrorType()
{
    return false;
}
template <>
inline bool isErrorType<Error>()
{
    return true;
}

/// Returns the type A augmented with nullity = nullity(A) | nullity(B)
template <typename A, typename B>
struct UpdateNullity
{
    using Type = typename Construct<
        typename Traits<A>::Sign,
        typename Traits<A>::Floatness,
        typename Traits<A>::Bits,
        typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type
    >::Type;
};

/** The result of addition or multiplication is calculated according to the following rules:
    * - if one of the arguments is floating-point, the result is a floating point, otherwise - the whole;
    * - if one of the arguments is signed, the result is signed, otherwise it is unsigned;
    * - the result contains more bits (not only meaningful) than the maximum in the arguments
    *   (for example, UInt8 + Int32 = Int64).
    */
template <typename A, typename B> struct ResultOfAdditionMultiplication
{
    using Type = typename Construct<
        typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
        typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
        typename Next<typename boost::mpl::max<typename Traits<A>::Bits, typename Traits<B>::Bits>::type>::Type,
        typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type>::Type;
};

template <typename A, typename B> struct ResultOfSubtraction
{
    using Type = typename Construct<
        Signed,
        typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
        typename Next<typename boost::mpl::max<typename Traits<A>::Bits, typename Traits<B>::Bits>::type>::Type,
        typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type>::Type;
};

/** When dividing, you always get a floating-point number.
    */
template <typename A, typename B> struct ResultOfFloatingPointDivision
{
    using Type = Float64;
};

/** For integer division, we get a number with the same number of bits as in divisible.
    */
template <typename A, typename B> struct ResultOfIntegerDivision
{
    using Type = typename Construct<
        typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
        Integer,
        typename Traits<A>::Bits,
        typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type>::Type;
};

/** Division with remainder you get a number with the same number of bits as in divisor.
    */
template <typename A, typename B> struct ResultOfModulo
{
    using Type = typename Construct<
        typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
        Integer,
        typename Traits<B>::Bits,
        typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type>::Type;
};

template <typename A> struct ResultOfNegate
{
    using Type = typename Construct<
        Signed,
        typename Traits<A>::Floatness,
        typename boost::mpl::if_<
            typename Traits<A>::Sign,
            typename Traits<A>::Bits,
            typename Next<typename Traits<A>::Bits>::Type>::type,
            typename Traits<A>::Nullity>::Type;
};

template <typename A> struct ResultOfAbs
{
    using Type = typename Construct<
        Unsigned,
        typename Traits<A>::Floatness,
        typename Traits <A>::Bits,
        typename Traits<A>::Nullity>::Type;
};

/** For bitwise operations, an integer is obtained with number of bits is equal to the maximum of the arguments.
    */
template <typename A, typename B> struct ResultOfBit
{
    using Type = typename Construct<
        typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
        Integer,
        typename boost::mpl::max<
            typename boost::mpl::if_<
                typename Traits<A>::Floatness,
                Bits64,
                typename Traits<A>::Bits>::type,
            typename boost::mpl::if_<
                typename Traits<B>::Floatness,
                Bits64,
                typename Traits<B>::Bits>::type>::type,
                typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type>::Type;
};

template <typename A> struct ResultOfBitNot
{
    using Type = typename Construct<
        typename Traits<A>::Sign,
        Integer,
        typename Traits<A>::Bits,
        typename Traits<A>::Nullity>::Type;
};


/** Type casting for `if` function:
    * 1)     void,      Type ->  Type
    * 2)  UInt<x>,   UInt<y> ->  UInt<max(x,y)>
    * 3)   Int<x>,    Int<y> ->   Int<max(x,y)>
    * 4) Float<x>,  Float<y> -> Float<max(x, y)>
    * 5)  UInt<x>,    Int<y> ->   Int<max(x*2, y)>
    * 6) Float<x>, [U]Int<y> -> Float<max(x, y*2)>
    * 7)  UInt64 ,    Int<x> -> Error
    * 8) Float<x>, [U]Int64  -> Error
    */
template <typename A, typename B>
struct ResultOfIf
{
    using Type =
        /// 1)
        typename boost::mpl::if_<
            typename boost::mpl::equal_to<typename Traits<A>::Bits, Bits0>::type,
            typename UpdateNullity<B, A>::Type,
        typename boost::mpl::if_<
            typename boost::mpl::equal_to<typename Traits<B>::Bits, Bits0>::type,
            typename UpdateNullity<A, B>::Type,
        /// 4) and 6)
        typename boost::mpl::if_<
            typename boost::mpl::or_<
                typename Traits<A>::Floatness,
                typename Traits<B>::Floatness>::type,
            typename Construct<
                Signed,
                Floating,
                typename boost::mpl::max< /// This maximum is needed only because `if_` always evaluates all arguments.
                    typename boost::mpl::max<
                        typename boost::mpl::if_<
                            typename Traits<A>::Floatness,
                            typename Traits<A>::Bits,
                            typename ExactNext<typename Traits<A>::Bits>::Type>::type,
                        typename boost::mpl::if_<
                            typename Traits<B>::Floatness,
                            typename Traits<B>::Bits,
                            typename ExactNext<typename Traits<B>::Bits>::Type>::type>::type,
                    Bits32>::type,
                    typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type>::Type,
        /// 2) and 3)
        typename boost::mpl::if_<
            typename boost::mpl::equal_to<
                typename Traits<A>::Sign,
                typename Traits<B>::Sign>::type,
            typename boost::mpl::if_<
                typename boost::mpl::less<
                    typename Traits<A>::Bits,
                    typename Traits<B>::Bits>::type,
                typename UpdateNullity<B, A>::Type,
                typename UpdateNullity<A, B>::Type>::type,
        /// 5)
        typename Construct<
            Signed,
            Integer,
            typename boost::mpl::max<
                typename boost::mpl::if_<
                    typename Traits<A>::Sign,
                    typename Traits<A>::Bits,
                    typename ExactNext<typename Traits<A>::Bits>::Type>::type,
                typename boost::mpl::if_<
                    typename Traits<B>::Sign,
                    typename Traits<B>::Bits,
                    typename ExactNext<typename Traits<B>::Bits>::Type>::type>::type,
            typename boost::mpl::or_<typename Traits<A>::Nullity, typename Traits<B>::Nullity>::type
        >::Type>::type>::type>::type>::type;
};

/** Before applying operator `%` and bitwise operations, operands are casted to whole numbers. */
template <typename A> struct ToInteger
{
    using Type = typename Construct<
        typename Traits<A>::Sign,
        Integer,
        typename boost::mpl::if_<
            typename Traits<A>::Floatness,
            Bits64,
            typename Traits<A>::Bits>::type,
        typename Traits<A>::Nullity
    >::Type;
};


// CLICKHOUSE-29. The same depth, different signs
// NOTE: This case is applied for 64-bit integers only (for backward compability), but colud be used for any-bit integers
template <typename A, typename B>
using LeastGreatestSpecialCase = std::integral_constant<bool, std::is_integral<A>::value && std::is_integral<B>::value
    && (8 == sizeof(A) && sizeof(A) == sizeof(B))
    && (std::is_signed<A>::value ^ std::is_signed<B>::value)>;

template <typename A, typename B>
using ResultOfLeast = std::conditional_t<LeastGreatestSpecialCase<A, B>::value,
    typename Construct<Signed, Integer, typename Traits<A>::Bits, HasNoNull>::Type,
    typename ResultOfIf<A, B>::Type>;

template <typename A, typename B>
using ResultOfGreatest = std::conditional_t<LeastGreatestSpecialCase<A, B>::value,
    typename Construct<Unsigned, Integer, typename Traits<A>::Bits, HasNoNull>::Type,
    typename ResultOfIf<A, B>::Type>;

}

}
