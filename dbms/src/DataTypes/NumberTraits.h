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

template <typename T, typename Nullability>
struct AddNullability;

template <typename T>
struct AddNullability<T, HasNull>
{
    using Type = Nullable<T>;
};

template <typename T>
struct AddNullability<T, HasNoNull>
{
    using Type = T;
};

template <typename T> struct Next;

template <> struct Next<Bits0>    { using Type = Bits0; };
template <> struct Next<Bits8>    { using Type = Bits16; };
template <> struct Next<Bits16>    { using Type = Bits32; };
template <> struct Next<Bits32>    { using Type = Bits64; };
template <> struct Next<Bits64>    { using Type = Bits64; };

template <typename T> struct ExactNext { using Type = typename Next<T>::Type; };
template <> struct ExactNext<Bits64>    { using Type = BitsTooMany; };

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

/// Notes on type composition.
///
/// I. Problem statement.
///
/// Type composition with ResultOfIf is not associative. Example:
/// (Int8 x UInt32) x Float32 = Int64 x Float32 = Error;
/// Int8 x (UInt32 x Float32) = Int8 x Float64 = Float64.
/// In order to sort out this issue, we design a slightly improved version
/// of ResultOfIf.
///
/// II. A more rigorous approach to ResultOfIf.
///
/// First we represent the set of types:
/// T = {Void,Int8,Int16,Int32,Int64,UInt8,UInt16,UInt32,UInt64,Float32,Float64}
/// as a poset P with the partial order being such that for any t1,t2 ∈ T,
/// t1 < t2 if and only if the domain of values of t1 is included in the domain
/// of values of t2.
///
/// For each type t ∈ T, we define C(t) as the set of chains of the poset P whose
/// unique minimal element is T.
///
/// Now for any two types t1,t2 ∈ T, we define the poset C(t1,t2) as the intersection
/// C(t1) ∩ C(t2).
///
/// Denote K(t1,t2) as the unique antichain of C(t1,t2) in which each element minimally
/// represents both t1 and t2. It is important to keep in mind that t1 and t2 are
/// *not* comparable.
///
/// For the most part, K(t1,t2) coincides with the result of the application of
/// ResultOfIf to t1 and t2. Nevertheless, for some particular combinations of t1
/// and t2, the map K returns one of the following two antichains: {Int32,Float32},
/// {Int64,Float64}.
///
/// From these observations, we conclude that the type system T and the composition
/// law ResultOfIf are not powerful enough to represent all the combinations of
/// elements of T. That is the reason why ResultOfIf is not associative.
///
/// III. Extending ResultOfIf.
///
/// Let's embed T into a larger set E of "enriched types" such that:
/// 1. E ⊂ TxT;
/// 2. for each t ∈ T, (T,Void) ∈ E.
/// 3. (Int32,Float32) ∈ E
/// 4. (Int64,Float64) ∈ E.
///
/// E represents the image A of the map K, a set of antichains, as a set of types.
///
/// Consider the canonical injection ψ : T x T ----> E x E and the natural bijection
/// φ : A ----> E.
/// Then there exists a unique map K' : E x E ----> E, that makes the diagram below
/// commutative:
///
///        K
/// T x T ----> A
///   |         |
///   | ψ       | φ
///   ↓    L    ↓
/// E x E ----> E
///
/// L is exactly the same map as K, the sole difference being that L takes as
/// parameters extended types that map to ordinary ones.
///
/// Finally we extend the map L. To this end, we complete the type composition table
/// with the new types (Int32,Float32) and (Int64,Float64) appearing on either
/// the left-hand side or the right-hand side. This extended map is called
/// TypeProduct in the implementation. TypeProduct is both commutative and associative.
///
/// IV. Usage.
///
/// When we need to compose ordinary types, the following is to be performed:
/// 1. embed each type into its counterpart in E with EmbedType;
/// 2. compose the resulting enriched types with TypeProduct;
/// 3. return the first component of the result with ToOrdinaryType, which means
/// that, given an extended type e = (p,q) ∈ E, we return the ordinary type p ∈ T.
///
/// The result is the type we are looking for.
///
/// V. Example.
///
/// Suppose we need to compose, as in the problem statement, the types Int8,
/// UInt32, and Float32. The corresponding embedded types are:
/// (Int8,Void), (UInt32,Void), (Float32,Void).
///
/// By computing (Int8 x UInt32) x Float32, we get:
///
/// TypeProduct(TypeProduct((Int8,Void),(UInt32,Void)), (Float32,Void))
/// = TypeProduct((Int64,Float64), (Float32,Void))
/// = (Float64,void)
/// Thus, (Int8 x UInt32) x Float32 = Float64.
///
/// By computing Int8 x (UInt32 x Float32), we get:
///
/// TypeProduct((Int8,Void), TypeProduct((UInt32,Void), (Float32,Void)))
/// = TypeProduct((Int8,Void), (Float64,Void))
/// = (Float64,void)
/// Thus, Int8 x (UInt32 x Float32) = Float64.
///

namespace Enriched
{

/// Definitions of enriched types.
template <typename Nullity> using Void = std::tuple<void, void, Nullity>;
template <typename Nullity> using Int8 = std::tuple<DB::Int8, void, Nullity>;
template <typename Nullity> using Int16 = std::tuple<DB::Int16, void, Nullity>;
template <typename Nullity> using Int32 = std::tuple<DB::Int32, void, Nullity>;
template <typename Nullity> using Int64 = std::tuple<DB::Int64, void, Nullity>;
template <typename Nullity> using UInt8 = std::tuple<DB::UInt8, void, Nullity>;
template <typename Nullity> using UInt16 = std::tuple<DB::UInt16, void, Nullity>;
template <typename Nullity> using UInt32 = std::tuple<DB::UInt32, void, Nullity>;
template <typename Nullity> using UInt64 = std::tuple<DB::UInt64, void, Nullity>;
template <typename Nullity> using Float32 = std::tuple<DB::Float32, void, Nullity>;
template <typename Nullity> using Float64 = std::tuple<DB::Float64, void, Nullity>;
template <typename Nullity> using IntFloat32 = std::tuple<DB::Int32, DB::Float32, Nullity>;
template <typename Nullity> using IntFloat64 = std::tuple<DB::Int64, DB::Float64, Nullity>;

}

/// Embed an ordinary type into the corresponding enriched type.
template <typename T>
struct EmbedType;

template <> struct EmbedType<Error> { using Type = Error; };

template <> struct EmbedType<void> { using Type = Enriched::Void<HasNoNull>; };
template <> struct EmbedType<Int8> { using Type = Enriched::Int8<HasNoNull>; };
template <> struct EmbedType<Int16> { using Type = Enriched::Int16<HasNoNull>; };
template <> struct EmbedType<Int32> { using Type = Enriched::Int32<HasNoNull>; };
template <> struct EmbedType<Int64> { using Type = Enriched::Int64<HasNoNull>; };
template <> struct EmbedType<UInt8> { using Type = Enriched::UInt8<HasNoNull>; };
template <> struct EmbedType<UInt16> { using Type = Enriched::UInt16<HasNoNull>; };
template <> struct EmbedType<UInt32> { using Type = Enriched::UInt32<HasNoNull>; };
template <> struct EmbedType<UInt64> { using Type = Enriched::UInt64<HasNoNull>; };
template <> struct EmbedType<Float32> { using Type = Enriched::Float32<HasNoNull>; };
template <> struct EmbedType<Float64> { using Type = Enriched::Float64<HasNoNull>; };

template <> struct EmbedType<Null> { using Type = Enriched::Void<HasNull>; };
template <> struct EmbedType<Nullable<Int8> > { using Type = Enriched::Int8<HasNull>; };
template <> struct EmbedType<Nullable<Int16> > { using Type = Enriched::Int16<HasNull>; };
template <> struct EmbedType<Nullable<Int32> > { using Type = Enriched::Int32<HasNull>; };
template <> struct EmbedType<Nullable<Int64> > { using Type = Enriched::Int64<HasNull>; };
template <> struct EmbedType<Nullable<UInt8> > { using Type = Enriched::UInt8<HasNull>; };
template <> struct EmbedType<Nullable<UInt16> > { using Type = Enriched::UInt16<HasNull>; };
template <> struct EmbedType<Nullable<UInt32> > { using Type = Enriched::UInt32<HasNull>; };
template <> struct EmbedType<Nullable<UInt64> > { using Type = Enriched::UInt64<HasNull>; };
template <> struct EmbedType<Nullable<Float32> > { using Type = Enriched::Float32<HasNull>; };
template <> struct EmbedType<Nullable<Float64> > { using Type = Enriched::Float64<HasNull>; };

/// Get an ordinary type from an enriched type.
template <typename TType>
struct ToOrdinaryType
{
    using Type = typename std::conditional<
        std::is_same<typename std::tuple_element<2, TType>::type, HasNoNull>::value,
        typename std::tuple_element<0, TType>::type,
        Nullable<typename std::tuple_element<0, TType>::type>
    >::type;
};

/// Get an ordinary type from an enriched type.
/// Error case.
template <>
struct ToOrdinaryType<Error>
{
    using Type = Error;
};

namespace
{

/// The following helper functions and structures are used for the TypeProduct implementation.

/// Check if two types are equal up to nullity.
template <typename T1, typename T2>
constexpr bool areSimilarTypes()
{
    return std::is_same<
            typename std::tuple_element<0, T1>::type,
            typename std::tuple_element<0, T2>::type
        >::value &&
        std::is_same<
            typename std::tuple_element<1, T1>::type,
            typename std::tuple_element<1, T2>::type
        >::value;
}

/// Check if a pair of types {A,B} equals a pair of types {A1,B1} up to nullity.
template <typename A, typename B, template <typename> class A1, template <typename> class B1>
constexpr bool areSimilarPairs()
{
    /// NOTE: the use of HasNoNull here is a trick. It has no meaning.
    return (areSimilarTypes<A, A1<HasNoNull>>() && areSimilarTypes<B, B1<HasNoNull>>()) ||
        (areSimilarTypes<A, B1<HasNoNull>>() && areSimilarTypes<B, A1<HasNoNull>>());
}

/// Check if a pair of enriched types {A,B} that have straight mappings to ordinary
/// types must be processed in a special way.
template <typename A, typename B>
constexpr bool isExceptionalPair()
{
    return    areSimilarPairs<A, B, Enriched::Int8, Enriched::UInt16>() ||
        areSimilarPairs<A, B, Enriched::Int8, Enriched::UInt32>() ||
        areSimilarPairs<A, B, Enriched::Int16, Enriched::UInt16>() ||
        areSimilarPairs<A, B, Enriched::Int16, Enriched::UInt32>() ||
        areSimilarPairs<A, B, Enriched::Int32, Enriched::UInt32>();
}

/// Check if a pair of enriched types {A,B} is ordinary. Here "ordinary" means
/// that both types map to ordinary types and that they are not exceptional as
/// defined in the function above.
template <typename A, typename B>
constexpr bool isOrdinaryPair()
{
    return    std::is_same<typename std::tuple_element<1, A>::type, void>::value &&
        std::is_same<typename std::tuple_element<1, B>::type, void>::value &&
        !isExceptionalPair<A, B>();
}

/// Returns nullity(A) | nullity(B).
template <typename A, typename B>
struct CombinedNullity
{
private:
    using NullityA = typename Traits<typename ToOrdinaryType<A>::Type>::Nullity;
    using NullityB = typename Traits<typename ToOrdinaryType<B>::Type>::Nullity;

public:
    using Type = typename boost::mpl::or_<NullityA, NullityB>::type;
};

}

/// Compute the product of two enriched numeric types.
/// This statement catches all the incorrect combinations.
template <typename T1, typename T2, typename Enable = void>
struct TypeProduct
{
    using Type = Error;
};

/// Compute the product of two enriched numeric types.
/// Case when both these types are ordinary in the meaning defined above.
template <typename A, typename B>
struct TypeProduct<A, B, typename std::enable_if<isOrdinaryPair<A, B>()>::type>
{
private:
    using Result = typename ResultOfIf<
        typename ToOrdinaryType<A>::Type,
        typename ToOrdinaryType<B>::Type
    >::Type;

public:
    using Type = typename EmbedType<Result>::Type;
};

/// Compute the product of two enriched numeric types.
/// Case when a source type or the resulting type does not map to any ordinary type.

#define DEFINE_TYPE_PRODUCT_RULE(T1, T2, T3)                       \
template <typename A, typename B>                                  \
struct TypeProduct<                                                \
    A,                                                             \
    B,                                                             \
    typename std::enable_if<                                       \
        !isOrdinaryPair<A, B>() &&                                 \
        areSimilarPairs<A, B, T1, T2>()                            \
    >::type>                                                       \
{                                                                  \
    using Type = typename T3<typename CombinedNullity<A, B>::Type>;\
}

DEFINE_TYPE_PRODUCT_RULE(Enriched::Int8, Enriched::UInt16, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::Int8, Enriched::UInt32, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::Int16, Enriched::UInt16, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::Int16, Enriched::UInt32, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::Int32, Enriched::UInt32, Enriched::IntFloat64);

DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Int8, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Int16, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Int32, Enriched::Int32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Int64, Enriched::Int64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Float32, Enriched::Float32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Float64, Enriched::Float64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::UInt8, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::UInt16, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::UInt32, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::IntFloat32, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::IntFloat64, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Int8, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Int16, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Int32, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Int64, Enriched::Int64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Float32, Enriched::Float64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Float64, Enriched::Float64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::UInt8, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::UInt16, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::UInt32, Enriched::IntFloat64);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::IntFloat64, Enriched::IntFloat64);

DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat32, Enriched::Void, Enriched::IntFloat32);
DEFINE_TYPE_PRODUCT_RULE(Enriched::IntFloat64, Enriched::Void, Enriched::IntFloat64);

#undef DEFINE_TYPE_PRODUCT_RULE

}

}
