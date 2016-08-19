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

#include <DB/Core/Types.h>
#include <tuple>

namespace DB
{

/** Позволяет получить тип результата применения функций +, -, *, /, %, div (целочисленное деление).
  * Правила отличаются от используемых в C++.
  */

namespace NumberTraits
{

using Unsigned = boost::mpl::false_ ;
using Signed = boost::mpl::true_ ;

using Integer = boost::mpl::false_ ;
using Floating = boost::mpl::true_ ;

using Bits0 = boost::mpl::int_<0> ;
using Bits8 = boost::mpl::int_<8> ;
using Bits16 = boost::mpl::int_<16> ;
using Bits32 = boost::mpl::int_<32> ;
using Bits64 = boost::mpl::int_<64> ;
using BitsTooMany = boost::mpl::int_<1024>;

struct Error {};

template <typename T> struct Next;

template <> struct Next<Bits0>	{ using Type = Bits0; };
template <> struct Next<Bits8>	{ using Type = Bits16; };
template <> struct Next<Bits16>	{ using Type = Bits32; };
template <> struct Next<Bits32>	{ using Type = Bits64; };
template <> struct Next<Bits64>	{ using Type = Bits64; };

template <typename T> struct ExactNext { using Type = typename Next<T>::Type; };
template <> struct ExactNext<Bits64>	{ using Type = BitsTooMany; };

template <typename T> struct Traits;

template <> struct Traits<void>        { typedef Unsigned Sign;        typedef Integer Floatness;      typedef Bits0 Bits; };
template <> struct Traits<UInt8>       { typedef Unsigned Sign;        typedef Integer Floatness;      typedef Bits8 Bits; };
template <> struct Traits<UInt16>      { typedef Unsigned Sign;        typedef Integer Floatness;      typedef Bits16 Bits; };
template <> struct Traits<UInt32>      { typedef Unsigned Sign;        typedef Integer Floatness;      typedef Bits32 Bits; };
template <> struct Traits<UInt64>      { typedef Unsigned Sign;        typedef Integer Floatness;      typedef Bits64 Bits; };
template <> struct Traits<Int8>        { typedef Signed Sign;          typedef Integer Floatness;      typedef Bits8 Bits; };
template <> struct Traits<Int16>       { typedef Signed Sign;          typedef Integer Floatness;      typedef Bits16 Bits; };
template <> struct Traits<Int32>       { typedef Signed Sign;          typedef Integer Floatness;      typedef Bits32 Bits; };
template <> struct Traits<Int64>       { typedef Signed Sign;          typedef Integer Floatness;      typedef Bits64 Bits; };
template <> struct Traits<Float32>     { typedef Signed Sign;          typedef Floating Floatness;     typedef Bits32 Bits; };
template <> struct Traits<Float64>     { typedef Signed Sign;          typedef Floating Floatness;     typedef Bits64 Bits; };

template <typename Sign, typename Floatness, typename Bits> struct Construct;

template <> struct Construct<Unsigned, Integer, Bits0>	{ using Type = void	; };
template <> struct Construct<Unsigned, Floating, Bits0>	{ using Type = void	; };
template <> struct Construct<Signed, Integer, Bits0>	{ using Type = void	; };
template <> struct Construct<Signed, Floating, Bits0>	{ using Type = void	; };
template <> struct Construct<Unsigned, Integer, Bits8> 	{ using Type = UInt8 ; };
template <> struct Construct<Unsigned, Integer, Bits16> 	{ using Type = UInt16 ; };
template <> struct Construct<Unsigned, Integer, Bits32> 	{ using Type = UInt32 ; };
template <> struct Construct<Unsigned, Integer, Bits64> 	{ using Type = UInt64 ; };
template <> struct Construct<Unsigned, Floating, Bits8> 	{ using Type = Float32 ; };
template <> struct Construct<Unsigned, Floating, Bits16> 	{ using Type = Float32 ; };
template <> struct Construct<Unsigned, Floating, Bits32> 	{ using Type = Float32 ; };
template <> struct Construct<Unsigned, Floating, Bits64> 	{ using Type = Float64 ; };
template <> struct Construct<Signed, Integer, Bits8> 		{ using Type = Int8 	; };
template <> struct Construct<Signed, Integer, Bits16> 		{ using Type = Int16 ; };
template <> struct Construct<Signed, Integer, Bits32> 		{ using Type = Int32 ; };
template <> struct Construct<Signed, Integer, Bits64> 		{ using Type = Int64 ; };
template <> struct Construct<Signed, Floating, Bits8> 		{ using Type = Float32 ; };
template <> struct Construct<Signed, Floating, Bits16>		{ using Type = Float32 ; };
template <> struct Construct<Signed, Floating, Bits32>		{ using Type = Float32 ; };
template <> struct Construct<Signed, Floating, Bits64>		{ using Type = Float64 ; };
template <typename Sign, typename Floatness> struct Construct<Sign, Floatness, BitsTooMany> { using Type = Error; };

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

/** Результат сложения или умножения вычисляется по следующим правилам:
	* - если один из аргументов с плавающей запятой, то результат - с плавающей запятой, иначе - целый;
	* - если одно из аргументов со знаком, то результат - со знаком, иначе - без знака;
	* - результат содержит больше бит (не только значащих), чем максимум в аргументах
	*   (например, UInt8 + Int32 = Int64).
	*/
template <typename A, typename B> struct ResultOfAdditionMultiplication
{
	typedef typename Construct<
		typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
		typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
		typename Next<typename boost::mpl::max<typename Traits<A>::Bits, typename Traits<B>::Bits>::type>::Type>::Type Type;
};

template <typename A, typename B> struct ResultOfSubtraction
{
	typedef typename Construct<
		Signed,
		typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
		typename Next<typename boost::mpl::max<typename Traits<A>::Bits, typename Traits<B>::Bits>::type>::Type>::Type Type;
};

/** При делении всегда получается число с плавающей запятой.
	*/
template <typename A, typename B> struct ResultOfFloatingPointDivision
{
	using Type = Float64;
};

/** При целочисленном делении получается число, битность которого равна делимому.
	*/
template <typename A, typename B> struct ResultOfIntegerDivision
{
	typedef typename Construct<
		typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
		Integer,
		typename Traits<A>::Bits>::Type Type;
};

/** При взятии остатка получается число, битность которого равна делителю.
	*/
template <typename A, typename B> struct ResultOfModulo
{
	typedef typename Construct<
		typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
		Integer,
		typename Traits<B>::Bits>::Type Type;
};

template <typename A> struct ResultOfNegate
{
	typedef typename Construct<
		Signed,
		typename Traits<A>::Floatness,
		typename boost::mpl::if_<
			typename Traits<A>::Sign,
			typename Traits<A>::Bits,
			typename Next<typename Traits<A>::Bits>::Type>::type>::Type Type;
};

template <typename A> struct ResultOfAbs
{
	typedef typename Construct<
		Unsigned,
		typename Traits<A>::Floatness,
		typename Traits <A>::Bits>::Type Type;
};

/** При побитовых операциях получается целое число, битность которого равна максимальной из битностей аргументов.
	*/
template <typename A, typename B> struct ResultOfBit
{
	typedef typename Construct<
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
				typename Traits<B>::Bits>::type>::type>::Type Type;
};

template <typename A> struct ResultOfBitNot
{
	typedef typename Construct<
		typename Traits<A>::Sign,
		Integer,
		typename Traits<A>::Bits>::Type Type;
};

/** Приведение типов для функции if:
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
	typedef
		/// 1)
		typename boost::mpl::if_<
			typename boost::mpl::equal_to<typename Traits<A>::Bits, Bits0>::type,
			B,
		typename boost::mpl::if_<
			typename boost::mpl::equal_to<typename Traits<B>::Bits, Bits0>::type,
			A,
		/// 4) и 6)
		typename boost::mpl::if_<
			typename boost::mpl::or_<
				typename Traits<A>::Floatness,
				typename Traits<B>::Floatness>::type,
			typename Construct<
				Signed,
				Floating,
				typename boost::mpl::max< /// Этот максимум нужен только потому что if_ всегда вычисляет все аргументы.
					typename boost::mpl::max<
						typename boost::mpl::if_<
							typename Traits<A>::Floatness,
							typename Traits<A>::Bits,
							typename ExactNext<typename Traits<A>::Bits>::Type>::type,
						typename boost::mpl::if_<
							typename Traits<B>::Floatness,
							typename Traits<B>::Bits,
							typename ExactNext<typename Traits<B>::Bits>::Type>::type>::type,
					Bits32>::type>::Type,
		/// 2) и 3)
		typename boost::mpl::if_<
			typename boost::mpl::equal_to<
				typename Traits<A>::Sign,
				typename Traits<B>::Sign>::type,
			typename boost::mpl::if_<
				typename boost::mpl::less<
					typename Traits<A>::Bits,
					typename Traits<B>::Bits>::type,
				B,
				A>::type,
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
					typename ExactNext<typename Traits<B>::Bits>::Type>::type>::type>::Type>::type>::type>::type>::type Type;
};

/** Перед применением оператора % и побитовых операций, операнды приводятся к целым числам. */
template <typename A> struct ToInteger
{
	typedef typename Construct<
		typename Traits<A>::Sign,
		Integer,
		typename boost::mpl::if_<
			typename Traits<A>::Floatness,
			Bits64,
			typename Traits<A>::Bits>::type>::Type Type;
};

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
using Void = std::tuple<void, void>;
using Int8 = std::tuple<DB::Int8, void>;
using Int16 = std::tuple<DB::Int16, void>;
using Int32 = std::tuple<DB::Int32, void>;
using Int64 = std::tuple<DB::Int64, void>;
using UInt8 = std::tuple<DB::UInt8, void>;
using UInt16 = std::tuple<DB::UInt16, void>;
using UInt32 = std::tuple<DB::UInt32, void>;
using UInt64 = std::tuple<DB::UInt64, void>;
using Float32 = std::tuple<DB::Float32, void>;
using Float64 = std::tuple<DB::Float64, void>;
using IntFloat32 = std::tuple<DB::Int32, DB::Float32>;
using IntFloat64 = std::tuple<DB::Int64, DB::Float64>;

}

/// Embed an ordinary type into the corresponding enriched type.
template <typename T>
struct EmbedType;

template <> struct EmbedType<void> { using Type = Enriched::Void; };
template <> struct EmbedType<Int8> { using Type = Enriched::Int8; };
template <> struct EmbedType<Int16> { using Type = Enriched::Int16; };
template <> struct EmbedType<Int32> { using Type = Enriched::Int32; };
template <> struct EmbedType<Int64> { using Type = Enriched::Int64; };
template <> struct EmbedType<UInt8> { using Type = Enriched::UInt8; };
template <> struct EmbedType<UInt16> { using Type = Enriched::UInt16; };
template <> struct EmbedType<UInt32> { using Type = Enriched::UInt32; };
template <> struct EmbedType<UInt64> { using Type = Enriched::UInt64; };
template <> struct EmbedType<Float32> { using Type = Enriched::Float32; };
template <> struct EmbedType<Float64> { using Type = Enriched::Float64; };

/// Get an ordinary type from an enriched type.
template <typename TType>
struct ToOrdinaryType
{
	using Type = typename std::tuple_element<0, TType>::type;
};

/// Get an ordinary type from an enriched type.
/// Error case.
template <>
struct ToOrdinaryType<Error>
{
	using Type = Error;
};

/// Compute the product of two enriched numeric types.
template <typename T1, typename T2>
struct TypeProduct
{
	using Type = Error;
};

/// Compute the product of two enriched numeric types.
/// Case when both of the source types and the resulting type map to ordinary types.
template <typename A, typename B>
struct TypeProduct<std::tuple<A, void>, std::tuple<B, void> >
{
private:
	using Result = typename NumberTraits::ResultOfIf<A, B>::Type;

public:
	using Type = typename std::conditional<
		std::is_same<Result, Error>::value,
		Error,
		std::tuple<Result, void>
	>::type;
};

/// Compute the product of two enriched numeric types.
/// Case when a source type or the resulting type does not map to any ordinary type.
template <> struct TypeProduct<Enriched::Int8, Enriched::UInt16> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::UInt16, Enriched::Int8> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::Int8, Enriched::UInt32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt32, Enriched::Int8> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::Int16, Enriched::UInt16> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::UInt16, Enriched::Int16> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::Int16, Enriched::UInt32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt32, Enriched::Int16> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::Int32, Enriched::UInt32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt32, Enriched::Int32> { using Type = Enriched::IntFloat64; };

template <> struct TypeProduct<Enriched::IntFloat32, Enriched::Int8> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::Int8, Enriched::IntFloat32> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::Int16> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::Int16, Enriched::IntFloat32> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::Int32> { using Type = Enriched::Int32; };
template <> struct TypeProduct<Enriched::Int32, Enriched::IntFloat32> { using Type = Enriched::Int32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::Int64> { using Type = Enriched::Int64; };
template <> struct TypeProduct<Enriched::Int64, Enriched::IntFloat32> { using Type = Enriched::Int64; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::Float32> { using Type = Enriched::Float32; };
template <> struct TypeProduct<Enriched::Float32, Enriched::IntFloat32> { using Type = Enriched::Float32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::Float64> { using Type = Enriched::Float64; };
template <> struct TypeProduct<Enriched::Float64, Enriched::IntFloat32> { using Type = Enriched::Float64; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::UInt8> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::UInt8, Enriched::IntFloat32> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::UInt16> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::UInt16, Enriched::IntFloat32> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::UInt32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt32, Enriched::IntFloat32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::IntFloat32> { using Type = Enriched::IntFloat32; };
template <> struct TypeProduct<Enriched::IntFloat32, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::IntFloat32> { using Type = Enriched::IntFloat64; };

template <> struct TypeProduct<Enriched::IntFloat64, Enriched::Int8> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::Int8, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::Int16> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::Int16, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::Int32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::Int32, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::Int64> { using Type = Enriched::Int64; };
template <> struct TypeProduct<Enriched::Int64, Enriched::IntFloat64> { using Type = Enriched::Int64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::Float32> { using Type = Enriched::Float64; };
template <> struct TypeProduct<Enriched::Float32, Enriched::IntFloat64> { using Type = Enriched::Float64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::Float64> { using Type = Enriched::Float64; };
template <> struct TypeProduct<Enriched::Float64, Enriched::IntFloat64> { using Type = Enriched::Float64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::UInt8> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt8, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::UInt16> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt16, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::UInt32> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::UInt32, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };
template <> struct TypeProduct<Enriched::IntFloat64, Enriched::IntFloat64> { using Type = Enriched::IntFloat64; };

}

}
