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


namespace DB
{

/** Позволяет получить тип результата применения функций +, -, *, /, %, div (целочисленное деление).
  * Правила отличаются от используемых в C++.
  */

namespace NumberTraits
{
	typedef boost::mpl::false_ 	Unsigned;
	typedef boost::mpl::true_ 	Signed;

	typedef boost::mpl::false_ 	Integer;
	typedef boost::mpl::true_ 	Floating;

	typedef boost::mpl::int_<8> 	Bits8;
	typedef boost::mpl::int_<16> 	Bits16;
	typedef boost::mpl::int_<32> 	Bits32;
	typedef boost::mpl::int_<64> 	Bits64;
	typedef boost::mpl::int_<1024> BitsTooMany;
	
	struct Error {};

	template <typename T> struct Next;

	template <> struct Next<Bits8>		{ typedef Bits16 Type; };
	template <> struct Next<Bits16>	{ typedef Bits32 Type; };
	template <> struct Next<Bits32>	{ typedef Bits64 Type; };
	template <> struct Next<Bits64>	{ typedef Bits64 Type; };
	
	template <typename T> struct ExactNext { typedef typename Next<T>::Type Type; };
	template <> struct ExactNext<Bits64>	{ typedef BitsTooMany Type; };

	template <typename T> struct Traits;

	template <> struct Traits<UInt8> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits8 Bits; };
	template <> struct Traits<UInt16> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits16 Bits; };
	template <> struct Traits<UInt32> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits32 Bits; };
	template <> struct Traits<UInt64> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits64 Bits; };
	template <> struct Traits<Int8> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits8 Bits; };
	template <> struct Traits<Int16> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits16 Bits; };
	template <> struct Traits<Int32> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits32 Bits; };
	template <> struct Traits<Int64> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits64 Bits; };
	template <> struct Traits<Float32>	{ typedef Signed Sign;		typedef Floating Floatness;	typedef Bits32 Bits; };
	template <> struct Traits<Float64>	{ typedef Signed Sign;		typedef Floating Floatness;	typedef Bits64 Bits; };

	template <typename Sign, typename Floatness, typename Bits> struct Construct;

	template <> struct Construct<Unsigned, Integer, Bits8> 	{ typedef UInt8 	Type; };
	template <> struct Construct<Unsigned, Integer, Bits16> 	{ typedef UInt16 	Type; };
	template <> struct Construct<Unsigned, Integer, Bits32> 	{ typedef UInt32 	Type; };
	template <> struct Construct<Unsigned, Integer, Bits64> 	{ typedef UInt64 	Type; };
	template <> struct Construct<Unsigned, Floating, Bits8> 	{ typedef Float32 	Type; };
	template <> struct Construct<Unsigned, Floating, Bits16> 	{ typedef Float32 	Type; };
	template <> struct Construct<Unsigned, Floating, Bits32> 	{ typedef Float32 	Type; };
	template <> struct Construct<Unsigned, Floating, Bits64> 	{ typedef Float64 	Type; };
	template <> struct Construct<Signed, Integer, Bits8> 		{ typedef Int8 		Type; };
	template <> struct Construct<Signed, Integer, Bits16> 		{ typedef Int16 	Type; };
	template <> struct Construct<Signed, Integer, Bits32> 		{ typedef Int32 	Type; };
	template <> struct Construct<Signed, Integer, Bits64> 		{ typedef Int64 	Type; };
	template <> struct Construct<Signed, Floating, Bits8> 		{ typedef Float32 	Type; };
	template <> struct Construct<Signed, Floating, Bits16>		{ typedef Float32 	Type; };
	template <> struct Construct<Signed, Floating, Bits32>		{ typedef Float32 	Type; };
	template <> struct Construct<Signed, Floating, Bits64>		{ typedef Float64 	Type; };
	template <typename Sign, typename Floatness> struct Construct<Sign, Floatness, BitsTooMany> { typedef Error Type; };
				
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
		typedef Float64 Type;
	};

	/** При целочисленном делении получается число, битность которого равна делимому.
	  */
	template <typename A, typename B> struct ResultOfIntegerDivision
	{
		typedef typename Construct<
			typename boost::mpl::or_<typename Traits<A>::Sign, typename Traits<B>::Sign>::type,
			typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
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
	  * 1)  UInt<x>,   UInt<y> ->  UInt<max(x,y)>
	  * 2)   Int<x>,    Int<y> ->   Int<max(x,y)>
	  * 3) Float<x>,  Float<y> -> Float<max(x, y)>
	  * 4)  UInt<x>,    Int<y> ->   Int<max(x*2, y)>
	  * 5) Float<x>, [U]Int<y> -> Float<max(x, y*2)>
	  * 6)  UInt64 ,    Int<x> -> Error
	  * 7) Float<x>, [U]Int64  -> Error
	  */
	template <typename A, typename B>
	struct ResultOfIf
	{
		typedef 
			/// 3) и 5)
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
			/// 1) и 2)
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
			/// 4)
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
						typename ExactNext<typename Traits<B>::Bits>::Type>::type>::type>::Type>::type>::type Type;
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
}

	
}
