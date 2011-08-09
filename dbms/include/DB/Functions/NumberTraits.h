#pragma once

#include <boost/mpl/bool.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/if.hpp>
#include <boost/mpl/and.hpp>
#include <boost/mpl/or.hpp>
#include <boost/mpl/not.hpp>
#include <boost/mpl/greater.hpp>
#include <boost/mpl/min_max.hpp>

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

	template <typename T> struct Next;

	template <> struct Next<Bits8>	{ typedef Bits16 Type; };
	template <> struct Next<Bits16>	{ typedef Bits32 Type; };
	template <> struct Next<Bits32>	{ typedef Bits64 Type; };
	template <> struct Next<Bits64>	{ typedef Bits64 Type; };

	template <typename T> struct Traits;

	template <> struct Traits<UInt8> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits8 Bits; };
	template <> struct Traits<UInt16> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits16 Bits; };
	template <> struct Traits<UInt32> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits32 Bits; };
	template <> struct Traits<UInt64> 	{ typedef Unsigned Sign;	typedef Integer Floatness;	typedef Bits64 Bits; };
	template <> struct Traits<Int8> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits8 Bits; };
	template <> struct Traits<Int16> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits16 Bits; };
	template <> struct Traits<Int32> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits32 Bits; };
	template <> struct Traits<Int64> 	{ typedef Signed Sign;		typedef Integer Floatness;	typedef Bits64 Bits; };
	template <> struct Traits<Float32> 	{ typedef Signed Sign;		typedef Floating Floatness;	typedef Bits32 Bits; };
	template <> struct Traits<Float64> 	{ typedef Signed Sign;		typedef Floating Floatness;	typedef Bits64 Bits; };

	template <typename Sign, typename Floatness, typename Bits> struct Construct;

	template <> struct Construct<Unsigned, Integer, Bits8> 		{ typedef UInt8 	Type; };
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
	template <> struct Construct<Signed, Floating, Bits16> 		{ typedef Float32 	Type; };
	template <> struct Construct<Signed, Floating, Bits32> 		{ typedef Float32 	Type; };
	template <> struct Construct<Signed, Floating, Bits64> 		{ typedef Float64 	Type; };

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
			typename boost::mpl::if_<
				typename boost::mpl::and_<
					typename boost::mpl::not_<typename Traits<A>::Sign>,
					typename boost::mpl::not_<typename Traits<A>::Floatness>,
					typename boost::mpl::not_<typename Traits<B>::Floatness>,
					typename boost::mpl::greater<typename Traits<A>::Bits, typename Traits<B>::Bits> >,
				Unsigned,
				Signed>::type,
			typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
			typename boost::mpl::if_<
				typename boost::mpl::and_<
					typename boost::mpl::not_<typename Traits<A>::Sign>,
					typename boost::mpl::not_<typename Traits<B>::Sign>,
					typename boost::mpl::not_<typename Traits<A>::Floatness>,
					typename boost::mpl::not_<typename Traits<B>::Floatness>,
					typename boost::mpl::greater<typename Traits<A>::Bits, typename Traits<B>::Bits> >,
				typename Traits<A>::Bits,
				typename Next<typename boost::mpl::max<typename Traits<A>::Bits, typename Traits<B>::Bits>::type>::Type>::type>::Type Type;
	};

	/** При делении всегда получается число с плавающей запятой.
	  */
	template <typename A, typename B> struct ResultOfFloatingPointDivision
	{
		typedef typename Construct<
			Signed,
			Floating,
			typename Next<typename boost::mpl::max<typename Traits<A>::Bits, typename Traits<B>::Bits>::type>::Type>::Type Type;
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
			typename boost::mpl::or_<typename Traits<A>::Floatness, typename Traits<B>::Floatness>::type,
			typename Traits<B>::Bits>::Type Type;
	};
}

	
}
