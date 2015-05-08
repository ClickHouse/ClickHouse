#pragma once

#include <DB/Functions/FunctionsArithmetic.h>
#include <cmath>	// log2()

namespace
{

/// Этот код генерирует во время сборки таблицу степеней числа 10.

/// Степени числа 10.

template <size_t N>
struct PowerOf10
{
    static const size_t value = 10 * PowerOf10<N - 1>::value;
};

template<>
struct PowerOf10<0>
{
    static const size_t value = 1;
};

/// Объявление и определение таблицы.

template <size_t... TArgs>
struct TablePowersOf10
{
    static const size_t value[sizeof...(TArgs)];
};

template <size_t... TArgs>
const size_t TablePowersOf10<TArgs...>::value[sizeof...(TArgs)] = { TArgs... };

/// Сгенерить первые N степеней.

template <size_t N, size_t... TArgs>
struct FillArrayImpl
{
    using result = typename FillArrayImpl<N - 1, PowerOf10<N>::value, TArgs...>::result;
};

template <size_t... TArgs>
struct FillArrayImpl<0, TArgs...>
{
    using result = TablePowersOf10<PowerOf10<0>::value, TArgs...>;
};

template <size_t N>
struct FillArray
{
    using result = typename FillArrayImpl<N-1>::result;
};

static const size_t powers_count = 16;
using powers_of_10 = FillArray<powers_count>::result;

}

namespace DB
{

	/** Функции округления:
	 * roundToExp2 - вниз до ближайшей степени двойки;
	 * roundDuration - вниз до ближайшего из: 0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000;
	 * roundAge - вниз до ближайшего из: 0, 18, 25, 35, 45.
	 * round(x, N) - арифметическое округление (N - сколько знаков после запятой оставить).
	 */

	template<typename A>
	struct RoundToExp2Impl
	{
		typedef A ResultType;

		static inline A apply(A x)
		{
			return x <= 0 ? static_cast<A>(0) : (static_cast<A>(1) << static_cast<UInt64>(log2(static_cast<double>(x))));
		}
	};

	template<>
	struct RoundToExp2Impl<Float32>
	{
		typedef Float32 ResultType;

		static inline Float32 apply(Float32 x)
		{
			return static_cast<Float32>(x < 1 ? 0. : pow(2., floor(log2(x))));
		}
	};

	template<>
	struct RoundToExp2Impl<Float64>
	{
		typedef Float64 ResultType;

		static inline Float64 apply(Float64 x)
		{
			return x < 1 ? 0. : pow(2., floor(log2(x)));
		}
	};

	template<typename A>
	struct RoundDurationImpl
	{
		typedef UInt16 ResultType;

		static inline ResultType apply(A x)
		{
			return x < 1 ? 0
				: (x < 10 ? 1
				: (x < 30 ? 10
				: (x < 60 ? 30
				: (x < 120 ? 60
				: (x < 180 ? 120
				: (x < 240 ? 180
				: (x < 300 ? 240
				: (x < 600 ? 300
				: (x < 1200 ? 600
				: (x < 1800 ? 1200
				: (x < 3600 ? 1800
				: (x < 7200 ? 3600
				: (x < 18000 ? 7200
				: (x < 36000 ? 18000
				: 36000))))))))))))));
		}
	};

	template<typename A>
	struct RoundAgeImpl
	{
		typedef UInt8 ResultType;

		static inline ResultType apply(A x)
		{
			return x < 18 ? 0
				: (x < 25 ? 18
				: (x < 35 ? 25
				: (x < 45 ? 35
				: 45)));
		}
	};

	template<typename A, typename B>
	struct RoundImpl
	{
		typedef A ResultType;

		template <typename Result = ResultType>
		static inline Result apply(A a, B b)
		{
			auto vb = typename NumberTraits::ToInteger<B>::Type(b);

			if (vb < 0)
				vb = 0;

			size_t scale = (vb < static_cast<int>(powers_count)) ? powers_of_10::value[vb] : pow(10, vb);
			return static_cast<Result>(round(a * scale) / scale);
		}
	};

	struct NameRoundToExp2		{ static constexpr auto name = "roundToExp2"; };
	struct NameRoundDuration	{ static constexpr auto name = "roundDuration"; };
	struct NameRoundAge 		{ static constexpr auto name = "roundAge"; };
	struct NameRound			{ static constexpr auto name = "round"; };

	typedef FunctionUnaryArithmetic<RoundToExp2Impl,	NameRoundToExp2> 	FunctionRoundToExp2;
	typedef FunctionUnaryArithmetic<RoundDurationImpl,	NameRoundDuration>	FunctionRoundDuration;
	typedef FunctionUnaryArithmetic<RoundAgeImpl,		NameRoundAge>		FunctionRoundAge;
	typedef FunctionBinaryArithmetic<RoundImpl,			NameRound>			FunctionRound;
}
