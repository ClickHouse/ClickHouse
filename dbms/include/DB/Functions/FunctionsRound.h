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

	template<typename A>
	struct RoundImpl
	{
		static inline A apply(A a, Int8 scale)
		{
			if (scale < 0)
				scale = 0;

			size_t power = (scale < static_cast<Int8>(powers_count)) ? powers_of_10::value[scale] : pow(10, scale);
			return static_cast<A>(round(a * power) / power);
		}
	};

	class FunctionRound : public IFunction
	{
	public:
		static constexpr auto name = "round";
		static IFunction * create(const Context & context) { return new FunctionRound; }

	private:
		template <typename T0>
		bool executeType(Block & block, const ColumnNumbers & arguments, Int8 scale, size_t result)
		{
			if (ColumnVector<T0> * col = typeid_cast<ColumnVector<T0> *>(&*block.getByPosition(arguments[0]).column))
			{
				ColumnVector<T0> * col_res = new ColumnVector<T0>;
				block.getByPosition(result).column = col_res;

				typename ColumnVector<T0>::Container_t & vec_res = col_res->getData();
				vec_res.resize(col->getData().size());

				const PODArray<T0> & a = col->getData();
				size_t size = a.size();
				for (size_t i = 0; i < size; ++i)
					vec_res[i] = RoundImpl<T0>::apply(a[i], scale);

				return true;
			}
			else if (ColumnConst<T0> * col = typeid_cast<ColumnConst<T0> *>(&*block.getByPosition(arguments[0]).column))
			{
				T0 res = RoundImpl<T0>::apply(col->getData(), scale);

				ColumnConst<T0> * col_res = new ColumnConst<T0>(col->size(), res);
				block.getByPosition(result).column = col_res;

				return true;
			}

			return false;
		}

	public:
		/// Получить имя функции.
		String getName() const override
		{
			return name;
		}

		/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
		DataTypePtr getReturnType(const DataTypes & arguments) const override
		{
			if ((arguments.size() < 1) || (arguments.size() > 2))
				throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
					+ toString(arguments.size()) + ", should be 1.",
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

			if ((arguments.size() == 2) && (arguments[1]->getName() != TypeName<Int8>::get()))
				throw Exception("Illegal type in second argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const IDataType * type = &*arguments[0];
			if (!type->behavesAsNumber())
				throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			return arguments[0];

			DataTypePtr result;
		}

		/// Выполнить функцию над блоком.
		void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
		{
			Int8 scale = 0;
			if (arguments.size() == 2)
			{
				const ColumnConst<Int8> * scale_col = typeid_cast<const ColumnConstInt8 *>(&*block.getByPosition(arguments[1]).column);

				if (scale_col == nullptr)
					throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
						+ " of second ('scale') argument of function " + name
						+ ". Must be constant int8.",
						ErrorCodes::ILLEGAL_COLUMN);

				scale = scale_col->getData();
			}

			if (!(	executeType<UInt8>(block, arguments, scale, result)
				||	executeType<UInt16>(block, arguments, scale, result)
				||	executeType<UInt32>(block, arguments, scale, result)
				||	executeType<UInt64>(block, arguments, scale, result)
				||	executeType<Int8>(block, arguments, scale, result)
				||	executeType<Int16>(block, arguments, scale, result)
				||	executeType<Int32>(block, arguments, scale, result)
				||	executeType<Int64>(block, arguments, scale, result)
				||	executeType<Float32>(block, arguments, scale, result)
				||	executeType<Float64>(block, arguments, scale, result)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
	};

	struct NameRoundToExp2		{ static constexpr auto name = "roundToExp2"; };
	struct NameRoundDuration	{ static constexpr auto name = "roundDuration"; };
	struct NameRoundAge 		{ static constexpr auto name = "roundAge"; };

	typedef FunctionUnaryArithmetic<RoundToExp2Impl,	NameRoundToExp2> 	FunctionRoundToExp2;
	typedef FunctionUnaryArithmetic<RoundDurationImpl,	NameRoundDuration>	FunctionRoundDuration;
	typedef FunctionUnaryArithmetic<RoundAgeImpl,		NameRoundAge>		FunctionRoundAge;
}
