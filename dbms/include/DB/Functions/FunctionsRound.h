#pragma once

#include <DB/Functions/FunctionsArithmetic.h>
#include <cmath>
#include <type_traits>
#include <array>


namespace DB
{

	/** Функции округления:
	 * roundToExp2 - вниз до ближайшей степени двойки;
	 * roundDuration - вниз до ближайшего из: 0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000;
	 * roundAge - вниз до ближайшего из: 0, 18, 25, 35, 45.
	 * round(x, N) - арифметическое округление (N - сколько знаков после запятой оставить; 0 по умолчанию).
	 * ceil(x, N) - наименьшее число, которое не меньше x (N - сколько знаков после запятой оставить; 0 по умолчанию).
	 * floor(x, N) - наибольшее число, которое не больше x (N - сколько знаков после запятой оставить; 0 по умолчанию).
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

	/// Реализация функций округления на низком уровне.

	template<typename T, int rounding_mode>
	struct RoundingComputation
	{
	};

	template<int rounding_mode>
	struct RoundingComputation<Float32, rounding_mode>
	{
		using Data = std::array<Float32, 4>;
		using Scale = __m128;

		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
			Float32 fscale = static_cast<Float32>(scale);
			mm_scale = _mm_load1_ps(&fscale);
		}

		static inline void compute(const Data & in, const Scale & mm_scale, Data & out)
		{
			Float32 input[4] __attribute__((aligned(16))) = {in[0], in[1], in[2], in[3]};
			__m128 mm_value = _mm_load_ps(input);

			mm_value = _mm_mul_ps(mm_value, mm_scale);
			mm_value = _mm_round_ps(mm_value, rounding_mode);
			mm_value = _mm_div_ps(mm_value, mm_scale);

			Float32 res[4] __attribute__((aligned(16)));
			_mm_store_ps(res, mm_value);
			out = {res[0], res[1], res[2], res[3]};
		}
	};

	template<int rounding_mode>
	struct RoundingComputation<Float64, rounding_mode>
	{
		using Data = std::array<Float64, 2>;
		using Scale = __m128d;

		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
			Float64 fscale = static_cast<Float64>(scale);
			mm_scale = _mm_load1_pd(&fscale);
		}

		static inline void compute(const Data & in, const Scale & mm_scale, Data & out)
		{
			Float64 input[2] __attribute__((aligned(16))) = { in[0], in[1] };
			__m128d mm_value = _mm_load_pd(input);

			mm_value = _mm_mul_pd(mm_value, mm_scale);
			mm_value = _mm_round_pd(mm_value, rounding_mode);
			mm_value = _mm_div_pd(mm_value, mm_scale);

			Float64 res[2] __attribute__((aligned(16)));
			_mm_store_pd(res, mm_value);
			out = {res[0], res[1]};
		}
	};

	/// Реализация функций округления на высоком уровне.

	template<typename T, int rounding_mode, typename Enable = void>
	struct FunctionRoundingImpl
	{
	};

	/// В случае целочисленных значений не выполяется округления.
	template<typename T, int rounding_mode>
	struct FunctionRoundingImpl<T, rounding_mode, typename std::enable_if<std::is_integral<T>::value>::type>
	{
		static inline void apply(const PODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
		{
			size_t size = in.size();
			for (size_t i = 0; i < size; ++i)
				out[i] = in[i];
		}

		static inline T apply(T val, size_t scale)
		{
			return val;
		}
	};

	template<typename T, int rounding_mode>
	struct FunctionRoundingImpl<T, rounding_mode, typename std::enable_if<std::is_floating_point<T>::value>::type>
	{
	private:
		using Op = RoundingComputation<T, rounding_mode>;
		using Data = typename Op::Data;
		using Scale = typename Op::Scale;

	public:
		static inline void apply(const PODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
		{
			Scale mm_scale;
			Op::prepareScale(scale, mm_scale);

			const size_t size = in.size();
			const size_t data_size = std::tuple_size<Data>();

			size_t i;
			for (i = 0; i < (size - data_size + 1); i += data_size)
			{
				Data tmp;
				for (size_t j = 0; j < data_size; ++j)
					tmp[j] = in[i + j];

				Data res;
				Op::compute(tmp, mm_scale, res);

				for (size_t j = 0; j < data_size; ++j)
					out[i + j] = res[j];
			}

			if (i < size)
			{
				Data tmp{0};
				for (size_t j = 0; (j < data_size) && ((i + j) < size); ++j)
					tmp[j] = in[i + j];

				Data res;
				Op::compute(tmp, mm_scale, res);

				for (size_t j = 0; (j < data_size) && ((i + j) < size); ++j)
					out[i + j] = in[i + j];
			}
		}

		static inline T apply(T val, size_t scale)
		{
			if (val == 0)
				return val;
			else
			{
				Scale mm_scale;
				Op::prepareScale(scale, mm_scale);

				Data tmp{0};
				tmp[0] = val;

				Data res;
				Op::compute(tmp, mm_scale, res);
				return res[0];
			}
		}
	};

	template<typename T, typename U>
	struct PrecisionForType
	{
		template<typename L = T>
		static inline bool apply(const ColumnPtr & column, UInt8 & precision,
								 typename std::enable_if<std::is_floating_point<L>::value>::type * = nullptr)
		{
			using ColumnType = ColumnConst<U>;

			const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
			if (precision_col == nullptr)
				return false;

			U val = precision_col->getData();
			if (val < 0)
				val = 0;
			else if (val >= static_cast<U>(std::numeric_limits<L>::digits10))
				val = static_cast<U>(std::numeric_limits<L>::digits10);

			precision = static_cast<UInt8>(val);

			return true;
		}

		/// Для целых чисел точность не имеет значения.
		template<typename L = T>
		static inline bool apply(const ColumnPtr & column, UInt8 & precision,
								 typename std::enable_if<std::is_integral<L>::value>::type * = nullptr)
		{
			using ColumnType = ColumnConst<U>;

			const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
			if (precision_col == nullptr)
				return false;

			precision = 0;

			return true;
		}
	};

	/// Следующий код генерирует во время сборки таблицу степеней числа 10.

namespace
{
	/// Отдельные степени числа 10.

	template<size_t N>
	struct PowerOf10
	{
		static const size_t value = 10 * PowerOf10<N - 1>::value;
	};

	template<>
	struct PowerOf10<0>
	{
		static const size_t value = 1;
	};
}

	/// Объявление и определение контейнера содержащего таблицу степеней числа 10.

	template<size_t... TArgs>
	struct TableContainer
	{
		static const std::array<size_t, sizeof...(TArgs)> values;
	};

	template<size_t... TArgs>
	const std::array<size_t, sizeof...(TArgs)> TableContainer<TArgs...>::values = { TArgs... };

	/// Генератор первых N степеней.

	template<size_t N, size_t... TArgs>
	struct FillArrayImpl
	{
		using result = typename FillArrayImpl<N - 1, PowerOf10<N>::value, TArgs...>::result;
	};

	template<size_t... TArgs>
	struct FillArrayImpl<0, TArgs...>
	{
		using result = TableContainer<PowerOf10<0>::value, TArgs...>;
	};

	template<size_t N>
	struct FillArray
	{
		using result = typename FillArrayImpl<N - 1>::result;
	};

	/** Шаблон для функций, которые вычисляют приближенное значение входного параметра
	  * типа (U)Int8/16/32/64 или Float32/64 и принимают дополнительный необязятельный
	  * параметр указывающий сколько знаков после запятой оставить (по умолчанию - 0).
	  * Op - функция (round/floor/ceil)
	  */
	template<typename Name, int rounding_mode>
	class FunctionRounding : public IFunction
	{
	public:
		static constexpr auto name = Name::name;
		static IFunction * create(const Context & context) { return new FunctionRounding; }

	private:
		using PowersOf10 = FillArray<std::numeric_limits<Float64>::digits10 + 1>::result;

	private:
		template<typename T>
		bool checkType(const IDataType * type) const
		{
			return typeid_cast<const T *>(type) != nullptr;
		}

		template<typename T>
		bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result)
		{
			using Op = FunctionRoundingImpl<T, rounding_mode>;

			if (ColumnVector<T> * col = typeid_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
			{
				UInt8 precision = 0;
				if (arguments.size() == 2)
					precision = getPrecision<T>(block.getByPosition(arguments[1]).column);

				ColumnVector<T> * col_res = new ColumnVector<T>;
				block.getByPosition(result).column = col_res;

				typename ColumnVector<T>::Container_t & vec_res = col_res->getData();
				vec_res.resize(col->getData().size());

				Op::apply(col->getData(), PowersOf10::values[precision], vec_res);

				return true;
			}
			else if (ColumnConst<T> * col = typeid_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
			{
				UInt8 precision = 0;
				if (arguments.size() == 2)
					precision = getPrecision<T>(block.getByPosition(arguments[1]).column);

				T res = Op::apply(col->getData(), PowersOf10::values[precision]);

				ColumnConst<T> * col_res = new ColumnConst<T>(col->size(), res);
				block.getByPosition(result).column = col_res;

				return true;
			}

			return false;
		}

		/// В зависимости от входного параметра, определить какая нужна точность
		/// для результата.
		template<typename T>
		UInt8 getPrecision(const ColumnPtr & column)
		{
			UInt8 precision = 0;

			if (!(	PrecisionForType<T, UInt8>::apply(column, precision)
				||	PrecisionForType<T, UInt16>::apply(column, precision)
				||	PrecisionForType<T, UInt16>::apply(column, precision)
				||	PrecisionForType<T, UInt32>::apply(column, precision)
				||	PrecisionForType<T, UInt64>::apply(column, precision)
				||	PrecisionForType<T, Int8>::apply(column, precision)
				||	PrecisionForType<T, Int16>::apply(column, precision)
				||	PrecisionForType<T, Int32>::apply(column, precision)
				||	PrecisionForType<T, Int64>::apply(column, precision)))
			{
				throw Exception("Illegal column " + column->getName()
						+ " of second ('precision') argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}

			return precision;
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
					+ toString(arguments.size()) + ", should be 1 or 2.",
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

			if (arguments.size() == 2)
			{
				const IDataType * type = &*arguments[1];
				if (!( checkType<DataTypeUInt8>(type)
					|| checkType<DataTypeUInt16>(type)
					|| checkType<DataTypeUInt32>(type)
					|| checkType<DataTypeUInt64>(type)
					|| checkType<DataTypeInt8>(type)
					|| checkType<DataTypeInt16>(type)
					|| checkType<DataTypeInt32>(type)
					|| checkType<DataTypeInt64>(type)))
				{
					throw Exception("Illegal type in second argument of function " + getName(),
									ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
				}
			}

			const IDataType * type = &*arguments[0];
			if (!type->behavesAsNumber())
				throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			return arguments[0];
		}

		/// Выполнить функцию над блоком.
		void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
		{
			if (!(	executeForType<UInt8>(block, arguments, result)
				||	executeForType<UInt16>(block, arguments, result)
				||	executeForType<UInt32>(block, arguments, result)
				||	executeForType<UInt64>(block, arguments, result)
				||	executeForType<Int8>(block, arguments, result)
				||	executeForType<Int16>(block, arguments, result)
				||	executeForType<Int32>(block, arguments, result)
				||	executeForType<Int64>(block, arguments, result)
				||	executeForType<Float32>(block, arguments, result)
				||	executeForType<Float64>(block, arguments, result)))
			{
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
			}
		}
	};

	struct NameRoundToExp2		{ static constexpr auto name = "roundToExp2"; };
	struct NameRoundDuration	{ static constexpr auto name = "roundDuration"; };
	struct NameRoundAge 		{ static constexpr auto name = "roundAge"; };
	struct NameRound			{ static constexpr auto name = "round"; };
	struct NameCeil				{ static constexpr auto name = "ceil"; };
	struct NameFloor			{ static constexpr auto name = "floor"; };

	typedef FunctionUnaryArithmetic<RoundToExp2Impl,	NameRoundToExp2> 	FunctionRoundToExp2;
	typedef FunctionUnaryArithmetic<RoundDurationImpl,	NameRoundDuration>	FunctionRoundDuration;
	typedef FunctionUnaryArithmetic<RoundAgeImpl,		NameRoundAge>		FunctionRoundAge;
	typedef FunctionRounding<NameRound,	_MM_FROUND_NINT>	FunctionRound;
	typedef FunctionRounding<NameCeil,	_MM_FROUND_CEIL>	FunctionCeil;
	typedef FunctionRounding<NameFloor,	_MM_FROUND_FLOOR>	FunctionFloor;
}
