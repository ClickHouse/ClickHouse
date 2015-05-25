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
	 * round(x, N) - арифметическое округление (N = 0 по умолчанию).
	 * ceil(x, N) - наименьшее число, которое не меньше x (N = 0 по умолчанию).
	 * floor(x, N) - наибольшее число, которое не больше x (N = 0 по умолчанию).
	 *
	 * Значение параметра N:
	 * - N > 0: округлять до числа с N десятичными знаками после запятой
	 * - N < 0: окурглять до целого числа с N нулевыми знаками
	 * - N = 0: округлять до целого числа
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

	/** Этот параметр контролирует поведение функций округления.
	  */
	enum ScaleMode
	{
		PositiveScale,	// округлять до числа с N десятичными знаками после запятой
		NegativeScale,  // окурглять до целого числа с N нулевыми знаками
		ZeroScale,		// округлять до целого числа
		NullScale 		// возвращать нулевое значение
	};

	/** Реализация низкоуровневых функций округления для целочисленных значений.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode, typename Enable = void>
	struct IntegerRoundingComputation
	{
	};

	template<typename T, int rounding_mode, ScaleMode scale_mode>
	struct IntegerRoundingComputation<T, rounding_mode, scale_mode,
		typename std::enable_if<std::is_integral<T>::value
			&& ((scale_mode == PositiveScale) || (scale_mode == ZeroScale))>::type>
	{
		static inline T compute(const T in, size_t scale)
		{
			return in;
		}
	};

	template<typename T>
	struct IntegerRoundingComputation<T, _MM_FROUND_NINT, NegativeScale,
		typename std::enable_if<std::is_integral<T>::value>::type>
	{
		static inline T compute(T in, size_t scale)
		{
			T rem = in % scale;
			if (rem == in)
				return 0;

			in -= rem;
			if (static_cast<size_t>(2 * rem) < scale)
				return in;
			else
				return in + scale;
		}
	};

	template<typename T>
	struct IntegerRoundingComputation<T, _MM_FROUND_CEIL, NegativeScale,
		typename std::enable_if<std::is_integral<T>::value>::type>
	{
		static inline T compute(const T in, size_t scale)
		{
			T rem = in % scale;
			if (rem == in)
				return 0;

			return in - rem + scale;
		}
	};

	template<typename T>
	struct IntegerRoundingComputation<T, _MM_FROUND_FLOOR, NegativeScale,
		typename std::enable_if<std::is_integral<T>::value>::type>
	{
		static inline T compute(const T in, size_t scale)
		{
			T rem = in % scale;
			if (rem == in)
				return 0;

			return in - rem;
		}
	};

	template<typename T>
	struct BaseFloatRoundingComputation
	{
	};

	template<>
	struct BaseFloatRoundingComputation<Float32>
	{
		using Scale = __m128;
		static const size_t data_count = 4;
	};

	template<>
	struct BaseFloatRoundingComputation<Float64>
	{
		using Scale = __m128d;
		static const size_t data_count = 2;
	};

	/** Реализация низкоуровневых функций округления для значений с плавающей точкой.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode>
	struct FloatRoundingComputation : public BaseFloatRoundingComputation<T>
	{
	};

	template<int rounding_mode>
	struct FloatRoundingComputation<Float32, rounding_mode, PositiveScale>
		: public BaseFloatRoundingComputation<Float32>
	{
		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
			Float32 fscale = static_cast<Float32>(scale);
			mm_scale = _mm_load1_ps(&fscale);
		}

		static inline void compute(const Float32 * in, const Scale & scale, Float32 * out)
		{
			__m128 val = _mm_loadu_ps(in);
			val = _mm_mul_ps(val, scale);
			val = _mm_round_ps(val, rounding_mode);
			val = _mm_div_ps(val, scale);
			_mm_storeu_ps(out, val);
		}
	};

	template<int rounding_mode>
	struct FloatRoundingComputation<Float32, rounding_mode, NegativeScale>
		: public BaseFloatRoundingComputation<Float32>
	{
		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
			Float32 fscale = static_cast<Float32>(scale);
			mm_scale = _mm_load1_ps(&fscale);
		}

		static inline void compute(const Float32 * in, const Scale & scale, Float32 * out)
		{
			__m128 val = _mm_loadu_ps(in);
			val = _mm_div_ps(val, scale);
			__m128 res = _mm_cmpge_ps(val, getOne());
			val = _mm_round_ps(val, rounding_mode);
			val = _mm_mul_ps(val, scale);
			val = _mm_and_ps(val, res);
			_mm_storeu_ps(out, val);
		}

	private:
		static inline const __m128 & getOne()
		{
			static const __m128 one = _mm_set1_ps(1.0);
			return one;
		}
	};

	template<int rounding_mode>
	struct FloatRoundingComputation<Float32, rounding_mode, ZeroScale>
		: public BaseFloatRoundingComputation<Float32>
	{
		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
		}

		static inline void compute(const Float32 * in, const Scale & scale, Float32 * out)
		{
			__m128 val = _mm_loadu_ps(in);
			val = _mm_round_ps(val, rounding_mode);
			_mm_storeu_ps(out, val);
		}
	};

	template<int rounding_mode>
	struct FloatRoundingComputation<Float64, rounding_mode, PositiveScale>
		: public BaseFloatRoundingComputation<Float64>
	{
		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
			Float64 fscale = static_cast<Float64>(scale);
			mm_scale = _mm_load1_pd(&fscale);
		}

		static inline void compute(const Float64 * in, const Scale & scale, Float64 * out)
		{
			__m128d val = _mm_loadu_pd(in);
			val = _mm_mul_pd(val, scale);
			val = _mm_round_pd(val, rounding_mode);
			val = _mm_div_pd(val, scale);
			_mm_storeu_pd(out, val);
		}
	};

	template<int rounding_mode>
	struct FloatRoundingComputation<Float64, rounding_mode, NegativeScale>
		: public BaseFloatRoundingComputation<Float64>
	{
		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
			Float64 fscale = static_cast<Float64>(scale);
			mm_scale = _mm_load1_pd(&fscale);
		}

		static inline void compute(const Float64 * in, const Scale & scale, Float64 * out)
		{
			__m128d val = _mm_loadu_pd(in);
			val = _mm_div_pd(val, scale);
			__m128d res = _mm_cmpge_pd(val, getOne());
			val = _mm_round_pd(val, rounding_mode);
			val = _mm_mul_pd(val, scale);
			val = _mm_and_pd(val, res);
			_mm_storeu_pd(out, val);
		}

	private:
		static inline const __m128d & getOne()
		{
			static const __m128d one = _mm_set1_pd(1.0);
			return one;
		}
	};

	template<int rounding_mode>
	struct FloatRoundingComputation<Float64, rounding_mode, ZeroScale>
		: public BaseFloatRoundingComputation<Float64>
	{
		static inline void prepareScale(size_t scale, Scale & mm_scale)
		{
		}

		static inline void compute(const Float64 * in, const Scale & scale, Float64 * out)
		{
			__m128d val = _mm_loadu_pd(in);
			val = _mm_round_pd(val, rounding_mode);
			_mm_storeu_pd(out, val);
		}
	};

	/** Реализация высокоуровневых функций округления.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode, typename Enable = void>
	struct FunctionRoundingImpl
	{
	};

	/** Реализация высокоуровневых функций округления для целочисленных значений.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode>
	struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
		typename std::enable_if<std::is_integral<T>::value && (scale_mode != NullScale)>::type>
	{
	private:
		using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode>;

	public:
		static inline void apply(const PODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
		{
			size_t size = in.size();
			for (size_t i = 0; i < size; ++i)
				out[i] = Op::compute(in[i], scale);
		}

		static inline T apply(T val, size_t scale)
		{
			return Op::compute(val, scale);
		}
	};

	/** Реализация высокоуровневых функций округления для значений с плавающей точкой.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode>
	struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
		typename std::enable_if<std::is_floating_point<T>::value && (scale_mode != NullScale)>::type>
	{
	private:
		using Op = FloatRoundingComputation<T, rounding_mode, scale_mode>;
		using Data = std::array<T, Op::data_count>;
		using Scale = typename Op::Scale;

	public:
		static inline void apply(const PODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
		{
			Scale mm_scale;
			Op::prepareScale(scale, mm_scale);

			const size_t size = in.size();
			const size_t data_count = std::tuple_size<Data>();

			size_t i;
			for (i = 0; i < (size - data_count + 1); i += data_count)
				Op::compute(reinterpret_cast<const T *>(&in[i]), mm_scale, reinterpret_cast<T *>(&out[i]));

			if (i < size)
			{
				Data tmp{0};
				for (size_t j = 0; (j < data_count) && ((i + j) < size); ++j)
					tmp[j] = in[i + j];

				Data res;
				Op::compute(reinterpret_cast<T *>(&tmp), mm_scale, reinterpret_cast<T *>(&res));

				for (size_t j = 0; (j < data_count) && ((i + j) < size); ++j)
					out[i + j] = res[j];
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
				Op::compute(reinterpret_cast<T *>(&tmp), mm_scale, reinterpret_cast<T *>(&res));
				return res[0];
			}
		}
	};

	/** Реализация высокоуровневых функций округления в том случае, когда возвращается нулевое значение.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode>
	struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
		typename std::enable_if<scale_mode == NullScale>::type>
	{
	public:
		static inline void apply(const PODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
		{
			size_t size = in.size();
			for (size_t i = 0; i < size; ++i)
				out[i] = 0;
		}

		static inline T apply(T val, size_t scale)
		{
			return 0;
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

	/** Этот шаблон определяет точность, которую используют функции round/ceil/floor,
	  * затем  преобразовывает её в значение, которое можно использовать в операциях
	  * умножения и деления. Поэтому оно называется масштабом.
	  */
	template<typename T, typename U, typename Enable = void>
	struct ScaleForRightType
	{
	};

	template<typename T, typename U>
	struct ScaleForRightType<T, U,
		typename std::enable_if<
			std::is_floating_point<T>::value
			&& std::is_signed<U>::value>::type>
	{
		static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
		{
			using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
			using ColumnType = ColumnConst<U>;

			const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
			if (precision_col == nullptr)
				return false;

			U val = precision_col->getData();
			if (val < 0)
			{
				if (val < -static_cast<U>(std::numeric_limits<T>::digits10))
				{
					scale_mode = NullScale;
					scale = 1;
				}
				else
				{
					scale_mode = NegativeScale;
					scale = PowersOf10::values[-val];
				}
			}
			else if (val == 0)
			{
				scale_mode = ZeroScale;
				scale = 1;
			}
			else
			{
				scale_mode = PositiveScale;
				if (val > std::numeric_limits<T>::digits10)
					val = static_cast<U>(std::numeric_limits<T>::digits10);
				scale = PowersOf10::values[val];
			}

			return true;
		}
	};

	template<typename T, typename U>
	struct ScaleForRightType<T, U,
		typename std::enable_if<
			std::is_floating_point<T>::value
			&& std::is_unsigned<U>::value>::type>
	{
		static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
		{
			using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
			using ColumnType = ColumnConst<U>;

			const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
			if (precision_col == nullptr)
				return false;

			U val = precision_col->getData();
			if (val == 0)
			{
				scale_mode = ZeroScale;
				scale = 1;
			}
			else
			{
				scale_mode = PositiveScale;
				if (val > static_cast<U>(std::numeric_limits<T>::digits10))
					val = static_cast<U>(std::numeric_limits<T>::digits10);
				scale = PowersOf10::values[val];
			}

			return true;
		}
	};

	template<typename T, typename U>
	struct ScaleForRightType<T, U,
		typename std::enable_if<
			std::is_integral<T>::value
			&& std::is_signed<U>::value>::type>
	{
		static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
		{
			using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
			using ColumnType = ColumnConst<U>;

			const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
			if (precision_col == nullptr)
					return false;

			U val = precision_col->getData();
			if (val < 0)
			{
				if (val < -std::numeric_limits<T>::digits10)
				{
					scale_mode = NullScale;
					scale = 1;
				}
				else
				{
					scale_mode = NegativeScale;
					scale = PowersOf10::values[-val];
				}
			}
			else
			{
				scale_mode = ZeroScale;
				scale = 1;
			}

			return true;
		}
	};

	template<typename T, typename U>
	struct ScaleForRightType<T, U,
		typename std::enable_if<
			std::is_integral<T>::value
			&& std::is_unsigned<U>::value>::type>
	{
		static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
		{
			using ColumnType = ColumnConst<U>;

			const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
			if (precision_col == nullptr)
				return false;

			scale_mode = ZeroScale;
			scale = 1;

			return true;
		}
	};

	/** Превратить параметр точности в масштаб.
	  */
	template<typename T>
	struct ScaleForLeftType
	{
		static inline void apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
		{
			if (!(	ScaleForRightType<T, UInt8>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, UInt16>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, UInt16>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, UInt32>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, UInt64>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, Int8>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, Int16>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, Int32>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, Int64>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, Float32>::apply(column, scale_mode, scale)
				||	ScaleForRightType<T, Float64>::apply(column, scale_mode, scale)))
			{
				throw Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
			}
		}
	};

	/** Главный шаблон применяющий функцию округления к значению или столбцу.
	  */
	template<typename T, int rounding_mode, ScaleMode scale_mode>
	struct Cruncher
	{
		using Op = FunctionRoundingImpl<T, rounding_mode, scale_mode>;

		static inline void apply(Block & block, ColumnVector<T> * col, const ColumnNumbers & arguments, size_t result, size_t scale)
		{
			ColumnVector<T> * col_res = new ColumnVector<T>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<T>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->getData().size());

			Op::apply(col->getData(), scale, vec_res);
		}

		static inline void apply(Block & block, ColumnConst<T> * col, const ColumnNumbers & arguments, size_t result, size_t scale)
		{
			T res = Op::apply(col->getData(), scale);
			ColumnConst<T> * col_res = new ColumnConst<T>(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
	};

	/** Выбрать подходящий алгоритм обработки в зависимости от масштаба.
	  */
	template<typename T, template<typename> class U, int rounding_mode>
	struct Dispatcher
	{
		static inline void apply(Block & block, U<T> * col, const ColumnNumbers & arguments, size_t result)
		{
			ScaleMode scale_mode;
			size_t scale;

			if (arguments.size() == 2)
				ScaleForLeftType<T>::apply(block.getByPosition(arguments[1]).column, scale_mode, scale);
			else
			{
				scale_mode = ZeroScale;
				scale = 1;
			}

			if (scale_mode == PositiveScale)
				Cruncher<T, rounding_mode, PositiveScale>::apply(block, col, arguments, result, scale);
			else if (scale_mode == ZeroScale)
				Cruncher<T, rounding_mode, ZeroScale>::apply(block, col, arguments, result, scale);
			else if (scale_mode == NegativeScale)
				Cruncher<T, rounding_mode, NegativeScale>::apply(block, col, arguments, result, scale);
			else if (scale_mode == NullScale)
				Cruncher<T, rounding_mode, NullScale>::apply(block, col, arguments, result, scale);
			else
				throw Exception("Illegal operation", ErrorCodes::LOGICAL_ERROR);
		}
	};

	/** Шаблон для функций, которые округляют значение входного параметра типа
	  * (U)Int8/16/32/64 или Float32/64, и принимают дополнительный необязятельный
	  * параметр (по умолчанию - 0).
	  */
	template<typename Name, int rounding_mode>
	class FunctionRounding : public IFunction
	{
	public:
		static constexpr auto name = Name::name;
		static IFunction * create(const Context & context) { return new FunctionRounding; }

	private:
		template<typename T>
		bool checkType(const IDataType * type) const
		{
			return typeid_cast<const T *>(type) != nullptr;
		}

		template<typename T>
		bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result)
		{
			if (ColumnVector<T> * col = typeid_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
			{
				Dispatcher<T, ColumnVector, rounding_mode>::apply(block, col, arguments, result);
				return true;
			}
			else if (ColumnConst<T> * col = typeid_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
			{
				Dispatcher<T, ColumnConst, rounding_mode>::apply(block, col, arguments, result);
				return true;
			}
			else
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
					|| checkType<DataTypeInt64>(type)
					|| checkType<DataTypeFloat32>(type)
					|| checkType<DataTypeFloat64>(type)))
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
