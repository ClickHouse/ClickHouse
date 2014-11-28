#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

template <typename Impl>
class FunctionMathNullaryConstFloat64 : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static IFunction * create(const Context &) { return new FunctionMathNullaryConstFloat64; }

private:
	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 0)
			throw Exception{
				"Number of arguments for function " + getName() + "doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		return new DataTypeFloat64;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		block.getByPosition(result).column = new ColumnConst<Float64>{
			block.rowsInFirstColumn(),
			Impl::value
		};
	}
};

struct EImpl
{
	static constexpr auto name = "e";
	static constexpr auto value = 2.7182818284590452353602874713526624977572470;
};

struct PiImpl
{
	static constexpr auto name = "pi";
	static constexpr auto value = 3.1415926535897932384626433832795028841971693;
};

template <typename Impl> class FunctionMathUnaryFloat64 : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static IFunction * create(const Context &) { return new FunctionMathUnaryFloat64; }
	static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

private:
	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception{
				"Number of arguments for function " + getName() + "doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		const auto check_argument_type = [this] (const IDataType * const arg) {
			if (!typeid_cast<const DataTypeUInt8 *>(arg) &&
				!typeid_cast<const DataTypeUInt16 *>(arg) &&
				!typeid_cast<const DataTypeUInt32 *>(arg) &&
				!typeid_cast<const DataTypeUInt64 *>(arg) &&
				!typeid_cast<const DataTypeInt8 *>(arg) &&
				!typeid_cast<const DataTypeInt16 *>(arg) &&
				!typeid_cast<const DataTypeInt32 *>(arg) &&
				!typeid_cast<const DataTypeInt64 *>(arg) &&
				!typeid_cast<const DataTypeFloat32 *>(arg) &&
				!typeid_cast<const DataTypeFloat64 *>(arg))
			{
				throw Exception{
					"Illegal type " + arg->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
				};
			}
		};

		check_argument_type(arguments.front().get());

		return new DataTypeFloat64;
	}

	template <typename FieldType>
	bool execute(Block & block, const IColumn * const arg, const size_t result)
	{
		if (const auto col = typeid_cast<const ColumnVector<FieldType> *>(arg))
		{
			const auto dst = new ColumnVector<Float64>;
			block.getByPosition(result).column = dst;

			const auto & src_data = col->getData();
			const auto src_size = src_data.size();
			auto & dst_data = dst->getData();
			dst_data.resize(src_size);

			const auto rows_remaining = src_size % Impl::rows_per_iteration;
			const auto rows_size = src_size - rows_remaining;

			for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
				Impl::execute(&src_data[i], &dst_data[i]);

			if (rows_remaining != 0)
			{
				FieldType src_remaining[Impl::rows_per_iteration];
				memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(FieldType));
				memset(src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(FieldType));
				Float64 dst_remaining[Impl::rows_per_iteration];
	
				Impl::execute(src_remaining, dst_remaining);

				memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
			}

			return true;
		}
		else if (const auto col = typeid_cast<const ColumnConst<FieldType> *>(arg))
		{
			const FieldType src[Impl::rows_per_iteration] { col->getData() };
			Float64 dst[Impl::rows_per_iteration];

			Impl::execute(src, dst);
			
			block.getByPosition(result).column = new ColumnConst<Float64>{col->size(), dst[0]};

			return true;
		}

		return false;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto arg = block.getByPosition(arguments[0]).column.get();

		if (!execute<UInt8>(block, arg, result) &&
			!execute<UInt16>(block, arg, result) &&
			!execute<UInt32>(block, arg, result) &&
			!execute<UInt64>(block, arg, result) &&
			!execute<Int8>(block, arg, result) &&
			!execute<Int16>(block, arg, result) &&
			!execute<Int32>(block, arg, result) &&
			!execute<Int64>(block, arg, result) &&
			!execute<Float32>(block, arg, result) &&
			!execute<Float64>(block, arg, result))
		{
			throw Exception{
				"Illegal column " + arg->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
		}
	}
};

struct SqrImpl
{
	static constexpr auto name = "sqr";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(src[0] * src[0]);
		dst[1] = static_cast<Float64>(src[1] * src[1]);
	}
};

struct ExpImpl
{
	static constexpr auto name = "exp";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::exp(src[0]));
		dst[1] = static_cast<Float64>(std::exp(src[1]));
	}
};

struct LogImpl
{
	static constexpr auto name = "log";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::log(src[0]));
		dst[1] = static_cast<Float64>(std::log(src[1]));
	}
};

struct Exp2Impl
{
	static constexpr auto name = "exp2";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::exp2(src[0]));
		dst[1] = static_cast<Float64>(std::exp2(src[1]));
	}
};

struct Log2Impl
{
	static constexpr auto name = "log2";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::log2(src[0]));
		dst[1] = static_cast<Float64>(std::log2(src[1]));
	}
};

struct Exp10Impl
{
	static constexpr auto name = "exp10";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::pow(10.0, src[0]));
		dst[1] = static_cast<Float64>(std::pow(10.0, src[1]));
	}
};

struct Log10Impl
{
	static constexpr auto name = "log10";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::log10(src[0]));
		dst[1] = static_cast<Float64>(std::log10(src[1]));
	}
};

struct SqrtImpl
{
	static constexpr auto name = "sqrt";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::sqrt(src[0]));
		dst[1] = static_cast<Float64>(std::sqrt(src[1]));
	}
};

struct CbrtImpl
{
	static constexpr auto name = "cbrt";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::cbrt(src[0]));
		dst[1] = static_cast<Float64>(std::cbrt(src[1]));
	}
};

struct ErfImpl
{
	static constexpr auto name = "erf";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::erf(src[0]));
		dst[1] = static_cast<Float64>(std::erf(src[1]));
	}
};

struct ErfcImpl
{
	static constexpr auto name = "erfc";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::erfc(src[0]));
		dst[1] = static_cast<Float64>(std::erfc(src[1]));
	}
};

struct LGammaImpl
{
	static constexpr auto name = "lgamma";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::lgamma(src[0]));
		dst[1] = static_cast<Float64>(std::tgamma(src[1]));
	}
};

struct TGammaImpl
{
	static constexpr auto name = "tgamma";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::tgamma(src[0]));
		dst[1] = static_cast<Float64>(std::tgamma(src[1]));
	}
};

struct SinImpl
{
	static constexpr auto name = "sin";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::sin(src[0]));
		dst[1] = static_cast<Float64>(std::sin(src[1]));
	}
};

struct CosImpl
{
	static constexpr auto name = "cos";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::cos(src[0]));
		dst[1] = static_cast<Float64>(std::cos(src[1]));
	}
};

struct TanImpl
{
	static constexpr auto name = "tan";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::tan(src[0]));
		dst[1] = static_cast<Float64>(std::tan(src[1]));
	}
};

struct AsinImpl
{
	static constexpr auto name = "asin";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::asin(src[0]));
		dst[1] = static_cast<Float64>(std::asin(src[1]));
	}
};

struct AcosImpl
{
	static constexpr auto name = "acos";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::acos(src[0]));
		dst[1] = static_cast<Float64>(std::acos(src[1]));
	}
};

struct AtanImpl
{
	static constexpr auto name = "atan";
	static constexpr auto rows_per_iteration = 2;

	template <typename T>
	static void execute(const T * const src, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::atan(src[0]));
		dst[1] = static_cast<Float64>(std::atan(src[1]));
	}
};

template <typename Impl> class FunctionMathBinaryFloat64 : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static IFunction * create(const Context &) { return new FunctionMathBinaryFloat64; }
	static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

private:
	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 2)
			throw Exception{
				"Number of arguments for function " + getName() + "doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		const auto check_argument_type = [this] (const IDataType * const arg) {
			if (!typeid_cast<const DataTypeUInt8 *>(arg) &&
				!typeid_cast<const DataTypeUInt16 *>(arg) &&
				!typeid_cast<const DataTypeUInt32 *>(arg) &&
				!typeid_cast<const DataTypeUInt64 *>(arg) &&
				!typeid_cast<const DataTypeInt8 *>(arg) &&
				!typeid_cast<const DataTypeInt16 *>(arg) &&
				!typeid_cast<const DataTypeInt32 *>(arg) &&
				!typeid_cast<const DataTypeInt64 *>(arg) &&
				!typeid_cast<const DataTypeFloat32 *>(arg) &&
				!typeid_cast<const DataTypeFloat64 *>(arg))
			{
				throw Exception{
					"Illegal type " + arg->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
				};
			}
		};

		check_argument_type(arguments.front().get());
		check_argument_type(arguments.back().get());

		return new DataTypeFloat64;
	}

	template <typename LeftType, typename RightType>
	bool executeRight(Block & block, const ColumnNumbers & arguments, const size_t result,
					  const ColumnConst<LeftType> * const left_arg)
	{
		const auto arg = block.getByPosition(arguments[1]).column.get();

		if (const auto right_arg = typeid_cast<const ColumnVector<RightType> *>(arg))
		{
			const auto dst = new ColumnVector<Float64>;
			block.getByPosition(result).column = dst;

			const LeftType left_src_data[Impl::rows_per_iteration] { left_arg->getData() };
			const auto & right_src_data = right_arg->getData();
			const auto src_size = right_src_data.size();
			auto & dst_data = dst->getData();
			dst_data.resize(src_size);

			const auto rows_remaining = src_size % Impl::rows_per_iteration;
			const auto rows_size = src_size - rows_remaining;

			for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
				Impl::execute(left_src_data, &right_src_data[i], &dst_data[i]);

			if (rows_remaining != 0)
			{
				RightType right_src_remaining[Impl::rows_per_iteration];
				memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
				memset(right_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
				Float64 dst_remaining[Impl::rows_per_iteration];
	
				Impl::execute(left_src_data, right_src_remaining, dst_remaining);

				memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
			}

			return true;
		}
		else if (const auto right_arg = typeid_cast<const ColumnConst<RightType> *>(arg))
		{
			const LeftType left_src[Impl::rows_per_iteration] { left_arg->getData() };
			const RightType right_src[Impl::rows_per_iteration] { right_arg->getData() };
			Float64 dst[Impl::rows_per_iteration];

			Impl::execute(left_src, right_src, dst);
			
			block.getByPosition(result).column = new ColumnConst<Float64>{left_arg->size(), dst[0]};

			return true;
		}

		return false;
	}

	template <typename LeftType, typename RightType>
	bool executeRight(Block & block, const ColumnNumbers & arguments, const size_t result,
					  const ColumnVector<LeftType> * const left_arg)
	{
		const auto arg = block.getByPosition(arguments[1]).column.get();

		if (const auto right_arg = typeid_cast<const ColumnVector<RightType> *>(arg))
		{
			const auto dst = new ColumnVector<Float64>;
			block.getByPosition(result).column = dst;

			const auto & left_src_data = left_arg->getData();
			const auto & right_src_data = right_arg->getData();
			const auto src_size = left_src_data.size();
			auto & dst_data = dst->getData();
			dst_data.resize(src_size);

			const auto rows_remaining = src_size % Impl::rows_per_iteration;
			const auto rows_size = src_size - rows_remaining;

			for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
				Impl::execute(&left_src_data[i], &right_src_data[i], &dst_data[i]);

			if (rows_remaining != 0)
			{
				LeftType left_src_remaining[Impl::rows_per_iteration];	
				memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
				memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
				RightType right_src_remaining[Impl::rows_per_iteration];
				memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
				memset(right_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
				Float64 dst_remaining[Impl::rows_per_iteration];
	
				Impl::execute(left_src_remaining, right_src_remaining, dst_remaining);

				memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
			}

			return true;
		}
		else if (const auto right_arg = typeid_cast<const ColumnConst<RightType> *>(arg))
		{
			const auto dst = new ColumnVector<Float64>;
			block.getByPosition(result).column = dst;

			const auto & left_src_data = left_arg->getData();
			const RightType right_src_data[Impl::rows_per_iteration] { right_arg->getData() };
			const auto src_size = left_src_data.size();
			auto & dst_data = dst->getData();
			dst_data.resize(src_size);

			const auto rows_remaining = src_size % Impl::rows_per_iteration;
			const auto rows_size = src_size - rows_remaining;

			for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
				Impl::execute(&left_src_data[i], right_src_data, &dst_data[i]);

			if (rows_remaining != 0)
			{
				LeftType left_src_remaining[Impl::rows_per_iteration];	
				memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
				memset(left_src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
				Float64 dst_remaining[Impl::rows_per_iteration];
	
				Impl::execute(left_src_remaining, right_src_data, dst_remaining);

				memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
			}

			return true;
		}

		return false;
	}

	template <typename LeftType, template <typename> class LeftColumnType>
	bool executeLeftImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		if (const auto arg = typeid_cast<const LeftColumnType<LeftType> *>(
				block.getByPosition(arguments[0]).column.get()))
		{
			if (executeRight<LeftType, UInt8>(block, arguments, result, arg) ||
				executeRight<LeftType, UInt16>(block, arguments, result, arg) ||
				executeRight<LeftType, UInt32>(block, arguments, result, arg) ||
				executeRight<LeftType, UInt64>(block, arguments, result, arg) ||
				executeRight<LeftType, Int8>(block, arguments, result, arg) ||
				executeRight<LeftType, Int16>(block, arguments, result, arg) ||
				executeRight<LeftType, Int32>(block, arguments, result, arg) ||
				executeRight<LeftType, Int64>(block, arguments, result, arg) ||
				executeRight<LeftType, Float32>(block, arguments, result, arg) ||
				executeRight<LeftType, Float64>(block, arguments, result, arg))
			{
				return true;
			}
			else
			{
				throw Exception{
					"Illegal column " + block.getByPosition(arguments[1]).column->getName() +
					" of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN
				};
			}
		}

		return false;
	}

	template <typename LeftType>
	bool executeLeft(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		if (executeLeftImpl<LeftType, ColumnVector>(block, arguments, result) ||
			executeLeftImpl<LeftType, ColumnConst>(block, arguments, result))
			return true;

		return false;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		if (!executeLeft<UInt8>(block, arguments, result) &&
			!executeLeft<UInt16>(block, arguments, result) &&
			!executeLeft<UInt32>(block, arguments, result) &&
			!executeLeft<UInt64>(block, arguments, result) &&
			!executeLeft<Int8>(block, arguments, result) &&
			!executeLeft<Int16>(block, arguments, result) &&
			!executeLeft<Int32>(block, arguments, result) &&
			!executeLeft<Int64>(block, arguments, result) &&
			!executeLeft<Float32>(block, arguments, result) &&
			!executeLeft<Float64>(block, arguments, result))
		{
			throw Exception{
				"Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
		}
	}
};

struct PowImpl
{
	static constexpr auto name = "pow";
	static constexpr auto rows_per_iteration = 2;

	template <typename T1, typename T2>
	static void execute(const T1 * const src_left, const T2 * const src_right, Float64 * const dst)
	{
		dst[0] = static_cast<Float64>(std::pow(src_left[0], src_right[0]));
		dst[1] = static_cast<Float64>(std::pow(src_left[1], src_right[0]));
	}
};

using FunctionE = FunctionMathNullaryConstFloat64<EImpl>;
using FunctionPi = FunctionMathNullaryConstFloat64<PiImpl>;
using FunctionSqr = FunctionMathUnaryFloat64<SqrImpl>;
using FunctionExp = FunctionMathUnaryFloat64<ExpImpl>;
using FunctionLog = FunctionMathUnaryFloat64<LogImpl>;
using FunctionExp2 = FunctionMathUnaryFloat64<Exp2Impl>;
using FunctionLog2 = FunctionMathUnaryFloat64<Log2Impl>;
using FunctionExp10 = FunctionMathUnaryFloat64<Exp10Impl>;
using FunctionLog10 = FunctionMathUnaryFloat64<Log10Impl>;
using FunctionSqrt = FunctionMathUnaryFloat64<SqrtImpl>;
using FunctionCbrt = FunctionMathUnaryFloat64<CbrtImpl>;
using FunctionErf = FunctionMathUnaryFloat64<ErfImpl>;
using FunctionErfc = FunctionMathUnaryFloat64<ErfcImpl>;
using FunctionLGamma = FunctionMathUnaryFloat64<LGammaImpl>;
using FunctionTGamma = FunctionMathUnaryFloat64<TGammaImpl>;
using FunctionSin = FunctionMathUnaryFloat64<SinImpl>;
using FunctionCos = FunctionMathUnaryFloat64<CosImpl>;
using FunctionTan = FunctionMathUnaryFloat64<TanImpl>;
using FunctionAsin = FunctionMathUnaryFloat64<AsinImpl>;
using FunctionAcos = FunctionMathUnaryFloat64<AcosImpl>;
using FunctionAtan = FunctionMathUnaryFloat64<AtanImpl>;
using FunctionPow = FunctionMathBinaryFloat64<PowImpl>;

}
