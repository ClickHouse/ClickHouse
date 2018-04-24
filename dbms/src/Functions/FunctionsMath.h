#pragma once

#include <common/preciseExp10.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Common/config.h>
#include <Common/typeid_cast.h>

/** More efficient implementations of mathematical functions are possible when using a separate library.
  * Disabled due to licence compatibility limitations.
  * To enable: download http://www.agner.org/optimize/vectorclass.zip and unpack to contrib/vectorclass
  * Then rebuild with -DENABLE_VECTORCLASS=1
  */

#if USE_VECTORCLASS
       #if __clang__
               #pragma clang diagnostic push
               #pragma clang diagnostic ignored "-Wshift-negative-value"
       #endif

       #include <vectorf128.h>
       #include <vectormath_exp.h>
       #include <vectormath_trig.h>

       #if __clang__
               #pragma clang diagnostic pop
       #endif
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

template <typename Impl>
class FunctionMathNullaryConstFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathNullaryConstFloat64>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, const size_t result) override
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Impl::value);
    }
};


template <typename Impl>
class FunctionMathUnaryFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathUnaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.front()->isNumber())
            throw Exception{
                "Illegal type " + arguments.front()->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename FieldType>
    bool execute(Block & block, const IColumn * arg, const size_t result)
    {
        if (const auto col = checkAndGetColumn<ColumnVector<FieldType>>(arg))
        {
            auto dst = ColumnVector<Float64>::create();

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

            block.getByPosition(result).column = std::move(dst);
            return true;
        }

        return false;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
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
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};


template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T>
    static void execute(const T * src, Float64 * dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src[0])));
    }
};

#if USE_VECTORCLASS

template <typename Name, Vec2d(Function)(const Vec2d &)>
struct UnaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 2;

    template <typename T>
    static void execute(const T * src, Float64 * dst)
    {
        const auto result = Function(Vec2d(src[0], src[1]));
        result.store(dst);
    }
};

#else

#define UnaryFunctionVectorized UnaryFunctionPlain

#endif


template <typename Impl>
class FunctionMathBinaryFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathBinaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto check_argument_type = [this] (const IDataType * arg)
        {
            if (!arg->isNumber())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename LeftType, typename RightType>
    bool executeRight(Block & block, const size_t result, const ColumnConst * left_arg,
        const IColumn * right_arg)
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            LeftType left_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(left_src_data), std::end(left_src_data), left_arg->template getValue<LeftType>());
            const auto & right_src_data = right_arg_typed->getData();
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

            block.getByPosition(result).column = std::move(dst);
            return true;
        }

        return false;
    }

    template <typename LeftType, typename RightType>
    bool executeRight(Block & block, const size_t result, const ColumnVector<LeftType> * left_arg,
        const IColumn * right_arg)
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            const auto & right_src_data = right_arg_typed->getData();
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

            block.getByPosition(result).column = std::move(dst);
            return true;
        }
        else if (const auto right_arg_typed = checkAndGetColumnConst<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            RightType right_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(right_src_data), std::end(right_src_data), right_arg_typed->template getValue<RightType>());
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

            block.getByPosition(result).column = std::move(dst);
            return true;
        }

        return false;
    }

    template <typename LeftType>
    bool executeLeft(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IColumn * left_arg)
    {
        if (const auto left_arg_typed = checkAndGetColumn<ColumnVector<LeftType>>(left_arg))
        {
            const auto right_arg = block.getByPosition(arguments[1]).column.get();

            if (executeRight<LeftType, UInt8>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, UInt16>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, UInt32>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, UInt64>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int8>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int16>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int32>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int64>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Float32>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Float64>(block, result, left_arg_typed, right_arg))
            {
                return true;
            }
            else
            {
                throw Exception{
                    "Illegal column " + block.getByPosition(arguments[1]).column->getName() +
                    " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }
        }
        else if (const auto left_arg_typed = checkAndGetColumnConst<ColumnVector<LeftType>>(left_arg))
        {
            const auto right_arg = block.getByPosition(arguments[1]).column.get();

            if (executeRight<LeftType, UInt8>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, UInt16>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, UInt32>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, UInt64>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int8>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int16>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int32>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Int64>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Float32>(block, result, left_arg_typed, right_arg) ||
                executeRight<LeftType, Float64>(block, result, left_arg_typed, right_arg))
            {
                return true;
            }
            else
            {
                throw Exception{
                    "Illegal column " + block.getByPosition(arguments[1]).column->getName() +
                    " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }
        }

        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto left_arg = block.getByPosition(arguments[0]).column.get();

        if (!executeLeft<UInt8>(block, arguments, result, left_arg) &&
            !executeLeft<UInt16>(block, arguments, result, left_arg) &&
            !executeLeft<UInt32>(block, arguments, result, left_arg) &&
            !executeLeft<UInt64>(block, arguments, result, left_arg) &&
            !executeLeft<Int8>(block, arguments, result, left_arg) &&
            !executeLeft<Int16>(block, arguments, result, left_arg) &&
            !executeLeft<Int32>(block, arguments, result, left_arg) &&
            !executeLeft<Int64>(block, arguments, result, left_arg) &&
            !executeLeft<Float32>(block, arguments, result, left_arg) &&
            !executeLeft<Float64>(block, arguments, result, left_arg))
        {
            throw Exception{
                "Illegal column " + left_arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};


template <typename Name, Float64(Function)(Float64, Float64)>
struct BinaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src_left[0]), static_cast<Float64>(src_right[0])));
    }
};

#if USE_VECTORCLASS

template <typename Name, Vec2d(Function)(const Vec2d &, const Vec2d &)>
struct BinaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 2;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        const auto result = Function(Vec2d(src_left[0], src_left[1]), Vec2d(src_right[0], src_right[1]));
        result.store(dst);
    }
};

#else

#define BinaryFunctionVectorized BinaryFunctionPlain

#endif


struct EImpl
{
    static constexpr auto name = "e";
    static const double value;    /// See .cpp
};

struct PiImpl
{
    static constexpr auto name = "pi";
    static const double value;
};

struct ExpName { static constexpr auto name = "exp"; };
struct LogName { static constexpr auto name = "log"; };
struct Exp2Name { static constexpr auto name = "exp2"; };
struct Log2Name { static constexpr auto name = "log2"; };
struct Exp10Name { static constexpr auto name = "exp10"; };
struct Log10Name { static constexpr auto name = "log10"; };
struct SqrtName { static constexpr auto name = "sqrt"; };
struct CbrtName { static constexpr auto name = "cbrt"; };
struct SinName { static constexpr auto name = "sin"; };
struct CosName { static constexpr auto name = "cos"; };
struct TanName { static constexpr auto name = "tan"; };
struct AsinName { static constexpr auto name = "asin"; };
struct AcosName { static constexpr auto name = "acos"; };
struct AtanName { static constexpr auto name = "atan"; };
struct ErfName { static constexpr auto name = "erf"; };
struct ErfcName { static constexpr auto name = "erfc"; };
struct LGammaName { static constexpr auto name = "lgamma"; };
struct TGammaName { static constexpr auto name = "tgamma"; };
struct PowName { static constexpr auto name = "pow"; };

using FunctionE = FunctionMathNullaryConstFloat64<EImpl>;
using FunctionPi = FunctionMathNullaryConstFloat64<PiImpl>;
using FunctionExp = FunctionMathUnaryFloat64<UnaryFunctionVectorized<ExpName, exp>>;
using FunctionLog = FunctionMathUnaryFloat64<UnaryFunctionVectorized<LogName, log>>;
using FunctionExp2 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Exp2Name, exp2>>;
using FunctionLog2 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Log2Name, log2>>;
using FunctionExp10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Exp10Name, 
#if USE_VECTORCLASS
    exp10
#else
    preciseExp10
#endif
>>;

using FunctionLog10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Log10Name, log10>>;
using FunctionSqrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<SqrtName, sqrt>>;
using FunctionCbrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<CbrtName, cbrt>>;
using FunctionSin = FunctionMathUnaryFloat64<UnaryFunctionVectorized<SinName, sin>>;
using FunctionCos = FunctionMathUnaryFloat64<UnaryFunctionVectorized<CosName, cos>>;
using FunctionTan = FunctionMathUnaryFloat64<UnaryFunctionVectorized<TanName, tan>>;
using FunctionAsin = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AsinName, asin>>;
using FunctionAcos = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AcosName, acos>>;
using FunctionAtan = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AtanName, atan>>;
using FunctionErf = FunctionMathUnaryFloat64<UnaryFunctionPlain<ErfName, std::erf>>;
using FunctionErfc = FunctionMathUnaryFloat64<UnaryFunctionPlain<ErfcName, std::erfc>>;
using FunctionLGamma = FunctionMathUnaryFloat64<UnaryFunctionPlain<LGammaName, std::lgamma>>;
using FunctionTGamma = FunctionMathUnaryFloat64<UnaryFunctionPlain<TGammaName, std::tgamma>>;
using FunctionPow = FunctionMathBinaryFloat64<BinaryFunctionVectorized<PowName, pow>>;

}
