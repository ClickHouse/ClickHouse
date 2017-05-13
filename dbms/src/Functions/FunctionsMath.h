#pragma once

#include <common/exp10.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Common/config.h>

/** More effective implementations of mathematical functions are possible when connecting a separate library
  * Disabled due licence compatibility limitations
  * To enable: download http://www.agner.org/optimize/vectorclass.zip and unpack to contrib/vectorclass
  *  Then rebuild with -DENABLE_VECTORCLASS=1
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

template <typename Impl>
class FunctionMathNullaryConstFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathNullaryConstFloat64>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        block.safeGetByPosition(result).column = std::make_shared<ColumnConst<Float64>>(
            block.rows(),
            Impl::value);
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

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename FieldType>
    bool execute(Block & block, const IColumn * const arg, const size_t result)
    {
        if (const auto col = typeid_cast<const ColumnVector<FieldType> *>(arg))
        {
            const auto dst = std::make_shared<ColumnVector<Float64>>();
            block.safeGetByPosition(result).column = dst;

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

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<Float64>>(col->size(), dst[0]);

            return true;
        }

        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto arg = block.safeGetByPosition(arguments[0]).column.get();

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


template <typename Name, Float64(&Function)(Float64)>
struct UnaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T>
    static void execute(const T * const src, Float64 * const dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src[0])));
    }
};

#if USE_VECTORCLASS

template <typename Name, Vec2d(&Function)(const Vec2d &)>
struct UnaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 2;

    template <typename T>
    static void execute(const T * const src, Float64 * const dst)
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

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
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
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename LeftType, typename RightType>
    bool executeRight(Block & block, const size_t result, const ColumnConst<LeftType> * const left_arg,
        const IColumn * const right_arg)
    {
        if (const auto right_arg_typed = typeid_cast<const ColumnVector<RightType> *>(right_arg))
        {
            const auto dst = std::make_shared<ColumnVector<Float64>>();
            block.safeGetByPosition(result).column = dst;

            LeftType left_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(left_src_data), std::end(left_src_data), left_arg->getData());
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

            return true;
        }
        else if (const auto right_arg_typed = typeid_cast<const ColumnConst<RightType> *>(right_arg))
        {
            const LeftType left_src[Impl::rows_per_iteration] { left_arg->getData() };
            const RightType right_src[Impl::rows_per_iteration] { right_arg_typed->getData() };
            Float64 dst[Impl::rows_per_iteration];

            Impl::execute(left_src, right_src, dst);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<Float64>>(left_arg->size(), dst[0]);

            return true;
        }

        return false;
    }

    template <typename LeftType, typename RightType>
    bool executeRight(Block & block, const size_t result, const ColumnVector<LeftType> * const left_arg,
        const IColumn * const right_arg)
    {
        if (const auto right_arg_typed = typeid_cast<const ColumnVector<RightType> *>(right_arg))
        {
            const auto dst = std::make_shared<ColumnVector<Float64>>();
            block.safeGetByPosition(result).column = dst;

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

            return true;
        }
        else if (const auto right_arg_typed = typeid_cast<const ColumnConst<RightType> *>(right_arg))
        {
            const auto dst = std::make_shared<ColumnVector<Float64>>();
            block.safeGetByPosition(result).column = dst;

            const auto & left_src_data = left_arg->getData();
            RightType right_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(right_src_data), std::end(right_src_data), right_arg_typed->getData());
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
    bool executeLeftImpl(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IColumn * const left_arg)
    {
        if (const auto left_arg_typed = typeid_cast<const LeftColumnType<LeftType> *>(left_arg))
        {
            const auto right_arg = block.safeGetByPosition(arguments[1]).column.get();

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
                    "Illegal column " + block.safeGetByPosition(arguments[1]).column->getName() +
                    " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN
                };
            }
        }

        return false;
    }

    template <typename LeftType>
    bool executeLeft(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IColumn * const left_arg)
    {
        if (executeLeftImpl<LeftType, ColumnVector>(block, arguments, result, left_arg) ||
            executeLeftImpl<LeftType, ColumnConst>(block, arguments, result, left_arg))
            return true;

        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto left_arg = block.safeGetByPosition(arguments[0]).column.get();

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
                ErrorCodes::ILLEGAL_COLUMN
            };
        }
    }
};


template <typename Name, Float64(&Function)(Float64, Float64)>
struct BinaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * const src_left, const T2 * const src_right, Float64 * const dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src_left[0]), static_cast<Float64>(src_right[0])));
    }
};

#if USE_VECTORCLASS

template <typename Name, Vec2d(&Function)(const Vec2d &, const Vec2d &)>
struct BinaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 2;

    template <typename T1, typename T2>
    static void execute(const T1 * const src_left, const T2 * const src_right, Float64 * const dst)
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
using FunctionExp10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Exp10Name, exp10>>;
using FunctionLog10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Log10Name, log10>>;
using FunctionSqrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<SqrtName, sqrt>>;

using FunctionCbrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<CbrtName,
#if USE_VECTORCLASS
    Power_rational<1, 3>::pow
#else
    cbrt
#endif
>>;

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
