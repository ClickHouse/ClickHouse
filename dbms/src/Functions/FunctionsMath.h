#pragma once

#include <common/preciseExp10.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Common/config.h>

/** More efficient implementations of mathematical functions are possible when using a separate library.
  * Disabled due to licence compatibility limitations.
  * To enable: download http://www.agner.org/optimize/vectorclass.zip and unpack to contrib/vectorclass
  * Then rebuild with -DENABLE_VECTORCLASS=1
  */

#if USE_VECTORCLASS
    #ifdef __clang__
            #pragma clang diagnostic push
            #pragma clang diagnostic ignored "-Wshift-negative-value"
    #endif

    #include <vectorf128.h> // Y_IGNORE
    #include <vectormath_exp.h> // Y_IGNORE
    #include <vectormath_trig.h> // Y_IGNORE

    #ifdef __clang__
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

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, Impl::value);
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
        const auto & arg = arguments.front();
        if (!isNumber(arg) && !isDecimal(arg))
            throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename T>
    static void executeInIterations(const T * src_data, Float64 * dst_data, size_t size)
    {
        const size_t rows_remaining = size % Impl::rows_per_iteration;
        const size_t rows_size = size - rows_remaining;

        for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
            Impl::execute(&src_data[i], &dst_data[i]);

        if (rows_remaining != 0)
        {
            T src_remaining[Impl::rows_per_iteration];
            memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(T));
            memset(src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(T));
            Float64 dst_remaining[Impl::rows_per_iteration];

            Impl::execute(src_remaining, dst_remaining);

            memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
        }
    }

    template <typename T>
    static bool execute(Block & block, const ColumnVector<T> * col, const size_t result)
    {
        const auto & src_data = col->getData();
        const size_t size = src_data.size();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(size);

        executeInIterations(src_data.data(), dst_data.data(), size);

        block.getByPosition(result).column = std::move(dst);
        return true;
    }

    template <typename T>
    static bool execute(Block & block, const ColumnDecimal<T> * col, const size_t result)
    {
        const auto & src_data = col->getData();
        const size_t size = src_data.size();
        UInt32 scale = src_data.getScale();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(size);

        for (size_t i = 0; i < size; ++i)
            dst_data[i] = convertFromDecimal<DataTypeDecimal<T>, DataTypeNumber<Float64>>(src_data[i], scale);

        executeInIterations(dst_data.data(), dst_data.data(), size);

        block.getByPosition(result).column = std::move(dst);
        return true;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnWithTypeAndName & col = block.getByPosition(arguments[0]);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>, ColumnVector<Type>>;

            const auto col_vec = checkAndGetColumn<ColVecType>(col.column.get());
            return execute<Type>(block, col_vec, result);
        };

        if (!callOnBasicType<void, true, true, true>(col.type->getTypeId(), call))
            throw Exception{"Illegal column " + col.column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
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
            if (!isNumber(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());

        return std::make_shared<DataTypeFloat64>();
    }

    template <typename LeftType, typename RightType>
    bool executeTyped(Block & block, const size_t result, const ColumnConst * left_arg, const IColumn * right_arg)
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
    bool executeTyped(Block & block, const size_t result, const ColumnVector<LeftType> * left_arg, const IColumn * right_arg)
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnWithTypeAndName & col_left = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & col_right = block.getByPosition(arguments[1]);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftType = typename Types::LeftType;
            using RightType = typename Types::RightType;
            using ColVecLeft = ColumnVector<LeftType>;

            const IColumn * left_arg = col_left.column.get();
            const IColumn * right_arg = col_right.column.get();

            if (const auto left_arg_typed = checkAndGetColumn<ColVecLeft>(left_arg))
            {
                if (executeTyped<LeftType, RightType>(block, result, left_arg_typed, right_arg))
                    return true;

                throw Exception{"Illegal column " + right_arg->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }
            else if (const auto left_arg_typed = checkAndGetColumnConst<ColVecLeft>(left_arg))
            {
                if (executeTyped<LeftType, RightType>(block, result, left_arg_typed, right_arg))
                    return true;

                throw Exception{"Illegal column " + right_arg->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }

            return false;
        };

        TypeIndex left_index = col_left.type->getTypeId();
        TypeIndex right_index = col_right.type->getTypeId();

        if (!callOnBasicTypes<true, true, false>(left_index, right_index, call))
            throw Exception{"Illegal column " + col_left.column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
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
