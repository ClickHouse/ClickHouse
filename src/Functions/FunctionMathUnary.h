#pragma once

#include <Core/callOnTypeIndex.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnDecimal.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include "config.h"

/** FastOps is a fast vector math library from Mikhail Parakhin, https://www.linkedin.com/in/mikhail-parakhin/
  * Enabled by default.
  */
#if USE_FASTOPS
#    include <fastops/fastops.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


template <typename Impl>
class FunctionMathUnary final : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMathUnary>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & argument = arguments.front();

        if (!isNumber(argument))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                argument->getName(),
                getName());

        /// Integers are converted to Float64.
        if (Impl::always_returns_float64 || !isFloat(argument))
            return std::make_shared<DataTypeFloat64>();
        return argument;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return Impl::always_returns_float64 ? std::make_shared<DataTypeFloat64>() : nullptr;
    }

    /// Impls whose kernel reads `src` after it has already written `dst` cannot run in place.
    static constexpr bool impl_reads_src_after_writing_dst = []
    {
        if constexpr (requires { Impl::reads_src_after_writing_dst; })
            return Impl::reads_src_after_writing_dst;
        else
            return false;
    }();

    template <typename T, typename ReturnType>
    static void executeInIterations(const T * src_data, ReturnType * dst_data, size_t size)
    {
        if constexpr (Impl::rows_per_iteration == 0)
        {
            /// Process all data as a whole and use FastOps implementation

            /// If the argument is integer, convert to Float64 beforehand
            if constexpr (!is_floating_point<T>)
            {
                PODArray<Float64> tmp_vec(size);
                for (size_t i = 0; i < size; ++i)
                    tmp_vec[i] = static_cast<Float64>(src_data[i]);

                Impl::execute(tmp_vec.data(), size, dst_data);
            }
            else
            {
                Impl::execute(src_data, size, dst_data);
            }
        }
        else
        {
            const size_t rows_remaining = size % Impl::rows_per_iteration;
            const size_t rows_size = size - rows_remaining;

            /// When rows_per_iteration == 1, the loop body is a scalar function
            /// call (e.g. exp2, sin). The compiler may aggressively unroll this
            /// on higher march targets, bloating i-cache for zero throughput
            /// gain since the bottleneck is the call itself.
#pragma clang loop unroll(disable)
            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&src_data[i], &dst_data[i]);

            if (rows_remaining != 0)
            {
                T src_remaining[Impl::rows_per_iteration];
                memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(T));
                memset(src_remaining + rows_remaining, 0, (Impl::rows_per_iteration - rows_remaining) * sizeof(T));
                ReturnType dst_remaining[Impl::rows_per_iteration];

                Impl::execute(src_remaining, dst_remaining);

                if constexpr (is_big_int_v<T> || std::is_same_v<T, Decimal256>)
                    for (size_t i = 0; i < rows_remaining; ++i)
                        dst_data[rows_size + i] = dst_remaining[i];
                else
                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(ReturnType));
            }
        }
    }

    template <typename T, typename ReturnType>
    static ColumnPtr execute(const ColumnVector<T> * col, size_t input_rows_count)
    {
        const auto & src_data = col->getData();

        auto dst = ColumnVector<ReturnType>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        executeInIterations(src_data.data(), dst_data.data(), input_rows_count);

        return dst;
    }

    template <typename T, typename ReturnType>
    static ColumnPtr execute(const ColumnDecimal<T> * col, size_t input_rows_count)
    {
        const auto & src_data = col->getData();
        UInt32 scale = col->getScale();

        auto dst = ColumnVector<ReturnType>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        if constexpr (impl_reads_src_after_writing_dst)
        {
            PODArray<ReturnType> converted(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
                converted[i] = DecimalUtils::convertTo<ReturnType>(src_data[i], scale);

            executeInIterations(converted.data(), dst_data.data(), input_rows_count);
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                dst_data[i] = DecimalUtils::convertTo<ReturnType>(src_data[i], scale);

            executeInIterations(dst_data.data(), dst_data.data(), input_rows_count);
        }

        return dst;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col = arguments[0];
        ColumnPtr res;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ReturnType = std::conditional_t<Impl::always_returns_float64 || !is_floating_point<Type>, Float64, Type>;
            using ColVecType = ColumnVectorOrDecimal<Type>;

            const auto col_vec = checkAndGetColumn<ColVecType>(col.column.get());
            if (col_vec == nullptr)
                return false;
            return (res = execute<Type, ReturnType>(col_vec, input_rows_count)) != nullptr;
        };

        if (!callOnBasicType<void, true, true, true, false>(col.type->getTypeId(), call))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                col.column->getName(),
                getName());

        return res;
    }
};


template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;
    static constexpr bool always_returns_float64 = true;

    template <typename T>
    static void execute(const T * __restrict src, Float64 * __restrict dst)
    {
        *dst = Function(static_cast<Float64>(*src));
    }
};


/// Whole-column vectorized unary math impl over a `void (const double *, size_t, double *)` kernel.
/// Always returns Float64, matching the historical behavior of these functions.
/// `ReadsSrcAfterWritingDst` must be set for kernels that re-read `src` after writing `dst`
/// (e.g. the `libm` fallback in `FastTrig`); it makes the callers keep the source in a buffer
/// separate from the destination instead of running the kernel in place.
template <typename Name, void (*Kernel)(const double *, size_t, double *), bool ReadsSrcAfterWritingDst = false>
struct VectorizedFloat64Impl
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 0;
    static constexpr bool always_returns_float64 = true;
    static constexpr bool reads_src_after_writing_dst = ReadsSrcAfterWritingDst;

    /// No `__restrict`: in-place-capable kernels are called with `src == dst` on the Decimal path.
    template <typename T>
    static void execute(const T * src, size_t size, Float64 * dst)
    {
        if constexpr (std::is_same_v<T, Float64>)
        {
            Kernel(src, size, dst);
        }
        else if constexpr (ReadsSrcAfterWritingDst)
        {
            /// Integer inputs already arrive as Float64, so this only runs for Float32/BFloat16 columns.
            /// The kernel re-reads `src` after writing `dst`, so the promoted values must live in a
            /// buffer separate from `dst`.
            PODArray<Float64> promoted(size);
            for (size_t i = 0; i < size; ++i)
                promoted[i] = static_cast<Float64>(src[i]);
            Kernel(promoted.data(), size, dst);
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = static_cast<Float64>(src[i]);
            Kernel(dst, size, dst);
        }
    }
};

#if USE_FASTOPS

/// log_b(x) = ln(x) / ln(b). `NFastOps::Log` handles all special values (0 -> -inf,
/// negatives -> NaN, +inf -> +inf), which the finite scale factor preserves.
inline void fastNaturalLogScaled(const double * src, size_t size, double * dst, double inv_ln_base)
{
    NFastOps::Log<true>(src, size, dst);
    for (size_t i = 0; i < size; ++i)
        dst[i] *= inv_ln_base;
}

#endif

}
