#pragma once

#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
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

        if (!callOnBasicType<void, true, true, true, false>(col.type->getTypeId(), call))
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

}
