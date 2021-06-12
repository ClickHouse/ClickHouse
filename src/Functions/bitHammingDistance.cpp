#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename A, typename B>
struct BitHammingDistanceImpl
{
    using ResultType = UInt8;

    static void NO_INLINE vectorVector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b[i]);
    }

    static void NO_INLINE vectorConstant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b);
    }

    static void NO_INLINE constantVector(A a, const PaddedPODArray<B> & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a, b[i]);
    }

private:
    static inline UInt8 apply(UInt64 a, UInt64 b)
    {
        UInt64 res = a ^ b;
        return __builtin_popcountll(res);
    }
};

template <typename F>
bool castType(const IDataType * type, F && f)
{
    return castTypeToEither<
        DataTypeInt8,
        DataTypeInt16,
        DataTypeInt32,
        DataTypeInt64,
        DataTypeUInt8,
        DataTypeUInt16,
        DataTypeUInt32,
        DataTypeUInt64>(type, std::forward<F>(f));
}

template <typename F>
static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
{
    return castType(left, [&](const auto & left_) { return castType(right, [&](const auto & right_) { return f(left_, right_); }); });
}

// bitHammingDistance function: (Integer, Integer) -> UInt8
class FunctionBitHammingDistance : public IFunction
{
public:
    static constexpr auto name = "bitHammingDistance";
    using ResultType = UInt8;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitHammingDistance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isInteger(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!isInteger(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * left_generic = arguments[0].type.get();
        const auto * right_generic = arguments[1].type.get();
        ColumnPtr result_column;
        bool valid = castBothTypes(left_generic, right_generic, [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ColVecT0 = ColumnVector<T0>;
            using ColVecT1 = ColumnVector<T1>;
            using ColVecResult = ColumnVector<ResultType>;

            using OpImpl = BitHammingDistanceImpl<T0, T1>;

            const auto * const col_left_raw = arguments[0].column.get();
            const auto * const col_right_raw = arguments[1].column.get();

            typename ColVecResult::MutablePtr col_res = nullptr;
            col_res = ColVecResult::create();

            auto & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            if (auto col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw))
            {
                if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                {
                    // constant integer - non-constant integer
                    OpImpl::constantVector(col_left_const->template getValue<T0>(), col_right->getData(), vec_res);
                }
                else
                    return false;
            }
            else if (auto col_left = checkAndGetColumn<ColVecT0>(col_left_raw))
            {
                if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                    // non-constant integer - non-constant integer
                    OpImpl::vectorVector(col_left->getData(), col_right->getData(), vec_res);
                else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                    // non-constant integer - constant integer
                    OpImpl::vectorConstant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res);
                else
                    return false;
            }
            else
                return false;

            result_column = std::move(col_res);
            return true;
        });
        if (!valid)
            throw Exception(getName() + "'s arguments do not match the expected data types", ErrorCodes::ILLEGAL_COLUMN);

        return result_column;
    }
};

void registerFunctionBitHammingDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitHammingDistance>();
}
}
