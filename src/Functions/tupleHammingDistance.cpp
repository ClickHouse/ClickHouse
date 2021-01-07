#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeTuple.h>
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
struct TupleHammingDistanceImpl
{
    using ResultType = UInt8;

    static void NO_INLINE vectorVector(
        const PaddedPODArray<A> & a1,
        const PaddedPODArray<A> & b1,
        const PaddedPODArray<B> & a2,
        const PaddedPODArray<B> & b2,
        PaddedPODArray<ResultType> & c)
    {
        size_t size = a1.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a1[i], a2[i]) + apply(b1[i], b2[i]);
    }

    static void NO_INLINE
    vectorConstant(const PaddedPODArray<A> & a1, const PaddedPODArray<A> & b1, UInt64 a2, UInt64 b2, PaddedPODArray<ResultType> & c)
    {
        size_t size = a1.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a1[i], a2) + apply(b1[i], b2);
    }

    static void NO_INLINE
    constantVector(UInt64 a1, UInt64 b1, const PaddedPODArray<B> & a2, const PaddedPODArray<B> & b2, PaddedPODArray<ResultType> & c)
    {
        size_t size = a2.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a1, a2[i]) + apply(b1, b2[i]);
    }

    static ResultType constantConstant(UInt64 a1, UInt64 b1, UInt64 a2, UInt64 b2) { return apply(a1, a2) + apply(b1, b2); }

private:
    static inline UInt8 apply(UInt64 a, UInt64 b) { return a != b; }
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

// tupleHammingDistance function: (Tuple(Integer, Integer), Tuple(Integer, Integer))->0/1/2
// in order to avoid code bloating, for non-constant tuple, we make sure that the elements
// in the tuple should have same data type, and for constant tuple, elements can be any integer
// data type, we cast all of them into UInt64
class FunctionTupleHammingDistance : public IFunction
{
public:
    static constexpr auto name = "tupleHammingDistance";
    using ResultType = UInt8;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTupleHammingDistance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isTuple(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!isTuple(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & arg1 = arguments[0];
        const ColumnWithTypeAndName & arg2 = arguments[1];
        const DataTypeTuple & type1 = static_cast<const DataTypeTuple &>(*arg1.type);
        const DataTypeTuple & type2 = static_cast<const DataTypeTuple &>(*arg2.type);
        const auto & left_elems = type1.getElements();
        const auto & right_elems = type2.getElements();
        if (left_elems.size() != 2 || right_elems.size() != 2)
            throw Exception(
                "Illegal column of arguments of function " + getName() + ", tuple should have exactly two elements.",
                ErrorCodes::ILLEGAL_COLUMN);

        ColumnPtr result_column;

        bool valid = castBothTypes(left_elems[0].get(), right_elems[0].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ColVecT0 = ColumnVector<T0>;
            using ColVecT1 = ColumnVector<T1>;
            using ColVecResult = ColumnVector<ResultType>;

            using OpImpl = TupleHammingDistanceImpl<T0, T1>;

            // we can not useDefaultImplementationForConstants,
            // because with that, tupleHammingDistance((10, 300), (10, 20)) does not work,
            // since 10 has data type UInt8, and 300 has data type UInt16
            if (const ColumnConst * const_col_left = checkAndGetColumnConst<ColumnTuple>(arg1.column.get()))
            {
                if (const ColumnConst * const_col_right = checkAndGetColumnConst<ColumnTuple>(arg2.column.get()))
                {
                    auto cols1 = convertConstTupleToConstantElements(*const_col_left);
                    auto cols2 = convertConstTupleToConstantElements(*const_col_right);
                    Field a1, b1, a2, b2;
                    cols1[0]->get(0, a1);
                    cols1[1]->get(0, b1);
                    cols2[0]->get(0, a2);
                    cols2[1]->get(0, b2);
                    auto res = OpImpl::constantConstant(a1.get<UInt64>(), b1.get<UInt64>(), a2.get<UInt64>(), b2.get<UInt64>());
                    result_column = DataTypeUInt8().createColumnConst(const_col_left->size(), toField(res));
                    return true;
                }
            }

            typename ColVecResult::MutablePtr col_res = nullptr;
            col_res = ColVecResult::create();
            auto & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);
            // constant tuple - non-constant tuple
            if (const ColumnConst * const_col_left = checkAndGetColumnConst<ColumnTuple>(arg1.column.get()))
            {
                if (const ColumnTuple * col_right = typeid_cast<const ColumnTuple *>(arg2.column.get()))
                {
                    auto const_cols = convertConstTupleToConstantElements(*const_col_left);
                    Field a1, b1;
                    const_cols[0]->get(0, a1);
                    const_cols[1]->get(0, b1);
                    auto col_r1 = checkAndGetColumn<ColVecT1>(&col_right->getColumn(0));
                    auto col_r2 = checkAndGetColumn<ColVecT1>(&col_right->getColumn(1));
                    if (col_r1 && col_r2)
                        OpImpl::constantVector(a1.get<UInt64>(), b1.get<UInt64>(), col_r1->getData(), col_r2->getData(), vec_res);
                    else
                        return false;
                }
                else
                    return false;
            }
            else if (const ColumnTuple * col_left = typeid_cast<const ColumnTuple *>(arg1.column.get()))
            {
                auto col_l1 = checkAndGetColumn<ColVecT0>(&col_left->getColumn(0));
                auto col_l2 = checkAndGetColumn<ColVecT0>(&col_left->getColumn(1));
                if (col_l1 && col_l2)
                {
                    // non-constant tuple - constant tuple
                    if (const ColumnConst * const_col_right = checkAndGetColumnConst<ColumnTuple>(arg2.column.get()))
                    {
                        auto const_cols = convertConstTupleToConstantElements(*const_col_right);
                        Field a2, b2;
                        const_cols[0]->get(0, a2);
                        const_cols[1]->get(0, b2);
                        OpImpl::vectorConstant(col_l1->getData(), col_l2->getData(), a2.get<UInt64>(), a2.get<UInt64>(), vec_res);
                    }
                    // non-constant tuple - non-constant tuple
                    else if (const ColumnTuple * col_right = typeid_cast<const ColumnTuple *>(arg2.column.get()))
                    {
                        auto col_r1 = checkAndGetColumn<ColVecT1>(&col_right->getColumn(0));
                        auto col_r2 = checkAndGetColumn<ColVecT1>(&col_right->getColumn(1));
                        if (col_r1 && col_r2)
                            OpImpl::vectorVector(col_l1->getData(), col_l2->getData(), col_r1->getData(), col_r2->getData(), vec_res);
                        else
                            return false;
                    }
                    else
                        return false;
                }
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

void registerFunctionTupleHammingDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTupleHammingDistance>();
}
}
