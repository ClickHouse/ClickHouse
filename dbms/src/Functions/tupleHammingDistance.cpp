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

template <typename A, typename B, typename C, typename D>
struct TupleHammingDistanceImpl
{
    using ResultType = UInt8;

    static void NO_INLINE vector_vector(
        const PaddedPODArray<A> & a1,
        const PaddedPODArray<B> & b1,
        const PaddedPODArray<C> & a2,
        const PaddedPODArray<D> & b2,
        PaddedPODArray<ResultType> & c)
    {
        size_t size = a1.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a1[i], a2[i]) + apply(b1[i], b2[i]);
    }

    static void NO_INLINE
    vector_constant(const PaddedPODArray<A> & a1, const PaddedPODArray<B> & b1, C a2, D b2, PaddedPODArray<ResultType> & c)
    {
        size_t size = a1.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a1[i], a2) + apply(b1[i], b2);
    }

    static void NO_INLINE
    constant_vector(A a1, B b1, const PaddedPODArray<C> & a2, const PaddedPODArray<D> & b2, PaddedPODArray<ResultType> & c)
    {
        size_t size = a2.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a1, a2[i]) + apply(b1, b2[i]);
    }

    static ResultType constant_constant(A a1, B b1, C a2, D b2) { return apply(a1, a2) + apply(b1, b2); }

private:
    static UInt8 pop_cnt(UInt64 res)
    {
        UInt8 count = 0;
        for (; res; res >>= 1)
            count += res & 1u;
        return count;
    }

    static inline UInt8 apply(UInt64 a, UInt64 b)
    {
        UInt64 res = a ^ b;
        return pop_cnt(res);
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
static bool castAllTypes(const IDataType * left1, const IDataType * right1, const IDataType * left2, const IDataType * right2, F && f)
{
    return castType(left1, [&](const auto & left1_) {
        return castType(right1, [&](const auto & right1_) {
            return castType(left2, [&](const auto & left2_) {
                return castType(right2, [&](const auto & right2_) { return f(left1_, right1_, left2_, right2_); });
            });
        });
    });
}

class FunctionTupleHammingDistance : public IFunction
{
public:
    static constexpr auto name = "tupleHammingDistance";
    using ResultType = UInt8;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTupleHammingDistance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; };

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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const ColumnWithTypeAndName & arg1 = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & arg2 = block.getByPosition(arguments[1]);
        const DataTypeTuple & type1 = static_cast<const DataTypeTuple &>(*arg1.type);
        const DataTypeTuple & type2 = static_cast<const DataTypeTuple &>(*arg2.type);
        auto & left_elems = type1.getElements();
        auto & right_elems = type2.getElements();
        if (left_elems.size() != 2 || right_elems.size() != 2)
            throw Exception(
                "Illegal column of arguments of function " + getName() + ", tuple should have exactly two elements.",
                ErrorCodes::ILLEGAL_COLUMN);
        bool valid = castAllTypes(
            left_elems[0].get(),
            left_elems[1].get(),
            right_elems[0].get(),
            right_elems[1].get(),
            [&](const auto & left1, const auto & left2, const auto & right1, const auto & right2) {
                using LeftDataType1 = std::decay_t<decltype(left1)>;
                using LeftDataType2 = std::decay_t<decltype(left2)>;
                using RightDataType1 = std::decay_t<decltype(right1)>;
                using RightDataType2 = std::decay_t<decltype(right2)>;
                using T0 = typename LeftDataType1::FieldType;
                using T1 = typename LeftDataType2::FieldType;
                using T2 = typename RightDataType1::FieldType;
                using T3 = typename RightDataType2::FieldType;
                using ColVecResult = ColumnVector<ResultType>;

                using OpImpl = TupleHammingDistanceImpl<T0, T1, T2, T3>;

                // constant tuple - constant tuple
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
                        auto res = OpImpl::constant_constant(a1.get<T0>(), b1.get<T1>(), a2.get<T2>(), b2.get<T3>());
                        block.getByPosition(result).column = DataTypeUInt8().createColumnConst(const_col_left->size(), toField(res));
                        return true;
                    }
                }

                typename ColVecResult::MutablePtr col_res = nullptr;
                col_res = ColVecResult::create();
                auto & vec_res = col_res->getData();
                vec_res.resize(block.rows());
                // constant tuple - vector tuple
                if (const ColumnConst * const_col_left = checkAndGetColumnConst<ColumnTuple>(arg1.column.get()))
                {
                    if (const ColumnTuple * col_right = typeid_cast<const ColumnTuple *>(arg2.column.get()))
                    {
                        auto const_cols = convertConstTupleToConstantElements(*const_col_left);
                        Field a1, b1;
                        const_cols[0]->get(0, a1);
                        const_cols[1]->get(0, b1);
                        auto col_r1 = checkAndGetColumn<ColumnVector<T2>>(&col_right->getColumn(0));
                        auto col_r2 = checkAndGetColumn<ColumnVector<T3>>(&col_right->getColumn(1));
                        if (col_r1 && col_r2)
                            OpImpl::constant_vector(a1.get<T0>(), b1.get<T1>(), col_r1->getData(), col_r2->getData(), vec_res);
                        else
                            return false;
                    }
                    else
                        return false;
                }
                else if (const ColumnTuple * col_left = typeid_cast<const ColumnTuple *>(arg1.column.get()))
                {
                    auto col_l1 = checkAndGetColumn<ColumnVector<T0>>(&col_left->getColumn(0));
                    auto col_l2 = checkAndGetColumn<ColumnVector<T1>>(&col_left->getColumn(1));
                    if (col_l1 && col_l2)
                    {
                        // vector tuple - constant tuple
                        if (const ColumnConst * const_col_right = checkAndGetColumnConst<ColumnTuple>(arg2.column.get()))
                        {
                            auto const_cols = convertConstTupleToConstantElements(*const_col_right);
                            Field a2, b2;
                            const_cols[0]->get(0, a2);
                            const_cols[1]->get(0, b2);
                            OpImpl::vector_constant(col_l1->getData(), col_l2->getData(), a2.get<T2>(), a2.get<T3>(), vec_res);
                        }
                        // vector tuple - vector tuple
                        else if (const ColumnTuple * col_right = typeid_cast<const ColumnTuple *>(arg2.column.get()))
                        {
                            auto col_r1 = checkAndGetColumn<ColumnVector<T2>>(&col_right->getColumn(0));
                            auto col_r2 = checkAndGetColumn<ColumnVector<T3>>(&col_right->getColumn(1));
                            if (col_r1 && col_r2)
                                OpImpl::vector_vector(col_l1->getData(), col_l2->getData(), col_r1->getData(), col_r2->getData(), vec_res);
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
                block.getByPosition(result).column = std::move(col_res);
                return true;
            });
        if (!valid)
            throw Exception(getName() + "'s arguments do not match the expected data types", ErrorCodes::LOGICAL_ERROR);
    }
};

void registerFunctionTupleHammingDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTupleHammingDistance>();
}
}
