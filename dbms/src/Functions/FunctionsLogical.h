#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/FunctionHelpers.h>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Functions are logical links: and, or, not, xor.
  * Accept any numeric types, return a UInt8 containing 0 or 1.
  */

template <typename B>
struct AndImpl
{
    static inline bool isSaturable()
    {
        return true;
    }

    static inline bool isSaturatedValue(UInt8 a)
    {
        return !a;
    }

    static inline UInt8 apply(UInt8 a, B b)
    {
        return a && b;
    }
};

template <typename B>
struct OrImpl
{
    static inline bool isSaturable()
    {
        return true;
    }

    static inline bool isSaturatedValue(UInt8 a)
    {
        return a;
    }

    static inline UInt8 apply(UInt8 a, B b)
    {
        return a || b;
    }
};

template <typename B>
struct XorImpl
{
    static inline bool isSaturable()
    {
        return false;
    }

    static inline bool isSaturatedValue(UInt8)
    {
        return false;
    }

    static inline UInt8 apply(UInt8 a, B b)
    {
        return (!a) != (!b);
    }
};

template <typename A>
struct NotImpl
{
    using ResultType = UInt8;

    static inline UInt8 apply(A a)
    {
        return !a;
    }
};


using UInt8Container = ColumnUInt8::Container;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;


template <typename Op, size_t N>
struct AssociativeOperationImpl
{
    /// Erases the N last columns from `in` (if there are less, then all) and puts into `result` their combination.
    static void NO_INLINE execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        if (N > in.size())
        {
            AssociativeOperationImpl<Op, N - 1>::execute(in, result);
            return;
        }

        AssociativeOperationImpl<Op, N> operation(in);
        in.erase(in.end() - N, in.end());

        size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
        {
            result[i] = operation.apply(i);
        }
    }

    const UInt8Container & vec;
    AssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData()), continuation(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline UInt8 apply(size_t i) const
    {
        if (Op::isSaturable())
        {
            UInt8 a = vec[i];
            return Op::isSaturatedValue(a) ? a : continuation.apply(i);
        }
        else
        {
            return Op::apply(vec[i], continuation.apply(i));
        }
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs &, UInt8Container &)
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
    }

    const UInt8Container & vec;

    AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - 1]->getData()) {}

    inline UInt8 apply(size_t i) const
    {
        return vec[i];
    }
};


template <template <typename> class Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); };

private:
    bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res)
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            if (in[i]->isColumnConst())
            {
                UInt8 x = !!in[i]->getUInt(0);
                if (has_res)
                {
                    res = Impl<UInt8>::apply(res, x);
                }
                else
                {
                    res = x;
                    has_res = true;
                }

                in.erase(in.begin() + i);
            }
        }
        return has_res;
    }

    template <typename T>
    bool convertTypeToUInt8(const IColumn * column, UInt8Container & res)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const typename ColumnVector<T>::Container & vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            res[i] = !!vec[i];

        return true;
    }

    void convertToUInt8(const IColumn * column, UInt8Container & res)
    {
        if (!convertTypeToUInt8<Int8>(column, res) &&
            !convertTypeToUInt8<Int16>(column, res) &&
            !convertTypeToUInt8<Int32>(column, res) &&
            !convertTypeToUInt8<Int64>(column, res) &&
            !convertTypeToUInt8<UInt16>(column, res) &&
            !convertTypeToUInt8<UInt32>(column, res) &&
            !convertTypeToUInt8<UInt64>(column, res) &&
            !convertTypeToUInt8<Float32>(column, res) &&
            !convertTypeToUInt8<Float64>(column, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename T>
    bool executeUInt8Type(const UInt8Container & uint8_vec, const IColumn * column, UInt8Container & res)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const typename ColumnVector<T>::Container & other_vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            res[i] = Impl<T>::apply(uint8_vec[i], other_vec[i]);
        return true;
    }

    void executeUInt8Other(const UInt8Container & uint8_vec, const IColumn * column, UInt8Container & res)
    {
        if (!executeUInt8Type<Int8 >(uint8_vec, column, res) &&
            !executeUInt8Type<Int16>(uint8_vec, column, res) &&
            !executeUInt8Type<Int32>(uint8_vec, column, res) &&
            !executeUInt8Type<Int64>(uint8_vec, column, res) &&
            !executeUInt8Type<UInt16>(uint8_vec, column, res) &&
            !executeUInt8Type<UInt32>(uint8_vec, column, res) &&
            !executeUInt8Type<UInt64>(uint8_vec, column, res) &&
            !executeUInt8Type<Float32>(uint8_vec, column, res) &&
            !executeUInt8Type<Float64>(uint8_vec, column, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < arguments.size(); ++i)
            if (!arguments[i]->isNumber())
                throw Exception("Illegal type ("
                    + arguments[i]->getName()
                    + ") of " + toString(i + 1) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t num_arguments = arguments.size();
        ColumnRawPtrs in(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            in[i] = block.getByPosition(arguments[i]).column.get();

        size_t rows = in[0]->size();

        /// Combine all constant columns into a single value.
        UInt8 const_val = 0;
        bool has_consts = extractConstColumns(in, const_val);

        // If this value uniquely determines the result, return it.
        if (has_consts && (in.empty() || Impl<UInt8>::apply(const_val, 0) == Impl<UInt8>::apply(const_val, 1)))
        {
            if (!in.empty())
                const_val = Impl<UInt8>::apply(const_val, 0);
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(rows, toField(const_val));
            return;
        }

        /// If this value is a neutral element, let's forget about it.
        if (has_consts && Impl<UInt8>::apply(const_val, 0) == 0 && Impl<UInt8>::apply(const_val, 1) == 1)
            has_consts = false;

        auto col_res = ColumnUInt8::create();
        UInt8Container & vec_res = col_res->getData();

        if (has_consts)
        {
            vec_res.assign(rows, const_val);
            in.push_back(col_res.get());
        }
        else
        {
            vec_res.resize(rows);
        }

        /// Divide the input columns into UInt8 and the rest. The first will be processed more efficiently.
        /// col_res at each moment will either be at the end of uint8_in, or not contained in uint8_in.
        UInt8ColumnPtrs uint8_in;
        ColumnRawPtrs other_in;
        for (const IColumn * column : in)
        {
            if (auto uint8_column = typeid_cast<const ColumnUInt8 *>(column))
                uint8_in.push_back(uint8_column);
            else
                other_in.push_back(column);
        }

        /// You need at least one column in uint8_in, so that you can combine columns from other_in.
        if (uint8_in.empty())
        {
            if (other_in.empty())
                throw Exception("Logical error in FunctionAnyArityLogical: other_in is empty", ErrorCodes::LOGICAL_ERROR);

            convertToUInt8(other_in.back(), vec_res);
            other_in.pop_back();
            uint8_in.push_back(col_res.get());
        }

        /// Effeciently combine all the columns of the correct type.
        while (uint8_in.size() > 1)
        {
            /// With a large block size, combining 6 columns per pass is the fastest.
            /// When small - more, is faster.
            AssociativeOperationImpl<Impl<UInt8>, 10>::execute(uint8_in, vec_res);
            uint8_in.push_back(col_res.get());
        }

        /// Add all the columns of the wrong type one at a time.
        while (!other_in.empty())
        {
            executeUInt8Other(uint8_in[0]->getData(), other_in.back(), vec_res);
            other_in.pop_back();
            uint8_in[0] = col_res.get();
        }

        /// This is possible if there is exactly one non-constant among the arguments, and it is of type UInt8.
        if (uint8_in[0] != col_res.get())
            vec_res.assign(uint8_in[0]->getData());

        block.getByPosition(result).column = std::move(col_res);
    }
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryLogical>(); };

private:
    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_res = ColumnUInt8::create();

            typename ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            UnaryOperationImpl<T, Impl<T>>::vector(col->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isNumber())
            throw Exception("Illegal type ("
                + arguments[0]->getName()
                + ") of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!( executeType<UInt8>(block, arguments, result)
            || executeType<UInt16>(block, arguments, result)
            || executeType<UInt32>(block, arguments, result)
            || executeType<UInt64>(block, arguments, result)
            || executeType<Int8>(block, arguments, result)
            || executeType<Int16>(block, arguments, result)
            || executeType<Int32>(block, arguments, result)
            || executeType<Int64>(block, arguments, result)
            || executeType<Float32>(block, arguments, result)
            || executeType<Float64>(block, arguments, result)))
           throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


struct NameAnd { static constexpr auto name = "and"; };
struct NameOr { static constexpr auto name = "or"; };
struct NameXor { static constexpr auto name = "xor"; };
struct NameNot { static constexpr auto name = "not"; };

using FunctionAnd = FunctionAnyArityLogical<AndImpl, NameAnd>;
using FunctionOr = FunctionAnyArityLogical<OrImpl, NameOr>;
using FunctionXor = FunctionAnyArityLogical<XorImpl, NameXor>;
using FunctionNot = FunctionUnaryLogical<NotImpl, NameNot>;

}
