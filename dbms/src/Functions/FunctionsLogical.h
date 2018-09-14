#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Common/FieldVisitors.h>
#include <type_traits>


#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h> // Y_IGNORE
#pragma GCC diagnostic pop
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

/** Behaviour in presence of NULLs:
  *
  * Functions AND, XOR, NOT use default implementation for NULLs:
  * - if one of arguments is Nullable, they return Nullable result where NULLs are returned when at least one argument was NULL.
  *
  * But function OR is different.
  * It always return non-Nullable result and NULL are equivalent to 0 (false).
  * For example, 1 OR NULL returns 1, not NULL.
  */

struct AndImpl
{
    static inline constexpr bool isSaturable()
    {
        return true;
    }

    static inline constexpr bool isSaturatedValue(bool a)
    {
        return !a;
    }

    static inline constexpr bool apply(bool a, bool b)
    {
        return a && b;
    }

    static inline constexpr bool specialImplementationForNulls() { return false; }
};

struct OrImpl
{
    static inline constexpr bool isSaturable()
    {
        return true;
    }

    static inline constexpr bool isSaturatedValue(bool a)
    {
        return a;
    }

    static inline constexpr bool apply(bool a, bool b)
    {
        return a || b;
    }

    static inline constexpr bool specialImplementationForNulls() { return true; }
};

struct XorImpl
{
    static inline constexpr bool isSaturable()
    {
        return false;
    }

    static inline constexpr bool isSaturatedValue(bool)
    {
        return false;
    }

    static inline constexpr bool apply(bool a, bool b)
    {
        return a != b;
    }

    static inline constexpr bool specialImplementationForNulls() { return false; }

#if USE_EMBEDDED_COMPILER
    static inline llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a, llvm::Value * b)
    {
        return builder.CreateXor(a, b);
    }
#endif
};

template <typename A>
struct NotImpl
{
    using ResultType = UInt8;

    static inline UInt8 apply(A a)
    {
        return !a;
    }

#if USE_EMBEDDED_COMPILER
    static inline llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a)
    {
        return builder.CreateNot(a);
    }
#endif
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


template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); }

private:
    bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res)
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            if (!in[i]->isColumnConst())
                continue;

            Field value = (*in[i])[0];

            UInt8 x = !value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
            if (has_res)
            {
                res = Impl::apply(res, x);
            }
            else
            {
                res = x;
                has_res = true;
            }

            in.erase(in.begin() + i);
        }
        return has_res;
    }

    template <typename T>
    bool convertTypeToUInt8(const IColumn * column, UInt8Container & res)
    {
        auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;
        const auto & vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            res[i] = !!vec[i];

        return true;
    }

    template <typename T>
    bool convertNullableTypeToUInt8(const IColumn * column, UInt8Container & res)
    {
        auto col_nullable = checkAndGetColumn<ColumnNullable>(column);

        auto col = checkAndGetColumn<ColumnVector<T>>(&col_nullable->getNestedColumn());
        if (!col)
            return false;

        const auto & vec = col->getData();
        const auto & null_map = col_nullable->getNullMapData();

        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
            res[i] = !!vec[i] && !null_map[i];

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
            !convertTypeToUInt8<Float64>(column, res) &&
            !convertNullableTypeToUInt8<Int8>(column, res) &&
            !convertNullableTypeToUInt8<Int16>(column, res) &&
            !convertNullableTypeToUInt8<Int32>(column, res) &&
            !convertNullableTypeToUInt8<Int64>(column, res) &&
            !convertNullableTypeToUInt8<UInt8>(column, res) &&
            !convertNullableTypeToUInt8<UInt16>(column, res) &&
            !convertNullableTypeToUInt8<UInt32>(column, res) &&
            !convertNullableTypeToUInt8<UInt64>(column, res) &&
            !convertNullableTypeToUInt8<Float32>(column, res) &&
            !convertNullableTypeToUInt8<Float64>(column, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return !Impl::specialImplementationForNulls(); }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < arguments.size(); ++i)
            if (!(isNumber(arguments[i])
                || (Impl::specialImplementationForNulls() && (arguments[i]->onlyNull() || isNumber(removeNullable(arguments[i]))))))
                throw Exception("Illegal type ("
                    + arguments[i]->getName()
                    + ") of " + toString(i + 1) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
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
        if (has_consts && (in.empty() || Impl::apply(const_val, 0) == Impl::apply(const_val, 1)))
        {
            if (!in.empty())
                const_val = Impl::apply(const_val, 0);
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(rows, toField(const_val));
            return;
        }

        /// If this value is a neutral element, let's forget about it.
        if (has_consts && Impl::apply(const_val, 0) == 0 && Impl::apply(const_val, 1) == 1)
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

        /// Convert all columns to UInt8
        UInt8ColumnPtrs uint8_in;
        Columns converted_columns;

        for (const IColumn * column : in)
        {
            if (auto uint8_column = checkAndGetColumn<ColumnUInt8>(column))
                uint8_in.push_back(uint8_column);
            else
            {
                auto converted_column = ColumnUInt8::create(rows);
                convertToUInt8(column, converted_column->getData());
                uint8_in.push_back(converted_column.get());
                converted_columns.emplace_back(std::move(converted_column));
            }
        }

        /// Effeciently combine all the columns of the correct type.
        while (uint8_in.size() > 1)
        {
            /// With a large block size, combining 6 columns per pass is the fastest.
            /// When small - more, is faster.
            AssociativeOperationImpl<Impl, 10>::execute(uint8_in, vec_res);
            uint8_in.push_back(col_res.get());
        }

        /// This is possible if there is exactly one non-constant among the arguments, and it is of type UInt8.
        if (uint8_in[0] != col_res.get())
            vec_res.assign(uint8_in[0]->getData());

        block.getByPosition(result).column = std::move(col_res);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &) const override { return true; }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        if constexpr (!Impl::isSaturable())
        {
            auto * result = nativeBoolCast(b, types[0], values[0]());
            for (size_t i = 1; i < types.size(); i++)
                result = Impl::apply(b, result, nativeBoolCast(b, types[i], values[i]()));
            return b.CreateSelect(result, b.getInt8(1), b.getInt8(0));
        }
        constexpr bool breakOnTrue = Impl::isSaturatedValue(true);
        auto * next = b.GetInsertBlock();
        auto * stop = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
        b.SetInsertPoint(stop);
        auto * phi = b.CreatePHI(b.getInt8Ty(), values.size());
        for (size_t i = 0; i < types.size(); i++)
        {
            b.SetInsertPoint(next);
            auto * value = values[i]();
            auto * truth = nativeBoolCast(b, types[i], value);
            if (!types[i]->equals(DataTypeUInt8{}))
                value = b.CreateSelect(truth, b.getInt8(1), b.getInt8(0));
            phi->addIncoming(value, b.GetInsertBlock());
            if (i + 1 < types.size())
            {
                next = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
                b.CreateCondBr(truth, breakOnTrue ? stop : next, breakOnTrue ? next : stop);
            }
        }
        b.CreateBr(stop);
        b.SetInsertPoint(stop);
        return phi;
    }
#endif
};


template <typename A, typename Op>
struct UnaryOperationImpl
{
    using ResultType = typename Op::ResultType;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayC = typename ColumnVector<ResultType>::Container;

    static void NO_INLINE vector(const ArrayA & a, ArrayC & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
    }

    static void constant(A a, ResultType & c)
    {
        c = Op::apply(a);
    }
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryLogical>(); }

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
        if (!isNumber(arguments[0]))
            throw Exception("Illegal type ("
                + arguments[0]->getName()
                + ") of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
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

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &) const override { return true; }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        return b.CreateSelect(Impl<UInt8>::apply(b, nativeBoolCast(b, types[0], values[0]())), b.getInt8(1), b.getInt8(0));
    }
#endif
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
