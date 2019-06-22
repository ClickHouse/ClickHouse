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
#include <llvm/IR/IRBuilder.h>
#pragma GCC diagnostic pop
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}


// TODO: This comment is to be rewritten
/** Behaviour in presence of NULLs:
  *
  * Functions AND, XOR, NOT use default implementation for NULLs:
  * - if one of arguments is Nullable, they return Nullable result where NULLs are returned when at least one argument was NULL.
  *
  * But function OR is different.
  * It always return non-Nullable result and NULL are equivalent to 0 (false).
  * For example, 1 OR NULL returns 1, not NULL.
  */

using UInt8Container = ColumnUInt8::Container;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;
using Columns3vPtrs = std::vector<ColumnUInt8::MutablePtr>;

namespace {
namespace Trivalent
{
    const UInt8 False = 0;
    const UInt8 True = -1;
    const UInt8 Null = 1;
}

inline UInt8 toTrivalent(bool value, bool is_null = false)
{
    if (is_null)
        return Trivalent::Null;
    return value ? Trivalent::True : Trivalent::False;
}

MutableColumnPtr makeColumnFromTrivalentData(const UInt8Container & trivalent_data, bool is_nullable)
{
    const size_t rows_count = trivalent_data.size();

    auto new_column = ColumnUInt8::create(rows_count);
    for (size_t i = 0; i < rows_count; ++i)
        new_column->getData()[i] = (trivalent_data[i] == Trivalent::True);

    if (!is_nullable)
        return new_column;

    auto null_column = ColumnUInt8::create(rows_count);
    for (size_t i = 0; i < rows_count; ++i)
        null_column->getData()[i] = (trivalent_data[i] == Trivalent::Null);

    return ColumnNullable::create(std::move(new_column), std::move(null_column));
}

template <typename T>
bool convertTypeToTrivalent(const IColumn * column, ConstNullMapPtr null_map_ptr, UInt8Container & res)
{
    auto col = checkAndGetColumn<ColumnVector<T>>(column);
    if (!col)
        return false;

    const size_t n = res.size();
    const auto & vec = col->getData();

    if (null_map_ptr != nullptr)
    {
        for (size_t i = 0; i < n; ++i)
            res[i] = toTrivalent(!!vec[i], (*null_map_ptr)[i]);
    }
    else
    {
        for (size_t i = 0; i < n; ++i)
            res[i] = toTrivalent(!!vec[i]);
    }

    return true;
}

ColumnUInt8::MutablePtr columnTrivalentFromColumnVector(const IColumn * column)
{
    ConstNullMapPtr null_map_ptr = nullptr;
    if (const auto column_nullable = checkAndGetColumn<ColumnNullable>(column))
    {
        null_map_ptr = &column_nullable->getNullMapData();
        column = column_nullable->getNestedColumnPtr().get();
    }

    auto result_column = ColumnUInt8::create(column->size());
    auto & result_data = result_column->getData();

    if (!convertTypeToTrivalent<Int8>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<Int16>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<Int32>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<Int64>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<UInt8>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<UInt16>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<UInt32>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<UInt64>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<Float32>(column, null_map_ptr, result_data) &&
        !convertTypeToTrivalent<Float64>(column, null_map_ptr, result_data))
        throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);

    return result_column;
}
}

struct AndImpl
{
    static inline constexpr bool isSaturable()
    {
        return true;
    }

    static inline constexpr bool isSaturatedValue(UInt8 a)
    {
        return a == Trivalent::False;
    }

    static inline constexpr UInt8 apply(UInt8 a, UInt8 b)
    {
        return a & b;
    }

    static inline constexpr bool specialImplementationForNulls() { return true; }
};

struct OrImpl
{
    static inline constexpr bool isSaturable()
    {
        return true;
    }

    static inline constexpr bool isSaturatedValue(UInt8 a)
    {
        return a == Trivalent::True;
    }

    static inline constexpr UInt8 apply(UInt8 a, UInt8 b)
    {
        return a | b;
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

    static inline constexpr UInt8 apply(UInt8 a, UInt8 b)
    {
        return (a != b) ? Trivalent::True : Trivalent::False;
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

/*
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

        AssociativeOperationImpl<Op, N - 1> operation(in);
        in.erase(in.end() - N, in.end());

        const size_t n = result.size();
        if (Op::isSaturable())
        {
            for (size_t i = 0; i < n; ++i)
                result[i] = Op::isSaturatedValue(result[i]) ? result[i] : Op::apply(result[i], operation.apply(i));
        }
        else
        {
            for (size_t i = 0; i < n; ++i)
                result[i] = Op::apply(result[i], operation.apply(i));
        }
    }

    const UInt8Container & vec;
    AssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData()), continuation(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline UInt8 applySaturable(size_t i) const
    {
        return Op::isSaturatedValue(vec[i]) ? vec[i] : Op::apply(vec[i], continuation.applySaturable(i));
    }
    inline UInt8 apply(size_t i) const
    {
        return Op::apply(vec[i], continuation.apply(i));
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        if (in.size() != 1)
        {
            throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
        }

        AssociativeOperationImpl<Op, 1> operation(in);

        const size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
            result[i] = Op::apply(result[i], in[0]);

        in.clear();
    }

    const UInt8Container & vec;

    AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in.back()->getData()) {}

    inline UInt8 apply(size_t i) const
    {
        return vec[i];
    }
};
*/

template <typename Op, size_t N>
struct AssociativeOperationImpl
{
    /// Erases the N last columns from `in` (if there are less, then all) and puts into `result` their combination.
    static void NO_INLINE execute(Columns3vPtrs & in, UInt8Container & result)
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
            result[i] = operation.apply(result[i], i);

    }

    const UInt8Container & vec;
    AssociativeOperationImpl<Op, N - 1> continuation;

    /// Remembers the last N columns from `in`.
    AssociativeOperationImpl(Columns3vPtrs & in)
        : vec(in[in.size() - N]->getData()), continuation(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline UInt8 apply(UInt8 a, size_t i) const
    {
        if (Op::isSaturable())
            return Op::isSaturatedValue(a) ? a : Op::apply(a, continuation.apply(vec[i], i));
        else
            return Op::apply(a, continuation.apply(vec[i], i));
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 1>
{
    static void execute(Columns3vPtrs & in, UInt8Container & result)
    {
        if (in.size() != 1)
        {
            throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
        }

        AssociativeOperationImpl<Op, 1> operation(in);

        const size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
            result[i] = operation.apply(result[i], i);

        in.clear();
    }

    const UInt8Container & vec;

    AssociativeOperationImpl(Columns3vPtrs & in)
        : vec(in.back()->getData()) {}

    inline UInt8 apply(UInt8 a, size_t i) const
    {
        return Op::apply(a, vec[i]);
    }
};

template <typename Op>
UInt8Container associativeOperationApplyAll(Columns3vPtrs & args)
{
    if (args.size() < 2)
        return {};

    UInt8Container result_data = std::move(args.back()->getData());
    args.pop_back();

    /// Effeciently combine all the columns of the correct type.
    while (args.size() > 0)
    {
        /// With a large block size, combining 10 columns per pass is the fastest.
        /// When small - more, is faster.
        AssociativeOperationImpl<Op, 10>::execute(args, result_data);
    }

    return result_data;
}


template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); }

private:
    static bool extractConstColumnsTrivalent(ColumnRawPtrs & in, UInt8 & res_3v)
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            if (!in[i]->isColumnConst())
                continue;

            const auto field_value = (*in[i])[0];

            UInt8 value_3v = field_value.isNull()
                    ? toTrivalent(false, true)
                    : toTrivalent(applyVisitor(FieldVisitorConvertToNumber<bool>(), field_value));

            if (has_res)
            {
                res_3v = Impl::apply(res_3v, value_3v);
            }
            else
            {
                res_3v = value_3v;
                has_res = true;
            }

            in.erase(in.begin() + i);
        }
        return has_res;
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
            throw Exception("Number of arguments for function \"" + getName() + "\" should be at least 2: passed "
                + toString(arguments.size()),
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

        bool has_nullable_arguments = false;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & arg_type = arguments[i];

            if (!has_nullable_arguments)
                has_nullable_arguments = arg_type->isNullable();

            if (!(isNativeNumber(arg_type)
                || (Impl::specialImplementationForNulls() && (arg_type->onlyNull() || isNativeNumber(removeNullable(arg_type))))))
                throw Exception("Illegal type ("
                    + arg_type->getName()
                    + ") of " + toString(i + 1) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        /// Put this check inside the loop
        if (has_nullable_arguments && !Impl::specialImplementationForNulls())
            throw Exception(
                "Function \"" + getName() + "\" does not support Nullable arguments",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto result_type = std::make_shared<DataTypeUInt8>();
        return has_nullable_arguments
                ? makeNullable(result_type)
                : result_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_index, size_t input_rows_count) override
    {
        auto & result_info = block.getByPosition(result_index);

        ColumnRawPtrs args_in;
        for (const auto arg_index : arguments)
        {
            const auto & arg_info = block.getByPosition(arg_index);
            args_in.push_back(arg_info.column.get());
        }

        /// Combine all constant columns into a single value.
        UInt8 const_3v_value = 0;
        bool has_consts = extractConstColumnsTrivalent(args_in, const_3v_value);

        /// If this value uniquely determines result, return it.
        if (has_consts && (args_in.empty() || (Impl::isSaturable() && Impl::isSaturatedValue(const_3v_value))))
        {
            result_info.column = ColumnConst::create(
                makeColumnFromTrivalentData(UInt8Container({const_3v_value}), result_info.type->isNullable()),
                input_rows_count
            );
            return;
        }

        // TODO: What if arguments are duplicated?

        /// Prepare Trivalent representation for all columns
        Columns3vPtrs args_in_3v;
        if (has_consts)
            args_in_3v.push_back(ColumnUInt8::create(input_rows_count, const_3v_value));
        for (const auto arg_column_ptr : args_in)
            args_in_3v.push_back(columnTrivalentFromColumnVector(arg_column_ptr));

        const auto result_data = associativeOperationApplyAll<Impl>(args_in_3v);

        result_info.column = makeColumnFromTrivalentData(result_data, result_info.type->isNullable());
    }

    /// TODO: Rework this part !!!
    /// TODO: Rework this part !!!
    /// TODO: Rework this part !!!
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
        if (!isNativeNumber(arguments[0]))
            throw Exception("Illegal type ("
                + arguments[0]->getName()
                + ") of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (!(executeType<UInt8>(block, arguments, result)
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
