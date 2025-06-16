#pragma once

#include <base/types.h>
#include <Core/Defines.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <type_traits>
#include <Interpreters/Context_fwd.h>


#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif


/** Logical functions AND, OR, XOR and NOT support three-valued (or ternary) logic
  * https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
  *
  * Functions XOR and NOT rely on "default implementation for NULLs":
  *   - if any of the arguments is of Nullable type, the return value type is Nullable
  *   - if any of the arguments is NULL, the return value is NULL
  *
  * Functions AND and OR provide their own special implementations for ternary logic
  */

namespace DB
{

struct NameAnd { static constexpr auto name = "and"; };
struct NameOr { static constexpr auto name = "or"; };
struct NameXor { static constexpr auto name = "xor"; };
struct NameNot { static constexpr auto name = "not"; };

namespace FunctionsLogicalDetail
{
namespace Ternary
{
    using ResultType = UInt8;

    /** These values are carefully picked so that they could be efficiently evaluated with bitwise operations, which
      * are feasible for auto-vectorization by the compiler. The expression for the ternary value evaluation writes:
      *
      * ternary_value = ((value << 1) | is_null) & (1 << !is_null)
      *
      * The truth table of the above formula lists:
      *  +---------------+--------------+-------------+
      *  | is_null\value |      0       |      1      |
      *  +---------------+--------------+-------------+
      *  |             0 | 0b00 (False) | 0b10 (True) |
      *  |             1 | 0b01 (Null)  | 0b01 (Null) |
      *  +---------------+--------------+-------------+
      *
      * As the numerical values of False, Null and True are assigned in ascending order, the "and" and "or" of
      * ternary logic could be implemented with minimum and maximum respectively, which are also vectorizable.
      * https://en.wikipedia.org/wiki/Three-valued_logic
      *
      * This logic does not apply for "not" and "xor" - they work with default implementation for NULLs:
      *  anything with NULL returns NULL, otherwise use conventional two-valued logic.
      */
    static constexpr UInt8 False = 0;   /// 0b00
    static constexpr UInt8 Null = 1;    /// 0b01
    static constexpr UInt8 True = 2;   /// 0b10

    template <typename T>
    inline ResultType makeValue(T value)
    {
        return value != 0 ? Ternary::True : Ternary::False;
    }

    template <typename T>
    inline ResultType makeValue(T value, bool is_null)
    {
        if (is_null)
            return Ternary::Null;
        return makeValue<T>(value);
    }
}


struct AndImpl
{
    using ResultType = UInt8;

    static constexpr bool isSaturable() { return true; }

    /// Final value in two-valued logic (no further operations with True, False will change this value)
    static constexpr bool isSaturatedValue(bool a) { return !a; }

    /// Final value in three-valued logic (no further operations with True, False, Null will change this value)
    static constexpr bool isSaturatedValueTernary(UInt8 a) { return a == Ternary::False; }

    static constexpr ResultType apply(UInt8 a, UInt8 b) { return a & b; }

    static constexpr ResultType ternaryApply(UInt8 a, UInt8 b) { return std::min(a, b); }

    /// Will use three-valued logic for NULLs (see above) or default implementation (any operation with NULL returns NULL).
    static constexpr bool specialImplementationForNulls() { return true; }
};

struct OrImpl
{
    using ResultType = UInt8;

    static constexpr bool isSaturable() { return true; }
    static constexpr bool isSaturatedValue(bool a) { return a; }
    static constexpr bool isSaturatedValueTernary(UInt8 a) { return a == Ternary::True; }
    static constexpr ResultType apply(UInt8 a, UInt8 b) { return a | b; }
    static constexpr ResultType ternaryApply(UInt8 a, UInt8 b) { return std::max(a, b); }
    static constexpr bool specialImplementationForNulls() { return true; }
};

struct XorImpl
{
    using ResultType = UInt8;

    static constexpr bool isSaturable() { return false; }
    static constexpr bool isSaturatedValue(bool) { return false; }
    static constexpr bool isSaturatedValueTernary(UInt8) { return false; }
    static constexpr ResultType apply(UInt8 a, UInt8 b) { return a != b; }
    static constexpr ResultType ternaryApply(UInt8 a, UInt8 b) { return a != b; }
    static constexpr bool specialImplementationForNulls() { return false; }

#if USE_EMBEDDED_COMPILER
    static llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a, llvm::Value * b)
    {
        return builder.CreateXor(a, b);
    }
#endif
};

template <typename A>
struct NotImpl
{
    using ResultType = UInt8;

    static ResultType apply(A a)
    {
        return !static_cast<bool>(a);
    }

#if USE_EMBEDDED_COMPILER
    static llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a)
    {
        return builder.CreateNot(a);
    }
#endif
};

template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionAnyArityLogical>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isShortCircuit(ShortCircuitSettings & settings, size_t /*number_of_arguments*/) const override
    {
        settings.arguments_with_disabled_lazy_execution.insert(0);
        settings.enable_lazy_execution_for_common_descendants_of_arguments = true;
        settings.force_enable_lazy_execution = false;
        return name == NameAnd::name || name == NameOr::name;
    }
    ColumnPtr executeShortCircuit(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool canBeExecutedOnLowCardinalityDictionary() const override { return false; }

    bool useDefaultImplementationForNulls() const override { return !Impl::specialImplementationForNulls(); }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override;

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override;

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &, const DataTypePtr &) const override { return useDefaultImplementationForNulls(); }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & values, const DataTypePtr &) const override
    {
        assert(!values.empty());

        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        if constexpr (!Impl::isSaturable())
        {
            auto * result = nativeBoolCast(b, values[0]);
            for (size_t i = 1; i < values.size(); ++i)
                result = Impl::apply(b, result, nativeBoolCast(b, values[i]));
            return b.CreateSelect(result, b.getInt8(1), b.getInt8(0));
        }

        constexpr bool break_on_true = Impl::isSaturatedValue(true);
        auto * next = b.GetInsertBlock();
        auto * stop = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
        b.SetInsertPoint(stop);

        auto * phi = b.CreatePHI(b.getInt8Ty(), static_cast<unsigned>(values.size()));

        for (size_t i = 0; i < values.size(); ++i)
        {
            b.SetInsertPoint(next);
            auto * value = values[i].value;
            auto * truth = nativeBoolCast(b, values[i]);
            if (!values[i].type->equals(DataTypeUInt8{}))
                value = b.CreateSelect(truth, b.getInt8(1), b.getInt8(0));
            phi->addIncoming(value, b.GetInsertBlock());
            if (i + 1 < values.size())
            {
                next = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
                b.CreateCondBr(truth, break_on_true ? stop : next, break_on_true ? next : stop);
            }
        }

        b.CreateBr(stop);
        b.SetInsertPoint(stop);

        return phi;
    }
#endif
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUnaryLogical>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override;

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &, const DataTypePtr &) const override { return true; }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & values, const DataTypePtr &) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        return b.CreateSelect(Impl<UInt8>::apply(b, nativeBoolCast(b, values[0])), b.getInt8(1), b.getInt8(0));
    }
#endif
};

}

using FunctionAnd = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::AndImpl, NameAnd>;
using FunctionOr = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::OrImpl, NameOr>;
using FunctionXor = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::XorImpl, NameXor>;
using FunctionNot = FunctionsLogicalDetail::FunctionUnaryLogical<FunctionsLogicalDetail::NotImpl, NameNot>;

}
