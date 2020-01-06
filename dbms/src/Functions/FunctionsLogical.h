#pragma once

#include <Core/Types.h>
#include <Core/Defines.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunctionImpl.h>
#include <IO/WriteHelpers.h>
#include <type_traits>


#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h>
#pragma GCC diagnostic pop
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
namespace FunctionsLogicalDetail
{
namespace Ternary
{
    using ResultType = UInt8;

    static constexpr UInt8 False = 0;
    static constexpr UInt8 True = -1;
    static constexpr UInt8 Null = 1;

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

    static inline constexpr bool isSaturable() { return true; }
    static inline constexpr bool isSaturatedValue(UInt8 a) { return a == Ternary::False; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a & b; }
    static inline constexpr bool specialImplementationForNulls() { return true; }
};

struct OrImpl
{
    using ResultType = UInt8;

    static inline constexpr bool isSaturable() { return true; }
    static inline constexpr bool isSaturatedValue(UInt8 a) { return a == Ternary::True; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a | b; }
    static inline constexpr bool specialImplementationForNulls() { return true; }
};

struct XorImpl
{
    using ResultType = UInt8;

    static inline constexpr bool isSaturable() { return false; }
    static inline constexpr bool isSaturatedValue(bool) { return false; }
    /** Considering that CH uses UInt8 for representation of boolean values this function
      * returns 255 as "true" but the current implementation of logical functions suggests that
      * any nonzero value is "true" as well. Also the current code provides no guarantee
      * for "true" to be represented with the value of 1.
      */
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return (a != b) ? Ternary::True : Ternary::False; }
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

    static inline ResultType apply(A a)
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

template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return !Impl::specialImplementationForNulls(); }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_index, size_t input_rows_count) override;

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &) const override { return useDefaultImplementationForNulls(); }

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


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryLogical>(); }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override;

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes &) const override { return true; }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        return b.CreateSelect(Impl<UInt8>::apply(b, nativeBoolCast(b, types[0], values[0]())), b.getInt8(1), b.getInt8(0));
    }
#endif
};

}

struct NameAnd { static constexpr auto name = "and"; };
struct NameOr { static constexpr auto name = "or"; };
struct NameXor { static constexpr auto name = "xor"; };
struct NameNot { static constexpr auto name = "not"; };

using FunctionAnd = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::AndImpl, NameAnd>;
using FunctionOr = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::OrImpl, NameOr>;
using FunctionXor = FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::XorImpl, NameXor>;
using FunctionNot = FunctionsLogicalDetail::FunctionUnaryLogical<FunctionsLogicalDetail::NotImpl, NameNot>;

}
