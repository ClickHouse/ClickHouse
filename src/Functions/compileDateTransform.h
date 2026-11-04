#pragma once

#include "config.h"

#if USE_EMBEDDED_COMPILER

#include <Common/DateLUTImpl.h>
#include <base/types.h>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include <llvm/IR/MDBuilder.h>

#include <llvm/IR/IRBuilder.h>


namespace DB
{

/** Compiled code reads the time-zone tables by calling the transform of the interpreted function, once per row:
  * `jitDateTransform<Transform, From, To>` runs `Transform::execute` on a value of type `From` with a `DateLUTImpl`,
  * exactly as `Transformer::vector` does, and the call passes the value and the result as `Int64`, so that no argument
  * of the call needs a sign or zero extension attribute. The address of the function and of the `DateLUTImpl` (which
  * lives as long as the process, see `DateLUT`) are constants of the compiled code; the time zone must therefore be part
  * of the key of the compiled expression (`IFunctionBase::updateHash`).
  *
  * The transform must not throw for `From`: an exception cannot pass through compiled code, so the function is `noexcept`.
  */
template <typename Transform, typename From, typename To>
Int64 jitDateTransform(Int64 value, const DateLUTImpl * time_zone) noexcept
{
    return static_cast<Int64>(static_cast<To>(Transform{}.execute(static_cast<From>(value), *time_zone)));
}

/// A call of `jitDateTransform<Transform, From, To>` on `value` (of the native type of `From`), returning the native type
/// of `To`.
template <typename Transform, typename From, typename To>
llvm::Value * compileDateTransformCall(llvm::IRBuilderBase & builder, llvm::Value * value, const DateLUTImpl & time_zone)
{
    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * int64_type = b.getInt64Ty();
    auto * pointer_type = b.getPtrTy();

    auto * argument = std::is_signed_v<From> ? b.CreateSExt(value, int64_type) : b.CreateZExt(value, int64_type);
    auto * function_type = llvm::FunctionType::get(int64_type, {int64_type, pointer_type}, /*isVarArg=*/false);
    auto * function = b.CreateIntToPtr(
        b.getInt64(reinterpret_cast<uintptr_t>(&jitDateTransform<Transform, From, To>)), pointer_type);
    auto * time_zone_pointer = b.CreateIntToPtr(b.getInt64(reinterpret_cast<uintptr_t>(&time_zone)), pointer_type);

    auto * call = b.CreateCall(function_type, function, {argument, time_zone_pointer});
    call->setDoesNotThrow();
    call->setOnlyReadsMemory();
    return b.CreateTrunc(call, b.getIntNTy(sizeof(To) * 8));
}

/// The transforms that only read the `Values` of the day of their argument (`DateLUTImpl::getValues`), so that compiled
/// code can read the lookup table itself instead of calling the transform.
enum class JITDateValuesField : uint8_t
{
    None,
    Year,
    Month,
    DayOfMonth,
    Quarter,
    DaysInMonth,
    YYYYMM,
    YYYYMMDD,
};

template <typename Transform>
constexpr JITDateValuesField jitDateValuesField()
{
    constexpr std::string_view name = Transform::name;
    if (name == "toYear" || name == "__toYearCalendarOnly")
        return JITDateValuesField::Year;
    if (name == "toMonth")
        return JITDateValuesField::Month;
    if (name == "toDayOfMonth")
        return JITDateValuesField::DayOfMonth;
    if (name == "toQuarter")
        return JITDateValuesField::Quarter;
    if (name == "toDaysInMonth")
        return JITDateValuesField::DaysInMonth;
    if (name == "toYYYYMM")
        return JITDateValuesField::YYYYMM;
    if (name == "toYYYYMMDD")
        return JITDateValuesField::YYYYMMDD;
    return JITDateValuesField::None;
}

/** The transform `Transform` (one of `jitDateValuesField`) on `value`, a `DateTime` (`UInt32`), a `Date` (`UInt16`) or a
  * `Date32` (`Int32`), reading the lookup table of `time_zone` as `DateLUTImpl::getValues` does:
  * - `DateTime`: `findIndex`: the day guessed from the timestamp, corrected by one day with the start of that day;
  * - `Date`: the day number plus the offset of the epoch (always in the table);
  * - `Date32`: the same when it is in the table; the days out of it call the transform on a cold branch, which computes
  *   their `Values` with cctz.
  */
template <typename Transform, typename From, typename To>
llvm::Value * compileDateValuesTransform(llvm::IRBuilderBase & builder, llvm::Value * value, const DateLUTImpl & time_zone)
{
    using Values = DateLUTImpl::Values;
    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * int64_type = b.getInt64Ty();
    auto * lut = b.CreateIntToPtr(b.getInt64(reinterpret_cast<uintptr_t>(time_zone.getLUT())), b.getPtrTy());
    const uint64_t epoch_index = DateLUTImpl::getDayNumOffsetEpoch();

    auto field_pointer = [&](llvm::Value * index, size_t offset)
    {
        auto * byte_offset = b.CreateAdd(b.CreateMul(index, b.getInt64(sizeof(Values))), b.getInt64(offset));
        return b.CreateInBoundsGEP(b.getInt8Ty(), lut, byte_offset);
    };

    llvm::Value * index = nullptr;
    llvm::Value * in_table = nullptr;
    if constexpr (std::is_same_v<From, UInt32>)
    {
        auto * t = b.CreateZExt(value, int64_type);
        auto * guess = b.CreateAdd(b.CreateUDiv(t, b.getInt64(86400)), b.getInt64(epoch_index));
        auto * next = b.CreateAdd(guess, b.getInt64(1));
        auto * guess_date = b.CreateLoad(int64_type, field_pointer(guess, offsetof(Values, date)));
        auto * next_date = b.CreateLoad(int64_type, field_pointer(next, offsetof(Values, date)));
        index = b.CreateSelect(
            b.CreateICmpSGE(t, guess_date),
            b.CreateSelect(b.CreateICmpSLT(t, next_date), guess, next),
            b.CreateSub(guess, b.getInt64(1)));
    }
    else if constexpr (std::is_same_v<From, UInt16>)
    {
        index = b.CreateAdd(b.CreateZExt(value, int64_type), b.getInt64(epoch_index));
    }
    else
    {
        static_assert(std::is_same_v<From, Int32>);
        auto * day_index = b.CreateAdd(b.CreateSExt(value, int64_type), b.getInt64(epoch_index));
        in_table = b.CreateICmpULT(day_index, b.getInt64(DATE_LUT_SIZE));
        index = b.CreateSelect(in_table, day_index, b.getInt64(0));
    }

    auto load_field = [&](size_t offset, llvm::Type * type)
    {
        return b.CreateZExt(b.CreateLoad(type, field_pointer(index, offset)), int64_type);
    };
    auto * year = load_field(offsetof(Values, year), b.getInt16Ty());
    auto * month = load_field(offsetof(Values, month), b.getInt8Ty());

    llvm::Value * result = nullptr;
    switch (jitDateValuesField<Transform>())
    {
        case JITDateValuesField::Year:
            result = year;
            break;
        case JITDateValuesField::Month:
            result = month;
            break;
        case JITDateValuesField::DayOfMonth:
            result = load_field(offsetof(Values, day_of_month), b.getInt8Ty());
            break;
        case JITDateValuesField::Quarter:
            result = b.CreateAdd(b.CreateUDiv(b.CreateSub(month, b.getInt64(1)), b.getInt64(3)), b.getInt64(1));
            break;
        case JITDateValuesField::DaysInMonth:
            result = load_field(offsetof(Values, days_in_month), b.getInt8Ty());
            break;
        case JITDateValuesField::YYYYMM:
            result = b.CreateAdd(b.CreateMul(year, b.getInt64(100)), month);
            break;
        case JITDateValuesField::YYYYMMDD:
            result = b.CreateAdd(
                b.CreateAdd(b.CreateMul(year, b.getInt64(10000)), b.CreateMul(month, b.getInt64(100))),
                load_field(offsetof(Values, day_of_month), b.getInt8Ty()));
            break;
        case JITDateValuesField::None:
            break;
    }
    auto * result_type = b.getIntNTy(sizeof(To) * 8);
    result = b.CreateTrunc(result, result_type);
    if (!in_table)
        return result;

    /// A `Date32` out of the table: the transform computes its `Values` with cctz.
    auto * function = b.GetInsertBlock()->getParent();
    auto * inline_block = b.GetInsertBlock();
    auto * out_of_table = llvm::BasicBlock::Create(b.getContext(), "date_out_of_table", function);
    auto * join = llvm::BasicBlock::Create(b.getContext(), "date_join", function);
    b.CreateCondBr(in_table, join, out_of_table, llvm::MDBuilder(b.getContext()).createLikelyBranchWeights());
    b.SetInsertPoint(out_of_table);
    auto * called = compileDateTransformCall<Transform, From, To>(builder, value, time_zone);
    b.CreateBr(join);
    b.SetInsertPoint(join);
    auto * phi = b.CreatePHI(result_type, 2);
    phi->addIncoming(result, inline_block);
    phi->addIncoming(called, out_of_table);
    return phi;
}

}

#endif
