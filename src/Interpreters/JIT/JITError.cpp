#include <Interpreters/JIT/JITError.h>

#if USE_EMBEDDED_COMPILER

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <base/extended_types.h>

#include <cstddef>
#include <limits>

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/MDBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
    extern const int FUNCTION_THROW_IF_VALUE_IS_NON_ZERO;
    extern const int ILLEGAL_DIVISION;
    extern const int LOGICAL_ERROR;
}

namespace
{

thread_local JITErrorSink * current_sink = nullptr;

/// The slot of an error site that has not failed. A failure is `row << 16 | code`, always smaller.
constexpr uint64_t no_error_key = std::numeric_limits<uint64_t>::max();

[[noreturn]] void throwWithDescription(Exception exception, const std::string & function_description)
{
    exception.addMessage("while executing JIT-compiled '{}'", function_description);
    throw exception;
}

const std::string & siteParameter(const JITErrorSite & site, size_t index)
{
    if (index >= site.parameters.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A JIT error site has {} parameters, expected at least {}", site.parameters.size(), index + 1);
    return site.parameters[index];
}

}

void throwJITError(const JITError & error, const JITErrorSite & site, const std::string & function_description)
{
    /// The messages are the ones the interpreted functions throw.
    switch (error.code)
    {
        case JITErrorCode::DivisionByZero:
            throwWithDescription(Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by zero"), function_description);
        case JITErrorCode::DivisionOfMinimalSignedNumberByMinusOne:
            throwWithDescription(
                Exception(ErrorCodes::ILLEGAL_DIVISION, "Division of minimal signed number by minus one"), function_description);
        case JITErrorCode::DivisionByTheMostNegativeNumber:
            throwWithDescription(Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by the most negative number"), function_description);
        case JITErrorCode::ThrowIfNonZero:
            throwWithDescription(
                Exception(ErrorCodes::FUNCTION_THROW_IF_VALUE_IS_NON_ZERO, "Value passed to '{}' function is non-zero", siteParameter(site, 0)),
                function_description);
        case JITErrorCode::DecimalMathOverflow:
            throwWithDescription(Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow"), function_description);
        case JITErrorCode::None:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "A JIT-compiled expression reported an error without a code");
}

JITErrorSink::JITErrorSink(llvm::Value * row_)
    : row(row_)
    , previous(current_sink)
{
    current_sink = this;
}

JITErrorSink::~JITErrorSink()
{
    current_sink = previous;
}

JITErrorSink * JITErrorSink::current()
{
    return current_sink;
}

void JITErrorSink::report(llvm::IRBuilderBase & builder, const JITErrorValue & error, std::vector<std::string> parameters)
{
    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * key_type = b.getInt64Ty();
    auto * operand_type = b.getInt128Ty();

    if (!error.fails || !error.code)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A JIT error was reported without a way to fail");

    llvm::Value * fails = error.fails;

    for (auto * mask : masks)
        fails = b.CreateAnd(fails, mask);

    /// The slots are allocated and set to "no error" at the start of the entry block, which runs once before the loop,
    /// so that LLVM promotes them to registers.
    auto & entry = b.GetInsertBlock()->getParent()->getEntryBlock();
    llvm::IRBuilder<> entry_builder(&entry, entry.getFirstInsertionPt());
    Slots site_slots;
    site_slots.key = entry_builder.CreateAlloca(key_type);
    entry_builder.CreateStore(llvm::ConstantInt::get(key_type, no_error_key), site_slots.key);
    if (error.operand)
    {
        site_slots.operand = entry_builder.CreateAlloca(operand_type);
        entry_builder.CreateStore(llvm::ConstantInt::get(operand_type, 0), site_slots.operand);
    }

    /// The error is recorded on a cold branch, so that the hot path of a scalar loop has no loop-carried chain.
    auto & context = b.getContext();
    auto * function = b.GetInsertBlock()->getParent();
    auto * record = llvm::BasicBlock::Create(context, "jit_error", function);
    auto * next = llvm::BasicBlock::Create(context, "jit_error_next", function);
    b.CreateCondBr(fails, record, next, llvm::MDBuilder(context).createUnlikelyBranchWeights());

    b.SetInsertPoint(record);
    auto * row_key = b.CreateShl(b.CreateZExtOrTrunc(row, key_type), 16);
    auto * failed_key = b.CreateOr(row_key, b.CreateZExt(error.code, key_type));
    auto * previous_key = b.CreateLoad(key_type, site_slots.key);
    b.CreateStore(b.CreateBinaryIntrinsic(llvm::Intrinsic::umin, previous_key, failed_key), site_slots.key);
    if (site_slots.operand)
    {
        /// The operand follows the key: it changes only where the key does.
        auto * is_first = b.CreateICmpULT(failed_key, previous_key);
        auto * operand = b.CreateSExtOrTrunc(error.operand, operand_type);
        auto * previous_operand = b.CreateLoad(operand_type, site_slots.operand);
        b.CreateStore(b.CreateSelect(is_first, operand, previous_operand), site_slots.operand);
    }
    b.CreateBr(next);

    b.SetInsertPoint(next);

    slots.push_back(site_slots);
    sites.push_back({.node = current_node, .parameters = std::move(parameters)});
}

void JITErrorSink::writeFirstError(llvm::IRBuilderBase & builder, llvm::Value * error) const
{
    /// The caller initializes `JITError` to "no error", so a function that cannot fail gets no extra code.
    if (slots.empty())
        return;

    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * key_type = b.getInt64Ty();
    auto * operand_type = b.getInt128Ty();
    auto * no_error = llvm::ConstantInt::get(key_type, no_error_key);

    /// Must match the layout of `JITError`.
    auto * error_type = llvm::StructType::get(b.getInt16Ty(), b.getInt32Ty(), b.getInt64Ty(), b.getInt64Ty(), b.getInt64Ty());

    /// Walking the sites backwards, an earlier site that failed replaces a later one.
    llvm::Value * first_key = no_error;
    llvm::Value * first_site = b.getInt32(0);
    llvm::Value * first_operand = llvm::ConstantInt::get(operand_type, 0);
    for (size_t i = slots.size(); i > 0; --i)
    {
        auto * key = b.CreateLoad(key_type, slots[i - 1].key);
        auto * failed = b.CreateICmpNE(key, no_error);
        first_key = b.CreateSelect(failed, key, first_key);
        first_site = b.CreateSelect(failed, b.getInt32(static_cast<uint32_t>(i - 1)), first_site);
        llvm::Value * operand = slots[i - 1].operand
            ? static_cast<llvm::Value *>(b.CreateLoad(operand_type, slots[i - 1].operand))
            : llvm::ConstantInt::get(operand_type, 0);
        first_operand = b.CreateSelect(failed, operand, first_operand);
    }

    auto * failed = b.CreateICmpNE(first_key, no_error);
    auto * code = b.CreateSelect(failed, b.CreateTrunc(first_key, b.getInt16Ty()), jitErrorCode(b, JITErrorCode::None));
    auto * row_value = b.CreateSelect(failed, b.CreateLShr(first_key, 16), llvm::ConstantInt::get(key_type, 0));

    b.CreateStore(code, b.CreateStructGEP(error_type, error, 0));
    b.CreateStore(first_site, b.CreateStructGEP(error_type, error, 1));
    b.CreateStore(row_value, b.CreateStructGEP(error_type, error, 2));
    b.CreateStore(b.CreateTrunc(first_operand, key_type), b.CreateStructGEP(error_type, error, 3));
    b.CreateStore(b.CreateTrunc(b.CreateLShr(first_operand, 64), key_type), b.CreateStructGEP(error_type, error, 4));
}

llvm::Value * jitErrorCode(llvm::IRBuilderBase & builder, JITErrorCode code)
{
    return builder.getInt16(static_cast<uint16_t>(code));
}

void addJITError(llvm::IRBuilderBase & builder, JITErrorValue & error, llvm::Value * fails, JITErrorCode code)
{
    auto * this_code = jitErrorCode(builder, code);
    if (!error.fails)
    {
        error.fails = fails;
        error.code = this_code;
        return;
    }

    /// The code is read only where the row fails: an earlier way to fail keeps its code.
    error.code = builder.CreateSelect(error.fails, error.code, this_code);
    error.fails = builder.CreateOr(error.fails, fails);
}

void reportJITError(llvm::IRBuilderBase & builder, const JITErrorValue & error, std::vector<std::string> parameters)
{
    auto * sink = JITErrorSink::current();
    if (!sink)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A JIT error was reported outside of the compilation of an expression function");

    sink->report(builder, error, std::move(parameters));
}

JITErrorMaskScope::JITErrorMaskScope(llvm::Value * mask)
    : sink(JITErrorSink::current())
{
    /// Without a sink no error can be reported, so there is nothing to mask.
    if (sink)
        sink->pushMask(mask);
}

JITErrorMaskScope::~JITErrorMaskScope()
{
    if (sink)
        sink->popMask();
}

static_assert(sizeof(JITError) == 32 && offsetof(JITError, code) == 0 && offsetof(JITError, site) == 4 && offsetof(JITError, row) == 8
        && offsetof(JITError, operand_low) == 16 && offsetof(JITError, operand_high) == 24,
    "The layout of JITError must match the structure written by JITErrorSink::writeFirstError");

}

#endif
