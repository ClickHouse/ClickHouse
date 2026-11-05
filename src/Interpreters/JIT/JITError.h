#pragma once

#include "config.h"

#if USE_EMBEDDED_COMPILER

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace llvm
{
    class AllocaInst;
    class IRBuilderBase;
    class Value;
}

namespace DB
{

/// The errors a JIT-compiled function can report. Each one is an exception of the interpreted function, see `throwJITError`.
enum class JITErrorCode : uint16_t
{
    None = 0,
    DivisionByZero,
    DivisionOfMinimalSignedNumberByMinusOne,
    DivisionByTheMostNegativeNumber,
    /// `throwIf` got a value that is not zero. The parameter of its site is the name of the function.
    ThrowIfNonZero,
    /// `Decimal` arithmetic overflows (`DecimalBinaryOperation` with `decimal_check_overflow`).
    DecimalMathOverflow,
};

/** The error of the function being compiled on the current row, built with `addJITError`: whether the row fails (an `i1`
  * value of the IR), and with which code (an `i16`). The code is only read on failing rows, so LLVM computes it off the
  * hot path of the loop.
  */
struct JITErrorValue
{
    llvm::Value * fails = nullptr;
    llvm::Value * code = nullptr;
    /// The value the message of the error shows, for codes whose message has one: a signed integer of up to 128 bits.
    llvm::Value * operand = nullptr;
};

/// An error site of a compiled expression function: the node whose function reported it, and the parameters of its
/// message that are known when the function is compiled.
struct JITErrorSite
{
    size_t node = 0;
    std::vector<std::string> parameters;
};

/// The first error of a compiled expression function, filled after its loop over the rows.
struct JITError
{
    /// `JITErrorCode::None` if no row failed.
    JITErrorCode code = JITErrorCode::None;
    /// The index of the site that failed, in `JITErrorSink::getSites`.
    uint32_t site = 0;
    uint64_t row = 0;
    /// The operand of the failing row, if the site reports one, as the low and the high half of a signed 128-bit integer.
    uint64_t operand_low = 0;
    uint64_t operand_high = 0;
};

/// Throws the exception the interpreted function throws for `error`, which `site` reported.
[[noreturn]] void throwJITError(const JITError & error, const JITErrorSite & site, const std::string & function_description);

/** Collects the error sites of one compiled expression function while its IR is generated.
  * Each site keeps the smallest `row << 16 | code` of the rows it failed on in a stack slot, which LLVM promotes to
  * a register: the lowest failing row, where the interpreter would throw. The slot is updated with `min` on a cold
  * branch, which costs nothing in a scalar loop, and which LLVM turns into a vector `min` when the loop is vectorized.
  * After the loop, the first site (in the order of reporting) that failed wins, and its index is written to `JITError`.
  * A site with an operand also keeps the operand of its lowest failing row, updated on the same cold branch.
  */
class JITErrorSink
{
public:
    /// `row` is the loop counter of the function being compiled.
    explicit JITErrorSink(llvm::Value * row);
    ~JITErrorSink();

    JITErrorSink(const JITErrorSink &) = delete;
    JITErrorSink & operator=(const JITErrorSink &) = delete;

    /// The sink of the function being compiled on this thread, or nullptr.
    static JITErrorSink * current();

    /// Writes the first error into `error` (a `JITError *`, initialized to "no error" by the caller).
    /// Must be called after the loop.
    void writeFirstError(llvm::IRBuilderBase & builder, llvm::Value * error) const;

    /// Set by `CompileDAG::compile` before each node is compiled.
    void setCurrentNode(size_t node) { current_node = node; }

    /// The sites, in the order they were reported; `JITError::site` indexes them.
    const std::vector<JITErrorSite> & getSites() const { return sites; }

private:
    friend void reportJITError(llvm::IRBuilderBase & builder, const JITErrorValue & error, std::vector<std::string> parameters);
    friend class JITErrorMaskScope;

    void report(llvm::IRBuilderBase & builder, const JITErrorValue & error, std::vector<std::string> parameters);
    void pushMask(llvm::Value * mask) { masks.push_back(mask); }
    void popMask() { masks.pop_back(); }

    /// The slots of a site: its smallest key, and the operand of that row (nullptr if the site has no operand).
    struct Slots
    {
        llvm::AllocaInst * key = nullptr;
        llvm::AllocaInst * operand = nullptr;
    };

    llvm::Value * row;
    std::vector<Slots> slots;
    std::vector<JITErrorSite> sites;
    std::vector<llvm::Value *> masks;
    size_t current_node = 0;
    JITErrorSink * previous;
};

/// `code` as a value of the IR being generated.
llvm::Value * jitErrorCode(llvm::IRBuilderBase & builder, JITErrorCode code);

/** Adds a way to fail to `error`: the row also fails with `code` where `fails` (an `i1`) is true. Start with an empty
  * `JITErrorValue` and add the ways to fail in the order the interpreted function checks them, so that the first one
  * wins on a row where several are true.
  */
void addJITError(llvm::IRBuilderBase & builder, JITErrorValue & error, llvm::Value * fails, JITErrorCode code);

/** Reports the error of the function being compiled on the current row, built with `addJITError`. Only valid while
  * an expression function is compiled. Each call is one error site: like the interpreter, which checks each row in turn,
  * the lowest failing row of the site wins. `parameters` are the parts of the message known at compile time, see
  * `JITErrorCode`.
  *
  * The error must be computed before the operation that would fail. Where the operation would trap or produce poison
  * on a failing row (integer division, a float to integer conversion, a shift), its inputs must be replaced on those
  * rows; the result there is discarded. Prefer arithmetic or saturating
  * forms to a `select` (see `JITIntegerDivision`), and check every new function with a performance test.
  *
  * A function with the default implementation for NULLs that reports errors must return false from
  * `canBeExecutedOnDefaultArguments`: the interpreter then never executes it on rows where an argument is NULL,
  * and `IFunction::compile` masks those rows, so both agree.
  */
void reportJITError(llvm::IRBuilderBase & builder, const JITErrorValue & error, std::vector<std::string> parameters = {});

/// Rows where `mask` (an `i1`) is false never fail, for example rows where the result is NULL.
class JITErrorMaskScope
{
public:
    explicit JITErrorMaskScope(llvm::Value * mask);
    ~JITErrorMaskScope();

    JITErrorMaskScope(const JITErrorMaskScope &) = delete;
    JITErrorMaskScope & operator=(const JITErrorMaskScope &) = delete;

private:
    JITErrorSink * sink;
};

}

#endif
