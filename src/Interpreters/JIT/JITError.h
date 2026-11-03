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
};

/** The error of the function being compiled on the current row, built with `addJITError`: whether the row fails (an `i1`
  * value of the IR), and with which code (an `i16`). The code is only read on failing rows, so LLVM computes it off the
  * hot path of the loop.
  */
struct JITErrorValue
{
    llvm::Value * fails = nullptr;
    llvm::Value * code = nullptr;
};

/// The first error of a compiled expression function, filled after its loop over the rows.
struct JITError
{
    /// `JITErrorCode::None` if no row failed.
    JITErrorCode code = JITErrorCode::None;
    /// The index of the `CompileDAG` node whose function failed.
    uint32_t node = 0;
    uint64_t row = 0;
};

/// Throws the exception the interpreted function throws for `code`.
[[noreturn]] void throwJITError(JITErrorCode code, const std::string & function_description);

/** Collects the error sites of one compiled expression function while its IR is generated.
  * Each site keeps the smallest `row << 16 | code` of the rows it failed on in a stack slot, which LLVM promotes to
  * a register: the lowest failing row, where the interpreter would throw. The slot is updated with `min` on a cold
  * branch, which costs nothing in a scalar loop, and which LLVM turns into a vector `min` when the loop is vectorized.
  * After the loop, the first site (in the order of reporting) that failed wins, and its node is written to `JITError`.
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

private:
    friend void reportJITError(llvm::IRBuilderBase & builder, const JITErrorValue & error);
    friend class JITErrorMaskScope;

    void report(llvm::IRBuilderBase & builder, const JITErrorValue & error);
    void pushMask(llvm::Value * mask) { masks.push_back(mask); }
    void popMask() { masks.pop_back(); }

    llvm::Value * row;
    std::vector<llvm::AllocaInst *> sites;
    std::vector<size_t> site_nodes;
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
  * the lowest failing row of the site wins.
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
void reportJITError(llvm::IRBuilderBase & builder, const JITErrorValue & error);

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
