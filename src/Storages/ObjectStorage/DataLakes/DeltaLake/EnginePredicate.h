#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Common/Exception.h>
#include <delta_kernel_ffi.hpp>

namespace DB
{
class ActionsDAG;
}

namespace DeltaLake
{

/// EnginePredicate used in ffi::scan.
/// A predicate which allows to implement internal delta-kernel-rs filtering
/// based on statistics and partitions pruning.
class EnginePredicate : public ffi::EnginePredicate
{
    friend struct EngineIteratorData;
public:
    explicit EnginePredicate(
        const DB::ActionsDAG & filter_,
        std::exception_ptr & exception_)
        : filter(filter_)
        , exception(exception_)
    {
        predicate = this;
        visitor = &visitPredicate;
    }

    bool hasException() const { return exception != nullptr; }

    void setException(std::exception_ptr exception_)
    {
        DB::tryLogException(exception_, log);
        if (!exception)
            exception = exception_;
    }

    const DB::ActionsDAG & getFilterDAG() const { return filter; }

private:
    const LoggerPtr log = getLogger("EnginePredicate");

    /// Predicate expression.
    const DB::ActionsDAG & filter;
    /// Exception which will be set during EnginePredicate execution.
    /// Exceptions cannot be rethrown as it will cause
    /// panic from rust and server terminate.
    std::exception_ptr & exception;

    static uintptr_t visitPredicate(void * data, ffi::KernelExpressionVisitorState * state);
};

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter, std::exception_ptr & exception);
}

#endif
