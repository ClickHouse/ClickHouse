#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Common/Exception.h>
#include <delta_kernel_ffi.hpp>

namespace DB
{
class ActionsDAG;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
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
        std::exception_ptr & exception_,
        DB::ContextPtr context_)
        : filter(filter_)
        , exception(exception_)
        , context(context_)
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
    DB::ContextPtr getContext() const { return context; }

private:
    const LoggerPtr log = getLogger("EnginePredicate");

    /// Predicate expression.
    const DB::ActionsDAG & filter;
    /// Exception which will be set during EnginePredicate execution.
    /// Exceptions cannot be rethrown as it will cause
    /// panic from rust and server terminate.
    std::exception_ptr & exception;
    /// Context for accessing settings
    DB::ContextPtr context;

    static uintptr_t visitPredicate(void * data, ffi::KernelExpressionVisitorState * state);
};

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter, std::exception_ptr & exception, DB::ContextPtr context);
}

#endif
