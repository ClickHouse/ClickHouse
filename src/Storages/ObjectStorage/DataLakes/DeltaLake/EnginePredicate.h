#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Formats/FormatFactory.h>

#include <delta_kernel_ffi.hpp>

#include <optional>
#include <unordered_map>

namespace DB
{
class ActionsDAG;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
}

namespace DeltaLake
{

/// Delta type behind a top-level column that ClickHouse reads as `DateTime64(6)`;
/// selects the kernel literal a comparison with that column is translated to.
enum class DeltaTimestampKind : uint8_t
{
    Timestamp,
    TimestampNtz,
};
using DeltaTimestampColumns = std::unordered_map<String, DeltaTimestampKind>;

/// EnginePredicate used in ffi::scan.
/// A predicate which allows to implement internal delta-kernel-rs filtering
/// based on statistics and partitions pruning.
class EnginePredicate : public ffi::EnginePredicate
{
    friend struct EngineIteratorData;
public:
    explicit EnginePredicate( // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        const DB::ActionsDAG & filter_,
        std::exception_ptr & exception_,
        DB::ContextPtr context_,
        DeltaTimestampColumns timestamp_columns_)
        : filter(filter_)
        , exception(exception_)
        , context(context_)
        , format_settings(makeComparisonFormatSettings(context_))
        , timestamp_columns(std::move(timestamp_columns_))
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
    const DB::FormatSettings & getFormatSettings() const { return format_settings; }

    std::optional<DeltaTimestampKind> getTimestampKind(const String & column_name) const
    {
        auto it = timestamp_columns.find(column_name);
        if (it == timestamp_columns.end())
            return std::nullopt;
        return it->second;
    }

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
    /// The settings `FunctionComparison` converts a literal with, so that pruning sees the same value.
    const DB::FormatSettings format_settings;
    /// Top-level columns backed by a Delta `timestamp` / `timestamp_ntz`, by logical name.
    const DeltaTimestampColumns timestamp_columns;

    static uintptr_t visitPredicate(void * data, ffi::KernelExpressionVisitorState * state);

    static DB::FormatSettings makeComparisonFormatSettings(const DB::ContextPtr & context_);
};

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter, std::exception_ptr & exception, DB::ContextPtr context,
    DeltaTimestampColumns timestamp_columns);
}

#endif
