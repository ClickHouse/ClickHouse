#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatFactory.h>

#include <delta_kernel_ffi.hpp>

namespace DB
{
class ActionsDAG;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
}

namespace DeltaLake
{

/// EnginePredicate used in ::scan.
/// A predicate which allows to implement internal delta-kernel-rs filtering
/// based on statistics and partitions pruning.
class EnginePredicate : public ::EnginePredicate
{
    friend struct EngineIteratorData;
public:
    explicit EnginePredicate( // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        const DB::ActionsDAG & filter_,
        std::exception_ptr & exception_,
        DB::ContextPtr context_,
        const DB::NamesAndTypesList & table_schema_)
        : filter(filter_)
        , exception(exception_)
        , context(context_)
        , format_settings(context_ ? DB::getFormatSettings(context_) : DB::FormatSettings{})
        , table_schema(table_schema_)
    {
        predicate = this;
        visitor = &visitPredicate;
    }

    [[nodiscard]] bool hasException() const { return exception != nullptr; }

    void setException(std::exception_ptr exception_)
    {
        DB::tryLogException(exception_, log);
        if (!exception)
            exception = exception_;
    }

    [[nodiscard]] const DB::ActionsDAG & getFilterDAG() const { return filter; }
    [[nodiscard]] DB::ContextPtr getContext() const { return context; }
    [[nodiscard]] const DB::FormatSettings & getFormatSettings() const { return format_settings; }

    /// Whether a column of the filter can be referenced in the predicate by its name.
    /// delta-kernel splits the name by dots into a path of struct fields and throws
    /// `Predicate references unknown column` if the path is not in the schema of the table.
    /// So it has to be a column of the table or a field of its struct (`t.x`), but not a subcolumn
    /// that exists only in ClickHouse (e.g. `c.null`, `arr.size0`, `m.keys`): the kernel would
    /// take it for a field of a struct. The comparisons with such columns are not translated.
    [[nodiscard]] bool canReferenceColumn(const std::string & name) const;

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
    const DB::FormatSettings format_settings;
    /// The logical schema of the table.
    const DB::NamesAndTypesList table_schema;

    static uintptr_t visitPredicate(void * data, ::KernelExpressionVisitorState * state);
};

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter,
    std::exception_ptr & exception,
    DB::ContextPtr context,
    const DB::NamesAndTypesList & table_schema);
}

#endif
