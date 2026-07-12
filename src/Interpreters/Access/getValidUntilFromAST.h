#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <ctime>

namespace DB
{
    /// Computes the absolute deadline (as a `time_t`) for a `VALID UNTIL`/`VALID FOR` clause.
    /// When `is_interval` is true, `valid_until` is an interval expression coming from
    /// `VALID FOR <interval>`, and the deadline is `now` plus the interval, evaluated at
    /// call time (which happens during query execution). Otherwise `valid_until` is a
    /// `VALID UNTIL` value that is parsed as a date/time. A returned value of `0` means "infinity".
    time_t getValidUntilFromAST(ASTPtr valid_until, ContextPtr context, bool is_interval = false);
}
