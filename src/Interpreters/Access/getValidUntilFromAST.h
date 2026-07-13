#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <ctime>
#include <optional>

namespace DB
{
    /// Samples the current wall-clock time (in seconds) used as the reference `now` when resolving
    /// `VALID FOR <interval>`. Sample it once per `CREATE`/`ALTER USER` statement and pass it to every
    /// `getValidUntilFromAST` call so that all `VALID FOR` clauses in the same query share one deadline
    /// base, instead of each clause taking its own (slightly later) sample.
    time_t sampleValidForBaseTime();

    /// Computes the absolute deadline (as a `time_t`) for a `VALID UNTIL`/`VALID FOR` clause.
    /// When `is_interval` is true, `valid_until` is an interval expression coming from
    /// `VALID FOR <interval>`, and the deadline is `now` plus the interval, evaluated at
    /// call time (which happens during query execution). Otherwise `valid_until` is a
    /// `VALID UNTIL` value that is parsed as a date/time. A returned value of `0` means "infinity".
    /// When `now` is provided, it is used as the reference time for `VALID FOR <interval>`; otherwise
    /// the current time is sampled inside this call.
    time_t getValidUntilFromAST(ASTPtr valid_until, ContextPtr context, bool is_interval = false, std::optional<time_t> now = std::nullopt);
}
