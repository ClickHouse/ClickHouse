#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <ctime>
#include <optional>

namespace DB
{
    /// Returns the current wall-clock time in seconds. When resolving `VALID FOR <interval>`, sample it
    /// once per `CREATE`/`ALTER USER` statement and pass it to every `getValidUntilFromAST` call, so that
    /// all `VALID FOR` clauses in the same query resolve against the same reference point, instead of each
    /// clause taking its own (slightly later) sample.
    time_t getCurrentTime();

    /// Formats a resolved `valid_until` deadline as a date-time string with an explicit `UTC` suffix,
    /// e.g. `2026-08-12 15:00:00 UTC`. Use it whenever the deadline is serialized to be parsed back by
    /// another server (`ON CLUSTER` distribution, replicated or disk access storage): the parser honours
    /// the explicit time zone, so every server obtains the same instant, whereas a bare local-time string
    /// would be reinterpreted in each server's own default time zone.
    String formatValidUntilInUTC(time_t valid_until);

    /// Computes the absolute deadline (as a `time_t`) for a `VALID UNTIL`/`VALID FOR` clause.
    /// The input AST is not modified.
    /// When `is_interval` is true, `valid_until` is an interval expression coming from
    /// `VALID FOR <interval>`, and the deadline is `now` plus the interval, evaluated at
    /// call time (which happens during query execution). Otherwise `valid_until` is a
    /// `VALID UNTIL` value that is parsed as a date/time. A returned value of `0` means "infinity".
    /// When `now` is provided, it is used as the reference time for `VALID FOR <interval>`; otherwise
    /// the current time is sampled inside this call.
    time_t getValidUntilFromAST(const ASTPtr & valid_until, ContextPtr context, bool is_interval = false, std::optional<time_t> now = std::nullopt);
}
