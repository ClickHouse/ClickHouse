#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <ctime>
#include <optional>

namespace DB
{
    /// The stored access entity encoding (see `AuthenticationData::toAST`) writes a pre-epoch deadline as
    /// a date-time string that older servers - whose `DateLUT` supports no year earlier than 1900 - can
    /// still parse in their own time zone. `CREATE`/`ALTER USER ... VALID UNTIL` therefore rejects deadlines
    /// before this bound: accepting them would make `SHOW CREATE USER` show a different (clamped) deadline
    /// after a restart or replication round-trip than the one that was originally specified.
    constexpr time_t MIN_VALID_UNTIL_TIME = -2208988800; /// 1900-01-01 00:00:00 UTC

    /// The upper bound is the latest instant `DateLUT` can represent and format. Accepting a deadline
    /// beyond it would make `SHOW CREATE USER` / `AuthenticationData::toAST` display a clamped
    /// (`9999-12-31 23:59:59`) deadline that is earlier than the one the authentication check actually
    /// enforces, so the credential would stay valid longer than shown. This is reachable with an explicit
    /// time-zone offset that pushes an otherwise in-range date past the ceiling, e.g.
    /// `VALID UNTIL '9999-12-31 23:59:59 -01:00'`.
    constexpr time_t MAX_VALID_UNTIL_TIME = 253402300799; /// 9999-12-31 23:59:59 UTC

    /// Returns the current wall-clock time in seconds. When resolving `VALID FOR <interval>`, sample it
    /// once per `CREATE`/`ALTER USER` statement and pass it to every `getValidUntilFromAST` call, so that
    /// all `VALID FOR` clauses in the same query resolve against the same reference point, instead of each
    /// clause taking its own (slightly later) sample.
    time_t getCurrentTime();

    /// Formats a resolved `valid_until` deadline as a date-time string with an explicit `UTC` suffix,
    /// e.g. `2026-08-12 15:00:00 UTC`. It is used when the deadline is embedded into a query distributed
    /// with `ON CLUSTER`: best-effort parsing honours the explicit time zone, so every replica obtains
    /// the same instant, whereas a bare local-time string would be reinterpreted in each replica's own
    /// default time zone. (Stored access entities serialize the deadline as a Unix timestamp string
    /// instead, which is also understood by older servers - see `AuthenticationData::toAST`.)
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
