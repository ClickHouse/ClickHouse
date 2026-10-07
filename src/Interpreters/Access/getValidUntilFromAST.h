#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <ctime>
#include <optional>

namespace DB
{
    /// The lower bound of an accepted absolute `VALID UNTIL` deadline coming from a query.
    /// `CREATE`/`ALTER USER ... VALID UNTIL` rejects a deadline earlier than this: best-effort date/time
    /// parsing of an implausibly ancient date is unreliable (see the year-`0000` handling in
    /// `getValidUntilFromAST`), and such a value is far more likely a mistake than an intentional
    /// "already expired" marker - any post-epoch past date, or `VALID FOR` a negative interval, expresses
    /// that. A representable pre-epoch deadline within `[MIN_VALID_UNTIL_TIME, 1970-01-01)` is accepted and
    /// normalized to the smallest expired instant (`1970-01-01 00:00:01`), so it is stored as a plain Unix
    /// timestamp that every reader - including an older or downgraded server - interprets fail-closed (a
    /// pre-1970 datetime string would instead be resolved to `0`, the "no expiration" sentinel, by an older
    /// reader). This bound applies to query input only; a deadline earlier than it that an older release
    /// already persisted is normalized fail-closed (not rejected) when deserialized, so upgrading does not
    /// skip such a user - see `getValidUntilFromAST`.
    constexpr time_t MIN_VALID_UNTIL_TIME = -2208988800; /// 1900-01-01 00:00:00 UTC

    /// The upper bound of a stored `valid_until` deadline: the latest instant whose local rendering
    /// stays within year 9999 in EVERY time zone. The deadline is displayed (`SHOW CREATE USER`,
    /// `system.users`) in the server or session time zone, and `DateLUT` clamps a local year above
    /// `9999` back to `9999-12-31 23:59:59`. So the bound cannot be the latest UTC instant of year 9999
    /// (`253402300799`): on a positive-offset node (up to UTC+14:00, e.g. `Pacific/Kiritimati`) that
    /// instant falls into local year 10000 and would be displayed clamped - earlier than the deadline
    /// the authentication check actually enforces, so the credential would stay valid longer than shown.
    /// Subtracting the largest UTC offset in the time-zone database (+14:00) makes every stored deadline
    /// display exactly on every node, which matters because an access entity created on one server can
    /// be rendered on another (replicated access storage, `ON CLUSTER`) or under any `session_timezone`.
    /// A deadline beyond this bound is rejected when it comes from a query and clamped down (fail-closed:
    /// the credential expires earlier, never later) when an already-stored entity is deserialized - see
    /// `getValidUntilFromAST`. The `VALID FOR` path saturates at the same bound.
    constexpr time_t MAX_VALID_UNTIL_TIME = 253402250399; /// 9999-12-31 09:59:59 UTC = 9999-12-31 23:59:59 in UTC+14:00

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
