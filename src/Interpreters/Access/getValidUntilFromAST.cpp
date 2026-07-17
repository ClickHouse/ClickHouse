#include <Interpreters/Access/getValidUntilFromAST.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/DateLUT.h>
#include <Common/ErrnoException.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <ctime>
#include <string_view>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int CANNOT_CLOCK_GETTIME;
    }

    time_t getCurrentTime()
    {
        timespec spec{};
        if (clock_gettime(CLOCK_REALTIME, &spec))
            throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");
        return spec.tv_sec;
    }

    String formatValidUntilInUTC(time_t valid_until)
    {
        WriteBufferFromOwnString out;
        writeDateTimeText(valid_until, out, DateLUT::instance("UTC"));
        writeCString(" UTC", out);
        return out.str();
    }

    time_t getValidUntilFromAST(const ASTPtr & valid_until, ContextPtr context, bool is_interval, std::optional<time_t> now)
    {
        /// The input AST is never modified: `evaluateConstantExpression*` folds a clone, and the
        /// rewrites below only build new nodes around this local copy of the pointer.
        ASTPtr ast = valid_until;

        if (is_interval)
        {
            /// `VALID FOR <interval>` is a shortcut for `VALID UNTIL now + <interval>`. We compute the
            /// deadline here, at query execution time.
            /// The current time is injected as a literal instead of using `now`, because `now` is
            /// non-deterministic and would not be folded to a constant by `evaluateConstantExpression`.
            /// `toDateTime64` (rather than `toDateTime`) is used so that large intervals do not overflow
            /// the year-2106 boundary of `DateTime` and instead saturate at the `DateTime64` bounds.
            if (!context)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "VALID FOR requires a query context to evaluate the interval");

            /// `VALID FOR` accepts only interval expressions (e.g. `INTERVAL 1 DAY` or
            /// `INTERVAL 1 DAY + INTERVAL 2 HOUR`), because the deadline is computed as `now` plus this value.
            /// The parser accepts an arbitrary expression, so a bare number such as `VALID FOR 365` would
            /// otherwise be resolved by `plus(DateTime64, Number)` as `addSeconds`, silently setting a
            /// 365-second lifetime instead of failing. Reject anything whose folded type is neither
            /// an `Interval` nor a tuple of `Interval`s. The tuple form appears because the type system
            /// has no single type for a sum of intervals of different kinds, so `FunctionBinaryArithmetic`
            /// folds e.g. `INTERVAL 1 DAY + INTERVAL 2 HOUR` into `Tuple(IntervalDay, IntervalHour)`
            /// (a sum of intervals of the same kind stays a plain `Interval`).
            const auto interval_type = evaluateConstantExpression(ast, context).second;
            bool is_interval_type = WhichDataType(*interval_type).isInterval();
            if (!is_interval_type && WhichDataType(*interval_type).isTuple())
            {
                const auto & elements = assert_cast<const DataTypeTuple &>(*interval_type).getElements();
                is_interval_type = !elements.empty()
                    && std::all_of(elements.begin(), elements.end(), [](const auto & element) { return WhichDataType(*element).isInterval(); });
            }
            if (!is_interval_type)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "VALID FOR expects an interval expression (for example, INTERVAL 1 DAY), but got an expression of type {}. "
                    "Use VALID UNTIL to specify an absolute point in time",
                    interval_type->getName());

            /// Use the reference time supplied by the caller when available, so that all `VALID FOR`
            /// clauses of one statement resolve against a single `now`; otherwise sample it here.
            const time_t now_seconds = now.has_value() ? *now : getCurrentTime();

            auto now_literal = make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(now_seconds)));
            auto scale_literal = make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(0)));
            ast = makeASTFunction("toUnixTimestamp64Second",
                makeASTFunction("plus", makeASTFunction("toDateTime64", now_literal, scale_literal), ast));

            /// The deadline is extracted numerically (`toUnixTimestamp64Second` scales any `DateTime64`
            /// to whole seconds) rather than by round-tripping through a datetime string, because the
            /// values saturated at the `DateTime64` bounds (years 0000 and 9999) do not all survive
            /// datetime-text parsing.
            /// Every deadline in the past means the same thing - the credential is already expired - so
            /// a pre-epoch deadline (e.g. a saturated huge negative interval) is clamped to the smallest
            /// expired instant, `1970-01-01 00:00:01` (the value 0 would mean "no expiration"). This also
            /// keeps every deadline produced by `VALID FOR` exactly representable in the stored access
            /// entity encoding (see `AuthenticationData::toAST`).
            const auto deadline = evaluateConstantExpression(ast, context).first;
            return std::max<time_t>(static_cast<time_t>(deadline.safeGet<Int64>()), 1);
        }

        if (context)
            ast = evaluateConstantExpressionAsLiteral(ast, context);

        const String valid_until_str = checkAndGetLiteralArgument<String>(ast, "valid_until");

        if (valid_until_str == "infinity")
            return 0;

        time_t time = 0;
        ReadBufferFromString in(valid_until_str);

        /// No query context means we are deserializing a stored access entity (`ATTACH USER` coming
        /// from replicated or disk access storage). Post-epoch deadlines are serialized as zero-padded
        /// Unix timestamp strings (see `AuthenticationData::toAST`), which denote the same instant
        /// regardless of the server time zone and are read here as plain integers. Otherwise the value
        /// is in the `YYYY-MM-DD hh:mm:ss[ UTC]` datetime form: a `VALID UNTIL` value coming from a
        /// query, a stored pre-1970 deadline (which carries an explicit `UTC` suffix), an entity written
        /// by an older version (a bare local-time string, resolved in the server time zone, as before),
        /// or a hand-edited stored definition.
        ///
        /// Both forms then go through the same bounds checks below: a hand-edited definition must fail
        /// to load, or resolve to an already-expired credential, rather than silently resolve to a
        /// different deadline or - worse - to the `0 == no expiration` sentinel, the same way
        /// `CREATE`/`ALTER USER` reject or normalize the value at query time. (The server still starts:
        /// the directory scan skips a broken definition with a logged error, and a lazy per-entity read
        /// reports the error to the operation that touches it.)
        if (!context && std::all_of(valid_until_str.begin(), valid_until_str.end(), isNumericASCII))
        {
            readIntText(time, in);
        }
        else
        {
            /// `parseDateTimeBestEffort` cannot represent an explicit year of `0000`: internally, a
            /// year field of `0` means "not specified", so it is silently replaced with the current
            /// (or previous) year instead of being kept as-is - see the `!year` fallback in
            /// `parseDateTimeBestEffortImpl` (src/IO/parseDateTimeBestEffort.cpp). That would make the
            /// bound check below pass on a deadline the caller never asked for. The documented `VALID
            /// UNTIL` syntax (docs/en/sql-reference/statements/create/user.md) is always a delimited
            /// date starting with the year, so a leading `0000` followed by a non-digit unambiguously
            /// means the year field itself is `0000`; reject it explicitly rather than let it round-trip
            /// through the "year omitted" fallback.
            /// `parseDateTimeBestEffort` skips leading spaces before it reads the year field (see the
            /// space handling in `parseDateTimeBestEffortImpl`), so the check must skip them too;
            /// otherwise `VALID UNTIL ' 0000-01-01 00:00:00 UTC'` would slip past it and still reach the
            /// "year omitted" fallback.
            std::string_view year_field = valid_until_str;
            while (year_field.starts_with(' '))
                year_field.remove_prefix(1);
            if (year_field.starts_with("0000") && (year_field.size() == 4 || !isNumericASCII(year_field[4])))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "VALID UNTIL deadline is too far in the past, the earliest supported deadline is 1900-01-01 00:00:00 UTC");

            /// Best-effort parsing honours an explicit time zone in the string, e.g. the `UTC` suffix
            /// produced by the `ON CLUSTER` rewrite (see `formatValidUntilInUTC`).
            const auto & time_zone = DateLUT::instance("");
            const auto & utc_time_zone = DateLUT::instance("UTC");

            parseDateTimeBestEffort(time, in, time_zone, utc_time_zone);
        }

        /// Deadlines before this bound cannot be represented exactly in the stored access entity
        /// encoding, so accepting them here would only be discovered later, as a silently clamped
        /// value after a restart or replication round-trip (see `AuthenticationData::toAST`). A stored
        /// numeric deadline is always non-negative, so for the `ATTACH` numeric branch this only guards
        /// against a hand-edited out-of-range value, which must fail to load rather than silently
        /// weaken expiration.
        if (time < MIN_VALID_UNTIL_TIME)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "VALID UNTIL deadline is too far in the past, the earliest supported deadline is 1900-01-01 00:00:00 UTC");

        /// Symmetrically, a deadline after the latest `DateLUT`-representable instant would be displayed
        /// clamped to `9999-12-31 23:59:59` by `SHOW CREATE USER` / `AuthenticationData::toAST` while the
        /// authentication check keeps enforcing the larger stored value, so the credential would outlive
        /// the shown deadline. This is reachable through an explicit time-zone offset that crosses the
        /// year-9999 boundary, e.g. `VALID UNTIL '9999-12-31 23:59:59 -01:00'`. Reject it rather than
        /// silently clamp on display. (The `VALID FOR` path saturates at this same bound via
        /// `toDateTime64`, so it never reaches this branch with a larger value.)
        if (time > MAX_VALID_UNTIL_TIME)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "VALID UNTIL deadline is too far in the future, the latest supported deadline is 9999-12-31 23:59:59 UTC");

        /// A non-`infinity` deadline that parses to exactly the Unix epoch (`time == 0`) collides with the
        /// sentinel value `0`, which means "no expiration": `AuthenticationData::toAST` serializes the
        /// deadline only when it is non-zero, and the authentication check skips the expiration test when
        /// `valid_until` is `0` (see `IAccessStorage::areCredentialsValid`). Only the literal `infinity`
        /// (handled above) is meant to disable expiration; `VALID UNTIL '1970-01-01 00:00:00'` is a real
        /// deadline in the past, so it is normalized to the smallest expired instant, `1970-01-01 00:00:01`,
        /// the same way the `VALID FOR` path clamps a pre-epoch deadline. A deadline strictly before the
        /// epoch is negative, stays as is (stored in datetime form), and remains distinct from `0`. This
        /// also covers a hand-edited `ATTACH USER ... VALID UNTIL '0'` (or `'0000000000'`), which the
        /// numeric branch above reads as `0`: it becomes an already-expired credential rather than a
        /// non-expiring one.
        if (time == 0)
            return 1;

        return time;
    }
}
