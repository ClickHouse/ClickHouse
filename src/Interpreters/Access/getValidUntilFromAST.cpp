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
#include <Common/assert_cast.h>

#include <algorithm>
#include <ctime>

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
            /// deadline here, at query execution time, and turn it into a string so that it flows through
            /// the regular `VALID UNTIL` handling below - the stored result is thus in the `VALID UNTIL` form.
            /// The current time is injected as a literal instead of using `now`, because `now` is
            /// non-deterministic and would not be folded to a constant by `evaluateConstantExpression`.
            /// `toDateTime64` (rather than `toDateTime`) is used so that large intervals do not overflow
            /// the year-2106 boundary of `DateTime` and instead saturate at the `DateTime64` upper bound.
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
            ast = makeASTFunction("toString",
                makeASTFunction("plus", makeASTFunction("toDateTime64", now_literal, scale_literal), ast));
        }

        if (context)
            ast = evaluateConstantExpressionAsLiteral(ast, context);

        const String valid_until_str = checkAndGetLiteralArgument<String>(ast, "valid_until");

        if (valid_until_str == "infinity")
            return 0;

        time_t time = 0;
        ReadBufferFromString in(valid_until_str);

        if (context)
        {
            /// Best-effort parsing honours an explicit time zone in the string, e.g. the `UTC` suffix
            /// produced by the `ON CLUSTER` rewrite (see `formatValidUntilInUTC`).
            const auto & time_zone = DateLUT::instance("");
            const auto & utc_time_zone = DateLUT::instance("UTC");

            parseDateTimeBestEffort(time, in, time_zone, utc_time_zone);
        }
        else
        {
            /// No query context means we are deserializing a stored access entity (`ATTACH USER` coming
            /// from replicated or disk access storage). Deadlines are serialized as Unix timestamp
            /// strings (see `AuthenticationData::toAST`), which denote the same instant regardless of
            /// the server time zone. Entities written by older versions store a bare local-time string
            /// instead, which is resolved in the server time zone, as before. `readDateTimeText` handles
            /// the non-negative timestamps and the datetime form, but not pre-1970 (negative) timestamps,
            /// which are read explicitly here.
            if (valid_until_str.starts_with('-'))
                readIntText(time, in);
            else
                readDateTimeText(time, in);
        }

        return time;
    }
}
