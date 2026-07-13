#include <Interpreters/Access/getValidUntilFromAST.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/ErrnoException.h>

#include <ctime>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int CANNOT_CLOCK_GETTIME;
    }

    time_t sampleValidForBaseTime()
    {
        timespec spec{};
        if (clock_gettime(CLOCK_REALTIME, &spec))
            throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");
        return spec.tv_sec;
    }

    time_t getValidUntilFromAST(ASTPtr valid_until, ContextPtr context, bool is_interval, std::optional<time_t> now)
    {
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

            /// Use the reference time supplied by the caller when available, so that all `VALID FOR`
            /// clauses of one statement resolve against a single `now`; otherwise sample it here.
            const time_t now_seconds = now.has_value() ? *now : sampleValidForBaseTime();

            auto now_literal = make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(now_seconds)));
            auto scale_literal = make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(0)));
            valid_until = makeASTFunction("toString",
                makeASTFunction("plus", makeASTFunction("toDateTime64", now_literal, scale_literal), valid_until));
        }

        if (context)
            valid_until = evaluateConstantExpressionAsLiteral(valid_until, context);

        const String valid_until_str = checkAndGetLiteralArgument<String>(valid_until, "valid_until");

        if (valid_until_str == "infinity")
            return 0;

        time_t time = 0;
        ReadBufferFromString in(valid_until_str);

        if (context)
        {
            const auto & time_zone = DateLUT::instance("");
            const auto & utc_time_zone = DateLUT::instance("UTC");

            parseDateTimeBestEffort(time, in, time_zone, utc_time_zone);
        }
        else
        {
            readDateTimeText(time, in);
        }

        return time;
    }
}
