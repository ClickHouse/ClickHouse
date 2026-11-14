#include <Interpreters/Access/getValidUntilFromAST.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/checkAndGetLiteralArgument.h>

namespace DB
{
    time_t getValidUntilFromAST(ASTPtr valid_until, ContextPtr context)
    {
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
