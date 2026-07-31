#include <Common/FunctionDocumentation.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

/// Internal helper used by the `EXTRACT(CENTURY|DECADE|MILLENNIUM FROM ...)`
/// lowering. Those units are computed as arithmetic over `toYear(expr)`, and
/// `toYear` now accepts `IntervalYear` operands, which would silently produce
/// meaningless results like `EXTRACT(CENTURY FROM INTERVAL 5 YEAR) = 1`.
/// This struct is `ToYearImpl` re-registered under a name that
/// `IntervalKind::tryParseFromNameOfFunctionExtractTimePart` does not
/// recognise, so `FunctionDateOrDateTimeBase::acceptsIntervalArgument` returns
/// false for it and the base validator rejects `Interval` operands.
struct ToYearCalendarOnlyImpl : public ToYearImpl
{
    static constexpr auto name = "__toYearCalendarOnly";
};

}

using FunctionToYearCalendarOnly = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearCalendarOnlyImpl>;

REGISTER_FUNCTION(ToYearCalendarOnly)
{
    factory.registerFunction<FunctionToYearCalendarOnly>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
