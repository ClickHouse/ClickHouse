#include <Functions/toStartOfIntervalBase.h>

namespace DB
{

class FunctionToStartOfIntervalAllowNegative : public FunctionToStartOfIntervalBase
{
public:
    static constexpr auto name = "toStartOfIntervalAllowNegative";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToStartOfIntervalAllowNegative>(); }

    String getName() const override { return name; }

    ResultType decideReturnType(const DataTypeInterval * interval_type) const override
    {
        /// Determine result type for default overload (no origin)
        switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
        {
            case IntervalKind::Kind::Nanosecond:
            case IntervalKind::Kind::Microsecond:
            case IntervalKind::Kind::Millisecond:
            case IntervalKind::Kind::Second:
            case IntervalKind::Kind::Minute:
            case IntervalKind::Kind::Hour:
            case IntervalKind::Kind::Day: /// weird why Day leads to DateTime64 but too afraid to change it
                return ResultType::DateTime64;
            case IntervalKind::Kind::Week:
            case IntervalKind::Kind::Month:
            case IntervalKind::Kind::Quarter:
            case IntervalKind::Kind::Year:
                return ResultType::Date32;
        }
    }
};

REGISTER_FUNCTION(toStartOfIntervalAllowNegative)
{
    factory.registerFunction<FunctionToStartOfIntervalAllowNegative>(
        FunctionDocumentation{
            .description=R"(Same logic as toStartOfInterval but forced to return Date32 or DateTime64.)",
            .syntax=R"(toStartOfIntervalAllowNegative(value, INTERVAL x unit, ...))",
            .arguments={{"value","Date or DateTime or DateTime64"}, {"interval","..."}},
            .examples={{"example","SELECT toStartOfIntervalAllowNegative(toDateTime64('1960-03-03 12:55:55', 2), INTERVAL 1 Hour);","1960-03-03 12:00:00"}}
        }
    );
    factory.registerAlias("time_bucket_allow_negative", "toStartOfIntervalAllowNegative", FunctionFactory::Case::Insensitive);
    factory.registerAlias("date_bin_allow_negative", "toStartOfIntervalAllowNegative", FunctionFactory::Case::Insensitive);
}

}
