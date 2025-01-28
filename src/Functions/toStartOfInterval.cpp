#include <Functions/toStartOfIntervalBase.h>

namespace DB
{

class FunctionToStartOfInterval : public FunctionToStartOfIntervalBase
{
public:
    static constexpr auto name = "toStartOfInterval";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToStartOfInterval>(); }

    String getName() const override { return name; }

    ResultType decideReturnType(const DataTypeInterval * interval_type) const override
    {
        /// Determine result type for default overload (no origin)
        switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
        {
            case IntervalKind::Kind::Nanosecond:
            case IntervalKind::Kind::Microsecond:
            case IntervalKind::Kind::Millisecond:
                return ResultType::DateTime64;
            case IntervalKind::Kind::Second:
            case IntervalKind::Kind::Minute:
            case IntervalKind::Kind::Hour:
            case IntervalKind::Kind::Day: /// weird why Day leads to DateTime but too afraid to change it
                return ResultType::DateTime;
            case IntervalKind::Kind::Week:
            case IntervalKind::Kind::Month:
            case IntervalKind::Kind::Quarter:
            case IntervalKind::Kind::Year:
                return ResultType::Date;
        }
    }
};

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval>();
    factory.registerAlias("time_bucket", "toStartOfInterval", FunctionFactory::Case::Insensitive);
    factory.registerAlias("date_bin", "toStartOfInterval", FunctionFactory::Case::Insensitive);
}

}
