#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLPlan.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** `kqlParameterCast(value, type, parameter)` - enforces the declared type of a KQL function
  * parameter at the call boundary.
  *
  * A KQL function declares its scalar parameters and its tabular parameters' columns with
  * types (`let f = (a: long) { a }`), and Kusto checks the arguments against them statically.
  * The KQL parser cannot: it lowers to SQL without ever seeing a schema, so the argument
  * types are only known once the query is analyzed. This resolver is that deferred check -
  * the parser wraps every argument in it, and at analysis time it rejects an argument whose
  * type does not belong to the declared KQL type, then delegates to `accurateCast`, which
  * widens losslessly and makes a value that does not fit (an `int` overflow, say) a per-row
  * error rather than silent truncation.
  *
  * `type` is the declared KQL type name and `parameter` describes the parameter for the error
  * message; both must be constant strings. A `NULL` literal argument is a typed null of every
  * declared type. This function backs typed parameters when `dialect = 'kusto'`. It is not
  * meant to be called directly from SQL.
  */
class FunctionKQLParameterCastOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlParameterCast";

    explicit FunctionKQLParameterCastOverloadResolver(ContextPtr context_)
        : WithContext(context_)
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLParameterCastOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override { return delegate(arguments); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override { return delegate(arguments)->getResultType(); }

private:
    String constantString(const ColumnsWithTypeAndName & arguments, size_t index, const char * what) const
    {
        const ColumnPtr & column = arguments[index].column;
        if (!column || !isColumnConst(*column) || !isString(removeNullable(arguments[index].type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires a constant string as {}", getName(), what);
        return assert_cast<const ColumnConst &>(*column).getValue<String>();
    }

    FunctionBasePtr delegate(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 3 arguments", getName());

        const String kql_type = constantString(arguments, 1, "the declared type");
        const String parameter = constantString(arguments, 2, "the parameter description");

        const DataTypePtr value_type = removeNullable(removeLowCardinality(arguments[0].type));

        /// A timespan is not a `CAST` target: an `Interval` argument of another fixed-length
        /// kind converts by integer tick arithmetic instead.
        if (kql_type == "timespan" || kql_type == "time")
        {
            if (!isNothing(value_type))
            {
                const auto * interval = typeid_cast<const DataTypeInterval *>(value_type.get());
                if (!interval || !interval->getKind().isFixedLength())
                    reject(kql_type, parameter, arguments[0].type);
                if (interval->getKind() != IntervalKind::Kind::Nanosecond)
                    return convertIntervalToNanoseconds(arguments, interval->getKind());
            }
            return castTo(arguments, "IntervalNanosecond");
        }

        const String & target = targetOf(kql_type, parameter, arguments[0].type, value_type);
        return castTo(arguments, target);
    }

    [[noreturn]] void reject(const String & kql_type, const String & parameter, const DataTypePtr & actual) const
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} is declared '{}', but the argument has type {}", parameter, kql_type, actual->getName());
    }

    /// The ClickHouse type the declared KQL type enforces, after checking that the argument's
    /// type belongs to it. The check is by type family: whether the value fits is per row,
    /// which `accurateCast` handles.
    const String & targetOf(const String & kql_type, const String & parameter, const DataTypePtr & original, const DataTypePtr & value_type) const
    {
        static const String string_target = "String";
        static const String long_target = "Int64";
        static const String int_target = "Int32";
        static const String real_target = "Float64";
        static const String bool_target = "Bool";
        static const String datetime_target = "DateTime64(7, 'UTC')";
        static const String decimal_target = "Decimal128(20)";
        static const String uuid_target = "UUID";

        /// A `NULL` literal is a typed null of whatever the parameter declares.
        const bool nothing = isNothing(value_type);

        if (kql_type == "string")
        {
            if (nothing || isStringOrFixedString(value_type))
                return string_target;
        }
        else if (kql_type == "long")
        {
            if (nothing || isNativeInteger(value_type))
                return long_target;
        }
        else if (kql_type == "int")
        {
            if (nothing || isNativeInteger(value_type))
                return int_target;
        }
        else if (kql_type == "real" || kql_type == "double")
        {
            if (nothing || isNativeNumber(value_type))
                return real_target;
        }
        else if (kql_type == "bool" || kql_type == "boolean")
        {
            if (nothing || isBool(value_type) || WhichDataType(value_type).isUInt8())
                return bool_target;
        }
        else if (kql_type == "datetime" || kql_type == "date")
        {
            if (nothing || isDateOrDate32OrDateTimeOrDateTime64(value_type))
                return datetime_target;
        }
        else if (kql_type == "decimal")
        {
            if (nothing || isNativeInteger(value_type) || isDecimal(value_type))
                return decimal_target;
        }
        else if (kql_type == "guid" || kql_type == "uuid")
        {
            if (nothing || isUUID(value_type))
                return uuid_target;
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} does not know the KQL type '{}'", getName(), kql_type);
        }

        reject(kql_type, parameter, original);
    }

    /// `accurateCast` over the value; the two metadata arguments feed no step. A nullable
    /// argument keeps its nullability, and a `NULL` literal becomes a typed null. The value
    /// slot drops `LowCardinality`, because the default implementation hands the plan an
    /// already-converted full column.
    FunctionBasePtr castTo(const ColumnsWithTypeAndName & arguments, const String & target) const
    {
        const DataTypePtr value_type = removeLowCardinality(arguments[0].type);
        const bool nullable = value_type->isNullable() || isNothing(removeNullable(value_type));

        KQLPlanBuilder plan(getContext());
        const size_t value_slot = plan.argument(value_type);
        plan.argument(arguments[1].type);
        plan.argument(arguments[2].type);
        const size_t target_slot
            = plan.constant(std::make_shared<DataTypeString>(), Field(nullable ? "Nullable(" + target + ")" : target));
        plan.step("accurateCast", {value_slot, target_slot});
        return std::move(plan).finish(name, arguments);
    }

    /// A fixed-length interval of another kind is an integer count of that kind's ticks: the
    /// argument slot retypes to `Int64`, the count scales to nanoseconds, and the last step
    /// makes the result an interval again.
    FunctionBasePtr convertIntervalToNanoseconds(const ColumnsWithTypeAndName & arguments, const IntervalKind & kind) const
    {
        const DataTypePtr ticks_type = arguments[0].type->isNullable() ? makeNullable(std::make_shared<DataTypeInt64>())
                                                                       : std::static_pointer_cast<const IDataType>(std::make_shared<DataTypeInt64>());

        KQLPlanBuilder plan(getContext());
        const size_t value_slot = plan.argument(ticks_type);
        plan.argument(arguments[1].type);
        plan.argument(arguments[2].type);
        const size_t scale = plan.constant(std::make_shared<DataTypeInt64>(), Field(kind.toAvgNanoseconds()));
        const size_t nanoseconds = plan.step("multiply", {value_slot, scale});
        plan.step("toIntervalNanosecond", {nanoseconds});
        return std::move(plan).finish(name, arguments);
    }
};

}

REGISTER_FUNCTION(KQLParameterCast)
{
    FunctionDocumentation documentation{
        .description = R"(
Checks a value against a declared Kusto Query Language type and converts it to that type's
ClickHouse carrier: an argument whose type does not belong to the declared KQL type is
rejected during analysis, and a value that does not fit (an `int` overflow, say) is a per-row
error. The second argument is the declared KQL type name and the third describes the
parameter for the error message; both must be constant strings.

This function backs typed function parameters when `dialect = 'kusto'`. It is not meant to be
called directly from SQL.
)",
        .syntax = "kqlParameterCast(value, type, parameter)",
        .arguments
        = {{"value", "The argument a KQL function call supplies."},
           {"type", "The declared KQL type name, a constant string."},
           {"parameter", "A description of the parameter, a constant string used in the error message."}},
        .returned_value = {"`value` as the declared type's ClickHouse carrier."},
        .examples
        = {{"long", "SELECT kqlParameterCast(5, 'long', 'the parameter') AS x, toTypeName(x)", "5\tInt64"},
           {"real", "SELECT kqlParameterCast(5, 'real', 'the parameter') AS x, toTypeName(x)", "5\tFloat64"}},
        .introduced_in = {26, 9},
        .category = FunctionDocumentation::Category::TypeConversion,
    };

    factory.registerFunction(
        FunctionKQLParameterCastOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLParameterCastOverloadResolver::create(std::move(context)); },
        documentation);
}

}
