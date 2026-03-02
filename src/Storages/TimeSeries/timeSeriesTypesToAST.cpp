#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    /// Adds a timezone to the list of argument of the function `toDateTime64()` or `toDateTime()`.
    boost::intrusive_ptr<ASTFunction> addTimeZone(boost::intrusive_ptr<ASTFunction> && ast, String timezone)
    {
        if (!timezone.empty())
            ast->arguments->children.push_back(make_intrusive<ASTLiteral>(std::move(timezone)));
        return ast;
    }
}


ASTPtr timeSeriesTimestampToAST(DateTime64 timestamp, const DataTypePtr & timestamp_data_type)
{
    if (isDateTime64(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        if (scale == 0)
            return timeSeriesTimestampASTCast(make_intrusive<ASTLiteral>(timestamp.value), timestamp_data_type);

        String str = toString(timestamp, scale);
        if (0 <= timestamp.value && timestamp.value < DecimalUtils::scaleMultiplier<Decimal64>(scale + 4))
        {
            /// For timestamps between 0 and 9999 we need to use expression toDateTime64(toDecimal64('timestamp', <scale>), <scale>)
            /// because otherwise it can be considered as a year (i.e. for example "1000" can be parsed as "year 1000" instead of
            /// "1000 seconds after January 1, 1970").
            /// TODO: Find a more elegant solution.
            return timeSeriesTimestampASTCast(makeASTFunction("toDecimal64", make_intrusive<ASTLiteral>(std::move(str)), make_intrusive<ASTLiteral>(scale)), timestamp_data_type);
        }

        return timeSeriesTimestampASTCast(make_intrusive<ASTLiteral>(std::move(str)), timestamp_data_type);
    }
    else if (isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        if (scale == 0)
            return timeSeriesTimestampASTCast(make_intrusive<ASTLiteral>(timestamp.value), timestamp_data_type);

        String str = toString(timestamp, scale);
        return timeSeriesTimestampASTCast(make_intrusive<ASTLiteral>(std::move(str)), timestamp_data_type);
    }
    else
    {
        return timeSeriesTimestampASTCast(make_intrusive<ASTLiteral>(timestamp.value), timestamp_data_type);
    }
}


ASTPtr timeSeriesDurationToAST(Decimal64 duration, const DataTypePtr & timestamp_data_type)
{
    if (isDateTime64(timestamp_data_type) || isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        String str = toString(duration, scale);
        return timeSeriesDurationASTCast(make_intrusive<ASTLiteral>(std::move(str)), timestamp_data_type);
    }
    else
    {
        return timeSeriesDurationASTCast(make_intrusive<ASTLiteral>(duration.value), timestamp_data_type);
    }
}


ASTPtr timeSeriesScalarToAST(Float64 value, const DataTypePtr & scalar_data_type)
{
    return timeSeriesScalarASTCast(make_intrusive<ASTLiteral>(value), scalar_data_type);
}


ASTPtr timeSeriesTimestampASTCast(ASTPtr && ast, const DataTypePtr & timestamp_data_type)
{
    WhichDataType which_data_type{timestamp_data_type};
    if (which_data_type.isDateTime64())
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        return addTimeZone(
            makeASTFunction("toDateTime64", std::move(ast), make_intrusive<ASTLiteral>(scale)), getDateTimeTimezone(*timestamp_data_type));
    }
    else if (which_data_type.isDecimal64())
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        return makeASTFunction("toDecimal64", std::move(ast), make_intrusive<ASTLiteral>(scale));
    }
    else if (which_data_type.isDecimal32())
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        return makeASTFunction("toDecimal32", std::move(ast), make_intrusive<ASTLiteral>(scale));
    }
    else if (which_data_type.isDateTime())
    {
        return addTimeZone(makeASTFunction("toDateTime", std::move(ast)), getDateTimeTimezone(*timestamp_data_type));
    }
    else if (which_data_type.isUInt32())
    {
        return makeASTFunction("toUInt32", std::move(ast));
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Can't cast {} to the timestamp type {}",
                        ast->formatForLogging(), timestamp_data_type->getName());
    }
}


ASTPtr timeSeriesDurationASTCast(ASTPtr && ast, const DataTypePtr & timestamp_data_type)
{
    WhichDataType which_data_type{timestamp_data_type};
    if (which_data_type.isDateTime64())
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        return makeASTFunction("toDecimal64", std::move(ast), make_intrusive<ASTLiteral>(scale));
    }
    else if (which_data_type.isDecimal64())
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        return makeASTFunction("toDecimal64", std::move(ast), make_intrusive<ASTLiteral>(scale));
    }
    else if (which_data_type.isDecimal32())
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        return makeASTFunction("toDecimal32", std::move(ast), make_intrusive<ASTLiteral>(scale));
    }
    else if (which_data_type.isDateTime() || which_data_type.isUInt32())
    {
        return makeASTFunction("toInt32", std::move(ast));
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Can't cast {} to the duration type for the timestamp type {}",
                        ast->formatForLogging(), timestamp_data_type->getName());
    }
}


ASTPtr timeSeriesScalarASTCast(ASTPtr && ast, const DataTypePtr & scalar_data_type)
{
    WhichDataType which_data_type{scalar_data_type};
    if (which_data_type.isFloat64())
    {
        return makeASTFunction("toFloat64", std::move(ast));
    }
    else if (which_data_type.isFloat32())
    {
        return makeASTFunction("toFloat32", std::move(ast));
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Can't cast {} to the scalar type {}",
                        ast->formatForLogging(), scalar_data_type->getName());
    }
}

}
