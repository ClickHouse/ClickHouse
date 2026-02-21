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
        String str = toString(static_cast<Decimal64>(timestamp), scale);
        /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to a floating-point number.
        if (str.find_first_of(".eE") == String::npos)
            str += ".";
        return timeSeriesTimestampASTCast(make_intrusive<ASTLiteral>(std::move(str)), timestamp_data_type);
    }
    else if (isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
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
