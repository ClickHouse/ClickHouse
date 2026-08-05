#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/Decimal.h>


namespace DB
{
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Converts a timestamp to SQL.
/// The function assumes that `timestamp` uses the same decimal scale as `timestamp_data_type`.
ASTPtr timeSeriesTimestampToAST(DateTime64 timestamp, const DataTypePtr & timestamp_data_type);

/// Converts a duration to SQL.
/// The function assumes that `duration` uses the same decimal scale as `timestamp_data_type`.
ASTPtr timeSeriesDurationToAST(Decimal64 duration, const DataTypePtr & timestamp_data_type);

/// Converts a scalar value to SQL.
ASTPtr timeSeriesScalarToAST(Float64 value, const DataTypePtr & scalar_data_type);

/// Makes an AST casting an expression representing a timestamp to a specified `timestamp_data_type`.
ASTPtr timeSeriesTimestampASTCast(ASTPtr && ast, const DataTypePtr & timestamp_data_type);

/// Makes an AST casting an expression representing a duration to the duration data type corresponding to a specified `timestamp_data_type`.
ASTPtr timeSeriesDurationASTCast(ASTPtr && ast, const DataTypePtr & timestamp_data_type);

/// Makes an AST casting an expression representing a timestamp to a specified `timestamp_data_type`.
ASTPtr timeSeriesScalarASTCast(ASTPtr && ast, const DataTypePtr & scalar_data_type);

}
