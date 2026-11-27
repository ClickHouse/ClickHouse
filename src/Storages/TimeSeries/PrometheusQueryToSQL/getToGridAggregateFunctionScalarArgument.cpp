#include <Storages/TimeSeries/PrometheusQueryToSQL/getToGridAggregateFunctionScalarArgument.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::PrometheusQueryToSQL
{

ToGridAggregateFunctionScalarArgument getToGridAggregateFunctionScalarArgument(SQLQueryPiece & scalar, ConverterContext & context)
{
    ToGridAggregateFunctionScalarArgument argument;
    switch (scalar.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            /// Keep the literal at Float64: casting it to a Float32 scalar type would round e.g. 1.00000003 to 1.0
            /// and hide a value that is just out of the range the aggregate function accepts.
            argument.ast = make_intrusive<ASTLiteral>(scalar.scalar_value);
            break;
        }

        case StoreMethod::SINGLE_SCALAR:
        {
            /// A scalar subquery is nullable, but it always has exactly one row here.
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar.select_query), SQLSubqueryType::SCALAR);
            argument.ast = makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
            break;
        }

        case StoreMethod::SCALAR_GRID:
        {
            /// A scalar grid is one row with one Array column, so it is a scalar subquery too.
            argument.is_constant = false;
            context.subqueries.emplace_back(context.subqueries.size(), std::move(scalar.select_query), SQLSubqueryType::SCALAR);
            argument.ast = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
            break;
        }

        case StoreMethod::EMPTY:
        {
            break;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::VECTOR_GRID:
        case StoreMethod::SAMPLES:
        case StoreMethod::TIMESTAMP_AND_VALUE:
        {
            /// Can't get in here because these store methods are incompatible with a scalar (the caller checks the argument type).
            throwUnexpectedStoreMethod(scalar, context);
        }
    }
    return argument;
}

}
