#include <Storages/TimeSeries/PrometheusQueryToSQL/getRangeVectorGroupKey.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::PrometheusQueryToSQL
{

RangeVectorGroupKey getRangeVectorGroupKey(const SQLQueryPiece & range_vector, const ConverterContext & context)
{
    RangeVectorGroupKey res;

    switch (range_vector.store_method)
    {
        case StoreMethod::RAW_DATA:
        {
            /// SELECT timeSeriesIdToGroup(id), ... GROUP BY id
            res.group_expression = makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID));
            res.group_by_key = make_intrusive<ASTIdentifier>(ColumnNames::ID);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group, ... GROUP BY group
            res.group_expression = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.group_by_key = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            break;
        }

        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// These store methods have no series to group by.
            throwUnexpectedStoreMethod(range_vector, context);
        }
    }

    return res;
}

}
