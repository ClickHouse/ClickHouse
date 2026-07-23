#include <Storages/TimeSeries/timeSeriesLocalityHash.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>

#include <xxhash.h>


namespace DB
{

UInt64 calculateTimeSeriesLocalityHash(std::string_view metric_name)
{
    /// Must return the same value as the SQL function `xxHash64` (see makeTimeSeriesLocalityHashAST()).
    return XXH64(metric_name.data(), metric_name.size(), /* seed= */ 0);
}

ColumnPtr buildTimeSeriesLocalityHashColumn(const IColumn & metric_name_column)
{
    /// The `metric_name` column of an external "tags" table is allowed to be Nullable(String),
    /// and ColumnNullable doesn't support getDataAt().
    const IColumn * column = &metric_name_column;
    if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(column))
        column = &nullable_column->getNestedColumn();

    size_t num_rows = column->size();
    auto res = ColumnUInt64::create();
    auto & data = res->getData();
    data.resize_exact(num_rows);
    for (size_t i = 0; i != num_rows; ++i)
        data[i] = calculateTimeSeriesLocalityHash(column->getDataAt(i));
    return res;
}

ASTPtr makeTimeSeriesLocalityHashAST()
{
    /// Must calculate the same value as calculateTimeSeriesLocalityHash().
    return makeASTFunction("xxHash64", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
}

}
