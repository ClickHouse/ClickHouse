LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    ColumnAggregateFunction.cpp
    ColumnArray.cpp
    ColumnConst.cpp
    ColumnDecimal.cpp
    ColumnFixedString.cpp
    ColumnLowCardinality.cpp
    ColumnNullable.cpp
    ColumnsCommon.cpp
    ColumnString.cpp
    ColumnTuple.cpp
    ColumnVector.cpp
    IColumn.cpp
)

END()
