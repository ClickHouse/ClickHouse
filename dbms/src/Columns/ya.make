LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    clickhouse/contrib/pdqsort
    clickhouse/dbms/src/Common
    contrib/libs/icu
)

SRCS(
    Collator.cpp
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
