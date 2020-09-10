# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

ADDINCL(
    contrib/libs/pdqsort
)

PEERDIR(
    clickhouse/src/Common
    contrib/libs/icu
    contrib/libs/pdqsort
)

CFLAGS(-g0)

SRCS(
    Collator.cpp
    ColumnAggregateFunction.cpp
    ColumnArray.cpp
    ColumnConst.cpp
    ColumnDecimal.cpp
    ColumnFixedString.cpp
    ColumnFunction.cpp
    ColumnLowCardinality.cpp
    ColumnNullable.cpp
    ColumnsCommon.cpp
    ColumnString.cpp
    ColumnTuple.cpp
    ColumnVector.cpp
    FilterDescription.cpp
    getLeastSuperColumn.cpp
    IColumn.cpp

)

END()
