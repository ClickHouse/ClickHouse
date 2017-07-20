#pragma once
#include <Core/Types.h>
#include <Common/UInt128.h>

namespace DB
{

template <typename T>
class ColumnVector;

template <typename T>
class ColumnConst;

#define GET_COLUMN_VECTOR_TYPE(TYPE, M) M(ColumnVector< TYPE >)
#define COLUMN_VECTO_TYPE_LIST(M) APPLY_FOR_NUMBERS(GET_COLUMN_VECTOR_TYPE, M)
#define APPLY_FOR_COLUMN_VECTOR(M) COLUMN_VECTO_TYPE_LIST(M) M(ColumnVector<UInt128>)

#define GET_COLUMN_CONST_TYPE(TYPE, M) M(ColumnConst< TYPE >)
#define COLUMN_CONST_TYPE_LIST(M) APPLY_FOR_NUMBERS(GET_COLUMN_CONST_TYPE, M)
#define APPLY_FOR_COLUMN_CONST(M) COLUMN_CONST_TYPE_LIST(M) M(ColumnConst<UInt128>)

#define APPLY_FOR_NOT_TEMPLATE_COLUMNS(M) \
    M(ColumnString) \
    M(ColumnFixedString) \
    M(ColumnAggregateFunction) \
    M(ColumnArray) \
    M(ColumnConstAggregateFunction) \
    M(ColumnExpression) \
    M(ColumnNullable) \
    M(ColumnSet) \
    M(ColumnTuple)

#define APPLY_FOR_COLUMNS(M) \
    APPLY_FOR_COLUMN_VECTOR(M) \
    APPLY_FOR_COLUMN_CONST(M) \
    APPLY_FOR_NOT_TEMPLATE_COLUMNS(M)

#define DECLARE_COLUMNS(COLUMN) \
    class COLUMN ;

APPLY_FOR_NOT_TEMPLATE_COLUMNS(DECLARE_COLUMNS);

template <typename T>
class ColumnConst;

template <typename T>
class ColumnVector;


#define MAKE_COLUMNS_LIST(COLUMN) COLUMN,

#define COLUMN_LIST APPLY_FOR_COLUMNS(MAKE_COLUMNS_LIST) IColumn

class IColumn;

#define DECLARE_WITH_COLUMNS_LIST(CLASS) CLASS < COLUMN_LIST >

}
