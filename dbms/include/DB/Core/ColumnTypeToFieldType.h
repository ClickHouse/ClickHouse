#ifndef DBMS_CORE_COLUMN_TYPE_TO_FIELD_TYPE_H
#define DBMS_CORE_COLUMN_TYPE_TO_FIELD_TYPE_H

#include <DB/Core/Field.h>
#include <DB/Core/Column.h>


namespace DB
{

/** Переводит типы, использующиеся в Column в типы, использующиеся в Field
  */
template <typename T> struct ColumnTypeToFieldType;

template <> struct ColumnTypeToFieldType<UInt8Column>  { typedef UInt64 Type; };
template <> struct ColumnTypeToFieldType<UInt16Column> { typedef UInt64 Type; };
template <> struct ColumnTypeToFieldType<UInt32Column> { typedef UInt64 Type; };
template <> struct ColumnTypeToFieldType<UInt64Column> { typedef UInt64 Type; };

template <> struct ColumnTypeToFieldType<Int8Column>  { typedef Int64 Type; };
template <> struct ColumnTypeToFieldType<Int16Column> { typedef Int64 Type; };
template <> struct ColumnTypeToFieldType<Int32Column> { typedef Int64 Type; };
template <> struct ColumnTypeToFieldType<Int64Column> { typedef Int64 Type; };

template <> struct ColumnTypeToFieldType<Float32Column> { typedef Float64 Type; };
template <> struct ColumnTypeToFieldType<Float64Column> { typedef Float64 Type; };

template <> struct ColumnTypeToFieldType<StringColumn> { typedef String Type; };
template <> struct ColumnTypeToFieldType<VariantColumn>  { typedef Field Type; };

}

#endif
