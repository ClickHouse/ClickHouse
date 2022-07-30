#pragma once

#include <type_traits>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>

#include "IValueSource.h"
#include "IArraySource.h"
#include "IArraySink.h"

/** These methods are intended for implementation of functions, that
  *  copy ranges from one or more columns to another column.
  *
  * Example:
  * - concatenation of strings and arrays (concat);
  * - extracting slices and elements of strings and arrays (substring, arraySlice, arrayElement);
  * - creating arrays from several columns ([x, y]);
  * - conditional selecting from several string or array columns (if, multiIf);
  * - push and pop elements from array front or back (arrayPushBack, etc);
  * - splitting strings into arrays and joining arrays back;
  * - formatting strings (format).
  *
  * There are various Sources, Sinks and Slices.
  * Source - allows to iterate over a column and obtain Slices.
  * Slice - a reference to elements to copy.
  * Sink - allows to build result column by copying Slices into it.
  */

namespace DB::GatherUtils
{

enum class ArraySearchType
{
    Any, // Corresponds to the hasAny array function
    All, // Corresponds to the hasAll array function
    Substr, // Corresponds to the hasSubstr array function
    StartsWith,
    EndsWith
};

std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows);
std::unique_ptr<IValueSource> createValueSource(const IColumn & col, bool is_const, size_t total_rows);
std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size);

ColumnArray::MutablePtr concat(const std::vector<std::unique_ptr<IArraySource>> & sources);

ColumnArray::MutablePtr sliceFromLeftConstantOffsetUnbounded(IArraySource & src, size_t offset);
ColumnArray::MutablePtr sliceFromLeftConstantOffsetBounded(IArraySource & src, size_t offset, ssize_t length);

ColumnArray::MutablePtr sliceFromRightConstantOffsetUnbounded(IArraySource & src, size_t offset);
ColumnArray::MutablePtr sliceFromRightConstantOffsetBounded(IArraySource & src, size_t offset, ssize_t length);

ColumnArray::MutablePtr sliceDynamicOffsetUnbounded(IArraySource & src, const IColumn & offset_column);
ColumnArray::MutablePtr sliceDynamicOffsetBounded(IArraySource & src, const IColumn & offset_column, const IColumn & length_column);

ColumnArray::MutablePtr sliceFromLeftDynamicLength(IArraySource & src, const IColumn & length_column);
ColumnArray::MutablePtr sliceFromRightDynamicLength(IArraySource & src, const IColumn & length_column);

void sliceHasAny(IArraySource & first, IArraySource & second, ColumnUInt8 & result);
void sliceHasAll(IArraySource & first, IArraySource & second, ColumnUInt8 & result);
void sliceHasSubstr(IArraySource & first, IArraySource & second, ColumnUInt8 & result);
void sliceHasStartsWith(IArraySource & first, IArraySource & second, ColumnUInt8 & result);
void sliceHasEndsWith(IArraySource & first, IArraySource & second, ColumnUInt8 & result);

inline void sliceHas(IArraySource & first, IArraySource & second, ArraySearchType search_type, ColumnUInt8 & result)
{
    switch (search_type)
    {
        case ArraySearchType::All:
            sliceHasAll(first, second, result);
            break;
        case ArraySearchType::Any:
            sliceHasAny(first, second, result);
            break;
        case ArraySearchType::Substr:
            sliceHasSubstr(first, second, result);
            break;
        case ArraySearchType::StartsWith:
            sliceHasStartsWith(first, second, result);
            break;
        case ArraySearchType::EndsWith:
            sliceHasEndsWith(first, second, result);
            break;
    }
}

void push(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, bool push_front);

void resizeDynamicSize(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, const IColumn & size_column);
void resizeConstantSize(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, ssize_t size);

}

