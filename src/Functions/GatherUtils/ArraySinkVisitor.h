#pragma once
#include <Common/Visitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{

template <typename T>
struct NumericArraySink;

struct GenericArraySink;

template <typename ArraySink>
struct NullableArraySink;

using NumericArraySinks = typename TypeListMap<NumericArraySink, TypeListNumbersAndUInt128>::Type;
using BasicArraySinks = typename AppendToTypeList<GenericArraySink, NumericArraySinks>::Type;
using NullableArraySinks = typename TypeListMap<NullableArraySink, BasicArraySinks>::Type;
using TypeListArraySinks = typename TypeListConcat<BasicArraySinks, NullableArraySinks>::Type;

class ArraySinkVisitor : public ApplyTypeListForClass<Visitor, TypeListArraySinks>::Type {};

template <typename Derived>
class ArraySinkVisitorImpl : public VisitorImpl<Derived, ArraySinkVisitor> {};

}
