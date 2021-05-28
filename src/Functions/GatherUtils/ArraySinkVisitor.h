#pragma once
#include <Common/Visitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
#pragma GCC visibility push(hidden)

template <typename T>
struct NumericArraySink;

struct GenericArraySink;

template <typename ArraySink>
struct NullableArraySink;

using NumericArraySinks = typename TypeListMap<NumericArraySink, TypeListNumbersAndUUID>::Type;
using BasicArraySinks = typename AppendToTypeList<GenericArraySink, NumericArraySinks>::Type;
using NullableArraySinks = typename TypeListMap<NullableArraySink, BasicArraySinks>::Type;
using TypeListArraySinks = typename TypeListConcat<BasicArraySinks, NullableArraySinks>::Type;

class ArraySinkVisitor : public ApplyTypeListForClass<Visitor, TypeListArraySinks>::Type
{
protected:
    ~ArraySinkVisitor() = default;
};

template <typename Derived>
class ArraySinkVisitorImpl : public VisitorImpl<Derived, ArraySinkVisitor>
{
protected:
    ~ArraySinkVisitorImpl() = default;
};

#pragma GCC visibility pop
}
