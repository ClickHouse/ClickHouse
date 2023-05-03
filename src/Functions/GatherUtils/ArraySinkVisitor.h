#pragma once
#include <Common/Visitor.h>
#include <base/TypeLists.h>

namespace DB::GatherUtils
{
#pragma GCC visibility push(hidden)

template <typename T>
struct NumericArraySink;

struct GenericArraySink;

template <typename ArraySink>
struct NullableArraySink;

using NumericArraySinks = TypeListMap<NumericArraySink, TypeListNumberWithUUID>;
using BasicArraySinks = TypeListAppend<GenericArraySink, NumericArraySinks>;
using NullableArraySinks = TypeListMap<NullableArraySink, BasicArraySinks>;
using TLArraySinks = TypeListConcat<BasicArraySinks, NullableArraySinks>;

class ArraySinkVisitor : public TypeListChangeRoot<Visitor, TLArraySinks>
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
