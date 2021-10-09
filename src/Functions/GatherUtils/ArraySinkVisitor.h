#pragma once
#include <Common/Visitor.h>
#include <base/Typelists.h>

namespace DB::GatherUtils
{
#pragma GCC visibility push(hidden)

template <typename T>
struct NumericArraySink;

struct GenericArraySink;

template <typename ArraySink>
struct NullableArraySink;

using NumericArraySinks = TLMap<NumericArraySink, TLNumbersWithUUID>;
using BasicArraySinks = TLAppend<GenericArraySink, NumericArraySinks>;
using NullableArraySinks = TLMap<NullableArraySink, BasicArraySinks>;
using TLArraySinks = TLConcat<BasicArraySinks, NullableArraySinks>;

class ArraySinkVisitor : public TLChangeRoot<Visitor, TLArraySinks>
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
