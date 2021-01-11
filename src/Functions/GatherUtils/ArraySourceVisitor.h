#pragma once
#include <Common/Visitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
#pragma GCC visibility push(hidden)

template <typename T>
struct NumericArraySource;

struct GenericArraySource;

template <typename ArraySource>
struct NullableArraySource;

template <typename Base>
struct ConstSource;

using NumericArraySources = typename TypeListMap<NumericArraySource, TypeListNumbersAndUInt128>::Type;
using BasicArraySources = typename AppendToTypeList<GenericArraySource, NumericArraySources>::Type;

class ArraySourceVisitor : public ApplyTypeListForClass<Visitor, BasicArraySources>::Type
{
protected:
    ~ArraySourceVisitor() = default;
};

template <typename Derived>
class ArraySourceVisitorImpl : public VisitorImpl<Derived, ArraySourceVisitor>
{
protected:
    ~ArraySourceVisitorImpl() = default;
};

#pragma GCC visibility pop
}
