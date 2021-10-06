#pragma once
#include <Common/Visitor.h>
#include <base/Typelists.h>

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

using NumericArraySources = TLMap<NumericArraySource, TLNumbersWithUUID>;
using BasicArraySources = TLAppend<GenericArraySource, NumericArraySources>;

class ArraySourceVisitor : public TLChangeRoot<Visitor, BasicArraySources>
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
