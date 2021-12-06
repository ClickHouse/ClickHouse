#pragma once
#include <Common/Visitor.h>
#include <base/TypeLists.h>

namespace DB::GatherUtils
{
#pragma GCC visibility push(hidden)

template <typename T>
struct NumericValueSource;

struct GenericValueSource;

template <typename ArraySource>
struct NullableValueSource;

template <typename Base>
struct ConstSource;

using NumericValueSources = TypeListMap<NumericValueSource, TypeListNumberWithUUID>;
using BasicValueSources = TypeListAppend<GenericValueSource, NumericValueSources>;
using NullableValueSources = TypeListMap<NullableValueSource, BasicValueSources>;
using BasicAndNullableValueSources = TypeListConcat<BasicValueSources, NullableValueSources>;
using ConstValueSources = TypeListMap<ConstSource, BasicAndNullableValueSources>;
using TypeListValueSources = TypeListConcat<BasicAndNullableValueSources, ConstValueSources>;

class ValueSourceVisitor : public TypeListChangeRoot<Visitor, TypeListValueSources>
{
protected:
    ~ValueSourceVisitor() = default;
};

template <typename Derived>
class ValueSourceVisitorImpl : public VisitorImpl<Derived, ValueSourceVisitor>
{
protected:
    ~ValueSourceVisitorImpl() = default;
};

#pragma GCC visibility pop
}
