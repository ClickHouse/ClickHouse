#pragma once
#include <Common/Visitor.h>
#include <base/Typelists.h>

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

using NumericValueSources = TLMap<NumericValueSource, TLNumbersWithUUID>;
using BasicValueSources = TLAppend<GenericValueSource, NumericValueSources>;
using NullableValueSources = TLMap<NullableValueSource, BasicValueSources>;
using BasicAndNullableValueSources = TLConcat<BasicValueSources, NullableValueSources>;
using ConstValueSources = TLMap<ConstSource, BasicAndNullableValueSources>;
using TypeListValueSources = TLConcat<BasicAndNullableValueSources, ConstValueSources>;

class ValueSourceVisitor : public TLChangeRoot<Visitor, TypeListValueSources>
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
