#pragma once
#include <Common/Visitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{

template <typename T>
struct NumericValueSource;

struct GenericValueSource;

template <typename ArraySource>
struct NullableValueSource;

template <typename Base>
struct ConstSource;

using NumericValueSources = typename TypeListMap<NumericValueSource, TypeListNumbers>::Type;
using BasicValueSources = typename AppendToTypeList<GenericValueSource, NumericValueSources>::Type;
using NullableValueSources = typename TypeListMap<NullableValueSource, BasicValueSources>::Type;
using BasicAndNullableValueSources = typename TypeListConcat<BasicValueSources, NullableValueSources>::Type;
using ConstValueSources = typename TypeListMap<ConstSource, BasicAndNullableValueSources>::Type;
using TypeListValueSources = typename TypeListConcat<BasicAndNullableValueSources, ConstValueSources>::Type;

class ValueSourceVisitor : public ApplyTypeListForClass<Visitor, TypeListValueSources>::Type {};

template <typename Derived>
class ValueSourceVisitorImpl : public VisitorImpl<Derived, ValueSourceVisitor> {};

}
