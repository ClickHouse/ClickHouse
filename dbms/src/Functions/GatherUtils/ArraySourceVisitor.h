#pragma once
#include <Common/Visitor.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{

template <typename T>
struct NumericArraySource;

struct GenericArraySource;

template <typename ArraySource>
struct NullableArraySource;

template <typename Base>
struct ConstSource;

using NumericArraySources = typename TypeListMap<NumericArraySource, TypeListNumbers>::Type;
using BasicArraySources = typename AppendToTypeList<GenericArraySource, NumericArraySources>::Type;
using NullableArraySources = typename TypeListMap<NullableArraySource, BasicArraySources>::Type;
using BasicAndNullableArraySources = typename TypeListConcat<BasicArraySources, NullableArraySources>::Type;
using ConstArraySources = typename TypeListMap<ConstSource, BasicAndNullableArraySources>::Type;
using TypeListArraySources = typename TypeListConcat<BasicAndNullableArraySources, ConstArraySources>::Type;

class ArraySourceVisitor : public ApplyTypeListForClass<Visitor, TypeListArraySources>::Type {};

template <typename Derived>
class ArraySourceVisitorImpl : public VisitorImpl<Derived, ArraySourceVisitor> {};

}
