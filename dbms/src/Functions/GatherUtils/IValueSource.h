#pragma once
#include <Functions/GatherUtils/ValueSourceVisitor.h>
#include <Common/Exception.h>

namespace DB::GatherUtils
{

struct IValueSource
{
    virtual ~IValueSource() = default;

    virtual void accept(ValueSourceVisitor &)
    {
        throw Exception("Accept not implemented for " + demangle(typeid(*this).name()));
    }

    virtual bool isConst() const { return false; }
};

template <typename Derived>
class ValueSourceImpl : public Visitable<Derived, IValueSource, ValueSourceVisitor> {};

}
