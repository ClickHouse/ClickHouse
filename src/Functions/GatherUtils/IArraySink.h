#pragma once

#include <Functions/GatherUtils/ArraySinkVisitor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace GatherUtils
{

[[noreturn]] inline void throwAcceptNotImplementedSink(const std::string & name)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Accept not implemented for {}", name);
}

struct IArraySink
{
    virtual ~IArraySink() = default;

    virtual void accept(ArraySinkVisitor &)
    {
        throwAcceptNotImplementedSink(demangle(typeid(*this).name()));
    }
};

template <typename Derived>
class ArraySinkImpl : public Visitable<Derived, IArraySink, ArraySinkVisitor> {};  /// NOLINT(bugprone-crtp-constructor-accessibility)

}

}
