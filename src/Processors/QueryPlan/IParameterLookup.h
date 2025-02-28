#pragma once
#include <Core/Field.h>
#include <boost/noncopyable.hpp>

namespace DB
{

/// Interface for looking up query plan parameters by name.
struct IParameterLookup : boost::noncopyable
{
    virtual ~IParameterLookup() = default;

    virtual Field getParameter(const String & name) const = 0;
};

}
