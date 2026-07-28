#pragma once
#include <Core/Field.h>
#include <boost/noncopyable.hpp>

namespace DB
{

/// Interface for looking up query plan parameters by name when building query pipeline.
/// Example use case: when executing distributed query plan we run many tasks with the same query plan on differentent partitions
/// of data. Each of this tasks will have exactly the same query plan, but a different value of parameter like `bucket_id` that
/// will determine which partition of data the particular task will read from.
struct IParameterLookup : boost::noncopyable
{
    virtual ~IParameterLookup() = default;

    virtual Field getParameter(const String & name) const = 0;
};

}
