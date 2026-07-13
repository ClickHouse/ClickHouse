#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <functional>

namespace DB
{

/// Cache for constant serialization objects.
/// Deduplicates identical serializations so that concurrent users of the
/// same type share one object.
namespace SerializationObjectPool
{
    using SerializationCreator = std::function<ISerialization *()>;

    /// Look up the pool by key.  On cache miss the creator is called
    /// (outside any pool lock) to build the object, which is then inserted.
    SerializationPtr getOrCreate(UInt128 key, SerializationCreator creator);
}

}
