#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// Cache for constant serialization objects.
/// Deduplicates identical serializations so that concurrent users of the
/// same type share one object.
namespace SerializationObjectPool
{
    SerializationPtr getOrCreate(UInt128 key, SerializationUniquePtr && serialization);
}

}
