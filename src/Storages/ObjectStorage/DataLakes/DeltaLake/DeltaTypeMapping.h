#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <DataTypes/IDataType.h>

namespace DB
{

/// Delta Lake primitive types a ClickHouse column can map to for CREATE TABLE. See `classifyDeltaPrimitive`.
enum class DeltaPrimitiveType
{
    Boolean,
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    String,
    Date,
    Timestamp,
    Decimal,
};

/// Map a ClickHouse leaf type to the Delta primitive that stores its values without loss; throws
/// `NOT_IMPLEMENTED` for a type with no loss-free Delta representation. Kept parquet-independent (unlike the
/// rest of `DeltaLakeMetadata`) so the create path can classify types even when built without Parquet.
DeltaPrimitiveType classifyDeltaPrimitive(const DataTypePtr & type);

}

#endif
