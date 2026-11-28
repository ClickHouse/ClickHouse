#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaTypeMapping.h>

#if USE_DELTA_KERNEL_RS
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DeltaPrimitiveType classifyDeltaPrimitive(const DataTypePtr & type)
{
    /// `Bool` is stored as `UInt8`, so detect it before the plain `UInt8` case below.
    if (isBool(type))
        return DeltaPrimitiveType::Boolean;

    /// ClickHouse does the query processing, not Delta, so a column only needs a Delta type that stores its
    /// values without loss; the type read back from the `_delta_log` may differ (e.g. `UInt8` -> Delta
    /// `short` -> `Int16`, `FixedString` -> Delta `string` -> `String`). Types with no loss-free Delta
    /// representation are still rejected.
    switch (type->getTypeId())
    {
        case TypeIndex::Int8:    return DeltaPrimitiveType::Byte;
        case TypeIndex::Int16:   return DeltaPrimitiveType::Short;
        case TypeIndex::Int32:   return DeltaPrimitiveType::Integer;
        case TypeIndex::Int64:   return DeltaPrimitiveType::Long;
        /// Unsigned integers widen to the narrowest signed Delta integer that holds all their values.
        case TypeIndex::UInt8:   return DeltaPrimitiveType::Short;
        case TypeIndex::UInt16:  return DeltaPrimitiveType::Integer;
        case TypeIndex::UInt32:  return DeltaPrimitiveType::Long;
        case TypeIndex::Float32: return DeltaPrimitiveType::Float;
        case TypeIndex::Float64: return DeltaPrimitiveType::Double;
        case TypeIndex::String:
        case TypeIndex::FixedString:
            return DeltaPrimitiveType::String;
        case TypeIndex::Date:
        case TypeIndex::Date32:
            return DeltaPrimitiveType::Date;
        case TypeIndex::DateTime:
            /// Delta `timestamp` has no time zone: an explicit one is dropped on read-back, so the column
            /// would change type. Reject it (a `DateTime` without an explicit zone is fine).
            if (assert_cast<const DataTypeDateTime &>(*type).hasExplicitTimeZone())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "DeltaLake `timestamp` has no time zone; declare `{}` without an explicit time zone for CREATE TABLE",
                    type->getName());
            return DeltaPrimitiveType::Timestamp;
        case TypeIndex::DateTime64:
        {
            const auto & datetime64 = assert_cast<const DataTypeDateTime64 &>(*type);
            /// Delta `timestamp` is microsecond precision; a finer scale would lose sub-microsecond digits.
            if (datetime64.getScale() > 6)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "DeltaLake `timestamp` is microsecond precision; `{}` has a finer scale and cannot be "
                    "stored without loss for CREATE TABLE",
                    type->getName());
            /// Delta `timestamp` has no time zone: an explicit one is dropped on read-back, changing the type.
            if (datetime64.hasExplicitTimeZone())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "DeltaLake `timestamp` has no time zone; declare `{}` without an explicit time zone for CREATE TABLE",
                    type->getName());
            return DeltaPrimitiveType::Timestamp;
        }
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            /// Delta `decimal` supports precision up to 38, so a higher precision cannot be stored.
            if (getDecimalPrecision(*type) > 38)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "DeltaLake `decimal` supports precision up to 38; `{}` exceeds it for CREATE TABLE",
                    type->getName());
            return DeltaPrimitiveType::Decimal;
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "ClickHouse type `{}` has no compatible Delta Lake type for CREATE TABLE",
                type->getName());
    }
}

}

#endif
