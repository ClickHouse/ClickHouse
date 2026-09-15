#pragma once

#include <memory>
#include <optional>
#include <cstddef>
#include <Core/Field.h>
#include <Core/Types_fwd.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct FormatSettings;
class IColumn;

/** Allow to customize an existing data type and set a different name and/or text serialization/deserialization methods.
 * See use in IPv4 and IPv6 data types, and also in SimpleAggregateFunction.
  */
class IDataTypeCustomName
{
public:
    virtual ~IDataTypeCustomName() = default;

    virtual String getName() const = 0;
    virtual std::optional<Field> getDefault() const { return std::nullopt; }

    /** Optional semantic identity for custom types whose logical type is stricter than
      * the storage type they customize. Custom types with the same non-empty identity
      * are treated as semantically compatible even when wrapped by another custom type
      * such as SimpleAggregateFunction. A custom type with a semantic identity is not
      * compatible with an otherwise layout-compatible plain storage type.
      */
    virtual std::optional<String> getSemanticIdentity() const { return std::nullopt; }

    /** Custom value validation is intentionally separate from serialization. Some
      * internal reconstruction paths can materialize a custom value without going
      * through its serialization, so generic consumers may validate before comparing,
      * hashing, sorting, or storing such values.
      */
    virtual bool requiresValueValidation() const { return false; }
    virtual void validateColumn(const IColumn &, const String &) const {}
};

using DataTypeCustomNamePtr = std::unique_ptr<const IDataTypeCustomName>;

/** Describe a data type customization
 */
struct DataTypeCustomDesc
{
    DataTypeCustomNamePtr name;
    SerializationPtr serialization;

    explicit DataTypeCustomDesc(
        DataTypeCustomNamePtr name_,
        SerializationPtr serialization_ = nullptr)
    : name(std::move(name_))
    , serialization(std::move(serialization_)) {}
};

using DataTypeCustomDescPtr = std::unique_ptr<DataTypeCustomDesc>;

/** A simple implementation of IDataTypeCustomName
 */
class DataTypeCustomFixedName : public IDataTypeCustomName
{
private:
    String name;
public:
    explicit DataTypeCustomFixedName(String name_) : name(name_) {}
    String getName() const override { return name; }
};

/// Generic helpers for custom types with logical semantics stricter than their
/// physical storage representation.
std::optional<String> getCustomTypeSemanticIdentity(const IDataType & type);
std::optional<String> getCustomTypeSemanticIdentity(const DataTypePtr & type);
bool containsCustomTypeSemanticIdentity(const IDataType & type);
bool containsCustomTypeSemanticIdentity(const DataTypePtr & type);
bool containsCustomTypeValueValidation(const IDataType & type);
bool containsCustomTypeValueValidation(const DataTypePtr & type);

/// Rejects pairwise operations when aligned custom semantic types differ or when
/// a semantic custom type is paired with a layout-compatible plain storage type.
void assertCustomDataTypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation);

/// Variant-backed sets may probe an exact alternative independently from the
/// Variant carrier. This helper preserves that adaptor behavior while applying
/// the same custom semantic compatibility checks.
void assertCustomDataTypeSetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type);

/// Applies custom value validation recursively through Array, Tuple, Map,
/// Variant, Nullable, and LowCardinality carriers.
void validateCustomDataTypeColumn(
    const IColumn & column, const DataTypePtr & type, const String & operation);

}
