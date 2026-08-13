#pragma once

#include <memory>
#include <cstddef>
#include <Core/Types_fwd.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct FormatSettings;
class IColumn;
class IDataTypeCustomName;
/// Shared so a copy of a type can carry the same custom name, and so transformChildren can return
/// the name via shared_from_this.
using DataTypeCustomNamePtr = std::shared_ptr<const IDataTypeCustomName>;

/** Allow to customize an existing data type and set a different name and/or text serialization/deserialization methods.
 * See use in IPv4 and IPv6 data types, and also in SimpleAggregateFunction.
  */
class IDataTypeCustomName : public std::enable_shared_from_this<IDataTypeCustomName>
{
public:
    virtual ~IDataTypeCustomName() = default;

    virtual String getName() const = 0;

    /// A copy in sync with `transformed`, the rebuilt type this name belongs to, or itself when the
    /// name embeds no child types (the common case). An override must take its new children out of
    /// `transformed`, which already holds them, rather than transform its own copy again.
    virtual DataTypeCustomNamePtr transformChildren(const IDataType &) const { return shared_from_this(); }
};

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

}
