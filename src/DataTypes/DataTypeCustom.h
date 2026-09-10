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

struct DataTypeCustomDesc;
using DataTypeCustomDescPtr = std::unique_ptr<DataTypeCustomDesc>;

/** Allow to customize an existing data type and set a different name and/or text serialization/deserialization methods.
 * See use in IPv4 and IPv6 data types, and also in SimpleAggregateFunction.
  */
class IDataTypeCustomName
{
public:
    virtual ~IDataTypeCustomName() = default;

    virtual String getName() const = 0;

    /// The customization this one becomes when the type it decorates is rebuilt with different
    /// children as `rebuilt`, or nullptr when it no longer applies - a `Point` over a `Tuple` that is
    /// no longer two `Float64` is not a `Point`.
    /// The default refuses, because a name that cannot be shown to be still correct must not be
    /// reattached: the name is what `toTypeName`, `DESCRIBE` and the binary type encoding report.
    virtual DataTypeCustomDescPtr rederiveFor(const DataTypePtr & /*rebuilt*/) const { return nullptr; }
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

}
