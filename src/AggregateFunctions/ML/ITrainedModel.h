#pragma once
#include <memory>
#include <vector>

namespace DB
{

class WriteBuffer;
class ReadBuffer;
class IDataType;
class IColumn;
class Arena;

using DataTypePtr = std::shared_ptr<IDataType>;
using DataTypes = std::vector<DataTypePtr>;

using ColumnRawPtrs = std::vector<const IColumn *>;

/// Interface for trained ML model.
class ITrainedModel
{
public:
    using StatePtr = char *;
    using States = std::vector<StatePtr>;

    virtual ~ITrainedModel() = default;

    virtual void destroy(const States & states) = 0;
    virtual void serialize(const StatePtr & states, WriteBuffer & buf) = 0;
    virtual void deserialize(StatePtr & states, size_t num_states, ReadBuffer & buf, Arena * arena) = 0;

    virtual DataTypePtr getReturnType(const DataTypes & argument_types) const = 0;
    virtual void predict(StatePtr & states, const ColumnRawPtrs & arguments, IColumn & result) = 0;
};

}
