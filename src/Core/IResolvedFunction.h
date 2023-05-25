#pragma once

#include <memory>
#include <vector>

namespace DB
{
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

struct Array;

/* Generic class for all functions.
 * Represents interface for function signature.
 */
class IResolvedFunction
{
public:
    virtual const DataTypePtr & getResultType() const = 0;

    virtual const DataTypes & getArgumentTypes() const = 0;

    virtual const Array & getParameters() const = 0;

    virtual ~IResolvedFunction() = default;
};

using IResolvedFunctionPtr = std::shared_ptr<const IResolvedFunction>;

}
