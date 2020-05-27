#pragma once

#include <shared_mutex>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <DataStreams/SizeLimits.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct Range;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

class ISet
{
public:
    ISet() = default;

    virtual bool empty() const { throw Exception("Not Implemented: empty", ErrorCodes::NOT_IMPLEMENTED); }

    virtual void createFromAST(const DataTypes&, ASTPtr, const Context&) { throw Exception("Not Implemented: createFromAST", ErrorCodes::NOT_IMPLEMENTED); }

    virtual void setHeader(const Block&) { throw Exception("Not Implemented: setHeader", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool insertFromBlock(const Block&) { throw Exception("Not Implemented: insertFromBlock", ErrorCodes::NOT_IMPLEMENTED); }

    virtual void finishInsert() { throw Exception("Not Implemented: finishInsert", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool isCreated() const { throw Exception("Not Implemented: isCreated", ErrorCodes::NOT_IMPLEMENTED); }

    virtual ColumnPtr execute(const Block&, bool) const { throw Exception("Not Implemented: execute", ErrorCodes::NOT_IMPLEMENTED); }

    virtual size_t getTotalRowCount() const { throw Exception("Not Implemented: getTotalRowCount", ErrorCodes::NOT_IMPLEMENTED); }

    virtual size_t getTotalByteCount() const { throw Exception("Not Implemented: getTotalByteCount", ErrorCodes::NOT_IMPLEMENTED); }

    virtual const DataTypes & getDataTypes() const { throw Exception("Not Implemented: getDataTypes", ErrorCodes::NOT_IMPLEMENTED); }
    virtual const DataTypes & getElementsTypes() const { throw Exception("Not Implemented: getElementsTypes", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool hasExplicitSetElements() const { throw Exception("Not Implemented: hasExplicitSetElements", ErrorCodes::NOT_IMPLEMENTED); }
    virtual Columns getSetElements() const { throw Exception("Not Implemented: getSetElements", ErrorCodes::NOT_IMPLEMENTED); }

    virtual void checkColumnsNumber(size_t) const { throw Exception("Not Implemented: checkColumnsNumber", ErrorCodes::NOT_IMPLEMENTED); }
    virtual void checkTypesEqual(size_t, const DataTypePtr&) const { throw Exception("Not Implemented: checkTypesEqual", ErrorCodes::NOT_IMPLEMENTED); }

    virtual ~ISet() {};
};

using SetPtr = std::shared_ptr<ISet>;
using ConstSetPtr = std::shared_ptr<const ISet>;
using Sets = std::vector<SetPtr>;

}  // namespace DB
