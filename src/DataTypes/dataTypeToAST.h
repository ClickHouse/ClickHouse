#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>


namespace DB
{
class ASTDataType;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Converts a data type to its AST representation.
boost::intrusive_ptr<ASTDataType> dataTypeToAST(const DataTypePtr & data_type);

}
