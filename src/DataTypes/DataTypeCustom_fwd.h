#pragma once

#include <memory>

namespace DB
{

class IDataTypeCustomName;
class IDataTypeCustomTextSerialization;
class IDataTypeCustomStreams;
struct DataTypeCustomDesc;

using DataTypeCustomNamePtr = std::unique_ptr<const IDataTypeCustomName>;
using DataTypeCustomTextSerializationPtr = std::unique_ptr<const IDataTypeCustomTextSerialization>;
using DataTypeCustomStreamsPtr = std::unique_ptr<const IDataTypeCustomStreams>;
using DataTypeCustomDescPtr = std::unique_ptr<DataTypeCustomDesc>;

}
