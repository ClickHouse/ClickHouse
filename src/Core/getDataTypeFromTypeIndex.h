#include <Core/Types.h>
#include <DataTypes/IDataType.h>

namespace DB
{

DataTypePtr getSimpleDataTypeFromTypeIndex(TypeIndex type_index);

bool isSimpleDataType(TypeIndex type_index);

}
