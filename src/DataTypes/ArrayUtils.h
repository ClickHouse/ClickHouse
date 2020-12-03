#include <DataTypes/IDataType.h>

namespace DB
{

void serializeArraySizesPositionIndependent(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit);
void deserializeArraySizesPositionIndependent(IColumn & column, ReadBuffer & istr, UInt64 limit);

}
