#include <Formats/IRowInputStream.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void IRowInputStream::syncAfterError()
{
    throw Exception("Method syncAfterError is not implemented for input format", ErrorCodes::NOT_IMPLEMENTED);
}

}
