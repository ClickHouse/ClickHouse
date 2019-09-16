#include <IO/ReadWriteBufferFromHTTP.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_REDIRECTS;
}

}

