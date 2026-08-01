#include <IO/ReadMethod.h>


namespace DB
{

LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool pread_no_wait_supported, bool direct_io)
{
    if (requested == LocalFSReadMethod::pread_threadpool && !pread_no_wait_supported && !direct_io)
        return LocalFSReadMethod::pread;

    return requested;
}

}
