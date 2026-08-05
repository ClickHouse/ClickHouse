#include <IO/ReadMethod.h>
#include <IO/preadNoWait.h>


namespace DB
{

LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool pread_no_wait_supported, bool direct_io)
{
    if (requested == LocalFSReadMethod::pread_threadpool && !pread_no_wait_supported && !direct_io)
        return LocalFSReadMethod::pread;

    return requested;
}

LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool direct_io)
{
    /// Other methods do not depend on `preadNoWait`, and must not reach the probe:
    /// it is a raw system call that a kill-on-deny `seccomp` profile terminates the process for.
    if (requested != LocalFSReadMethod::pread_threadpool)
        return requested;

    return resolveLocalFSReadMethod(requested, getPreadNoWaitSupport().supported, direct_io);
}

}
