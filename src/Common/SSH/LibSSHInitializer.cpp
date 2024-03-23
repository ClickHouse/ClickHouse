#include "LibSSHInitializer.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SSH_EXCEPTION;
}

}

namespace ssh
{

LibSSHInitializer::LibSSHInitializer()
{
    int rc = ssh_init();
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to initialize libssh");
    }
}

LibSSHInitializer::~LibSSHInitializer()
{
    ssh_finalize();
}

}
