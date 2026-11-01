#include <Client/NonInteractiveAuthentication.h>

namespace DB
{

namespace
{
thread_local bool non_interactive = false;
}

NonInteractiveAuthenticationScope::NonInteractiveAuthenticationScope() : previous(non_interactive)
{
    non_interactive = true;
}

NonInteractiveAuthenticationScope::~NonInteractiveAuthenticationScope()
{
    non_interactive = previous;
}

bool NonInteractiveAuthenticationScope::isActive()
{
    return non_interactive;
}

}
