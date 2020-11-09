#pragma once

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#include <Access/LDAPParams.h>
#include <Core/Types.h>

#if USE_LDAP
#   include <ldap.h>
#   define MAYBE_NORETURN
#else
#   define MAYBE_NORETURN [[noreturn]]
#endif


namespace DB
{

class LDAPClient
{
public:
    explicit LDAPClient(const LDAPServerParams & params_);
    ~LDAPClient();

    LDAPClient(const LDAPClient &) = delete;
    LDAPClient(LDAPClient &&) = delete;
    LDAPClient & operator= (const LDAPClient &) = delete;
    LDAPClient & operator= (LDAPClient &&) = delete;

protected:
    MAYBE_NORETURN void diag(const int rc);
    MAYBE_NORETURN void openConnection();
    int openConnection(const bool graceful_bind_failure = false);
    void closeConnection() noexcept;

protected:
    const LDAPServerParams params;
#if USE_LDAP
    LDAP * handle = nullptr;
#endif
};

class LDAPSimpleAuthClient
    : private LDAPClient
{
public:
    using LDAPClient::LDAPClient;
    bool check();
};

}

#undef MAYBE_NORETURN
