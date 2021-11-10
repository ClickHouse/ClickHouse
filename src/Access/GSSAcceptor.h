#pragma once

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#include <Access/Credentials.h>
#include <common/types.h>
#include <memory>

#if USE_KRB5
#   include <gssapi/gssapi.h>
#   include <gssapi/gssapi_ext.h>
#   define MAYBE_NORETURN
#else
#   define MAYBE_NORETURN [[noreturn]]
#endif

namespace Poco { class Logger; }

namespace DB
{

class GSSAcceptorContext
    : public Credentials
{
public:
    struct Params
    {
        String mechanism = "1.2.840.113554.1.2.2"; // OID: krb5
        String principal;
        String realm;
    };

    explicit GSSAcceptorContext(const Params& params_);
    virtual ~GSSAcceptorContext() override;

    GSSAcceptorContext(const GSSAcceptorContext &) = delete;
    GSSAcceptorContext(GSSAcceptorContext &&) = delete;
    GSSAcceptorContext & operator= (const GSSAcceptorContext &) = delete;
    GSSAcceptorContext & operator= (GSSAcceptorContext &&) = delete;

    const String & getRealm() const;
    bool isFailed() const;
    MAYBE_NORETURN String processToken(const String & input_token, Poco::Logger * log);

private:
    void reset();
    void resetHandles() noexcept;
    void initHandles();

private:
    const Params params;

    bool is_failed = false;
    String realm;

#if USE_KRB5
    gss_ctx_id_t context_handle = GSS_C_NO_CONTEXT;
    gss_cred_id_t acceptor_credentials_handle = GSS_C_NO_CREDENTIAL;
#endif
};

}

#undef MAYBE_NORETURN
