#include <Access/KerberosInit.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Loggers/Loggers.h>
#include <filesystem>
#include <boost/core/noncopyable.hpp>
#include <fmt/format.h>
#if USE_KRB5
#include <krb5.h>
#include <mutex>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int KERBEROS_ERROR;
}
}

namespace
{
struct K5Data
{
    krb5_context ctx;
    krb5_ccache out_cc;
    krb5_principal me;
    char * name;
    krb5_boolean switch_to_cache;
};

/**
 * This class implements programmatic implementation of kinit.
 */
class KerberosInit : boost::noncopyable
{
public:
    void init(const String & keytab_file, const String & principal, const String & cache_name = "");
    ~KerberosInit();
private:
    struct K5Data k5 {};
    krb5_ccache defcache = nullptr;
    krb5_get_init_creds_opt * options = nullptr;
    // Credentials structure including ticket, session key, and lifetime info.
    krb5_creds my_creds;
    krb5_keytab keytab = nullptr;
    krb5_principal defcache_princ = nullptr;
    String fmtError(krb5_error_code code) const;
};
}


String KerberosInit::fmtError(krb5_error_code code) const
{
    const char *msg;
    msg = krb5_get_error_message(k5.ctx, code);
    String fmt_error = fmt::format(" ({}, {})", code, msg);
    krb5_free_error_message(k5.ctx, msg);
    return fmt_error;
}

void KerberosInit::init(const String & keytab_file, const String & principal, const String & cache_name)
{
    auto * log = &Poco::Logger::get("KerberosInit");
    LOG_TRACE(log,"Trying to authenticate with Kerberos v5");

    krb5_error_code ret;

    const char *deftype = nullptr;

    if (!std::filesystem::exists(keytab_file))
        throw Exception("Keytab file does not exist", ErrorCodes::KERBEROS_ERROR);

    ret = krb5_init_context(&k5.ctx);
    if (ret)
        throw Exception(ErrorCodes::KERBEROS_ERROR, "Error while initializing Kerberos 5 library ({})", ret);

    if (!cache_name.empty())
    {
        ret = krb5_cc_resolve(k5.ctx, cache_name.c_str(), &k5.out_cc);
        if (ret)
            throw Exception("Error in resolving cache" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
        LOG_TRACE(log,"Resolved cache");
    }
    else
    {
        // Resolve the default cache and get its type and default principal (if it is initialized).
        ret = krb5_cc_default(k5.ctx, &defcache);
        if (ret)
            throw Exception("Error while getting default cache" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
        LOG_TRACE(log,"Resolved default cache");
        deftype = krb5_cc_get_type(k5.ctx, defcache);
        if (krb5_cc_get_principal(k5.ctx, defcache, &defcache_princ) != 0)
            defcache_princ = nullptr;
    }

    // Use the specified principal name.
    ret = krb5_parse_name_flags(k5.ctx, principal.c_str(), 0, &k5.me);
    if (ret)
        throw Exception("Error when parsing principal name " + principal + fmtError(ret), ErrorCodes::KERBEROS_ERROR);

    // Cache related commands
    if (k5.out_cc == nullptr && krb5_cc_support_switch(k5.ctx, deftype))
    {
        // Use an existing cache for the client principal if we can.
        ret = krb5_cc_cache_match(k5.ctx, k5.me, &k5.out_cc);
        if (ret && ret != KRB5_CC_NOTFOUND)
            throw Exception("Error while searching for cache for " + principal + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
        if (0 == ret)
        {
            LOG_TRACE(log,"Using default cache: {}", krb5_cc_get_name(k5.ctx, k5.out_cc));
            k5.switch_to_cache = 1;
        }
        else if (defcache_princ != nullptr)
        {
            // Create a new cache to avoid overwriting the initialized default cache.
            ret = krb5_cc_new_unique(k5.ctx, deftype, nullptr, &k5.out_cc);
            if (ret)
                throw Exception("Error while generating new cache" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
            LOG_TRACE(log,"Using default cache: {}", krb5_cc_get_name(k5.ctx, k5.out_cc));
            k5.switch_to_cache = 1;
        }
    }

    // Use the default cache if we haven't picked one yet.
    if (k5.out_cc == nullptr)
    {
        k5.out_cc = defcache;
        defcache = nullptr;
        LOG_TRACE(log,"Using default cache: {}", krb5_cc_get_name(k5.ctx, k5.out_cc));
    }

    ret = krb5_unparse_name(k5.ctx, k5.me, &k5.name);
    if (ret)
        throw Exception("Error when unparsing name" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
    LOG_TRACE(log,"Using principal: {}", k5.name);

    // Allocate a new initial credential options structure.
    ret = krb5_get_init_creds_opt_alloc(k5.ctx, &options);
    if (ret)
        throw Exception("Error in options allocation" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);

    // Resolve keytab
    ret = krb5_kt_resolve(k5.ctx, keytab_file.c_str(), &keytab);
    if (ret)
        throw Exception("Error in resolving keytab "+keytab_file + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
    LOG_TRACE(log,"Using keytab: {}", keytab_file);

    // Set an output credential cache in initial credential options.
    ret = krb5_get_init_creds_opt_set_out_ccache(k5.ctx, options, k5.out_cc);
    if (ret)
        throw Exception("Error in setting output credential cache" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);

    // Action: init or renew
    LOG_TRACE(log,"Trying to renew credentials");
    memset(&my_creds, 0, sizeof(my_creds));
    // Get renewed credential from KDC using an existing credential from output cache.
    ret = krb5_get_renewed_creds(k5.ctx, &my_creds, k5.me, k5.out_cc, nullptr);
    if (ret)
    {
        LOG_TRACE(log,"Renew failed {}", fmtError(ret));
        LOG_TRACE(log,"Trying to get initial credentials");
        // Request KDC for an initial credentials using keytab.
        ret = krb5_get_init_creds_keytab(k5.ctx, &my_creds, k5.me, keytab, 0, nullptr, options);
        if (ret)
            throw Exception("Error in getting initial credentials" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
        else
            LOG_TRACE(log,"Got initial credentials");
    }
    else
    {
        LOG_TRACE(log,"Successful renewal");
        // Initialize a credential cache. Destroy any existing contents of cache and initialize it for the default principal.
        ret = krb5_cc_initialize(k5.ctx, k5.out_cc, k5.me);
        if (ret)
            throw Exception("Error when initializing cache" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
        LOG_TRACE(log,"Initialized cache");
        // Store credentials in a credential cache.
        ret = krb5_cc_store_cred(k5.ctx, k5.out_cc, &my_creds);
        if (ret)
            LOG_TRACE(log,"Error while storing credentials");
        LOG_TRACE(log,"Stored credentials");
    }

    if (k5.switch_to_cache)
    {
        // Make a credential cache the primary cache for its collection.
        ret = krb5_cc_switch(k5.ctx, k5.out_cc);
        if (ret)
            throw Exception("Error while switching to new cache" + fmtError(ret), ErrorCodes::KERBEROS_ERROR);
    }

    LOG_TRACE(log,"Authenticated to Kerberos v5");
}

KerberosInit::~KerberosInit()
{
    if (k5.ctx)
    {
        if (defcache)
            krb5_cc_close(k5.ctx, defcache);
        krb5_free_principal(k5.ctx, defcache_princ);

        if (options)
            krb5_get_init_creds_opt_free(k5.ctx, options);
        if (my_creds.client == k5.me)
            my_creds.client = nullptr;
        krb5_free_cred_contents(k5.ctx, &my_creds);
        if (keytab)
            krb5_kt_close(k5.ctx, keytab);

        krb5_free_unparsed_name(k5.ctx, k5.name);
        krb5_free_principal(k5.ctx, k5.me);
        if (k5.out_cc != nullptr)
            krb5_cc_close(k5.ctx, k5.out_cc);
        krb5_free_context(k5.ctx);
    }
}

void kerberosInit(const String & keytab_file, const String & principal, const String & cache_name)
{
    // Using mutex to prevent cache file corruptions
    static std::mutex kinit_mtx;
    std::lock_guard lck(kinit_mtx);
    KerberosInit k_init;
    k_init.init(keytab_file, principal, cache_name);
}
#endif // USE_KRB5
