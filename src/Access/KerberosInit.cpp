#include <Access/KerberosInit.h>
#include <Common/Exception.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Loggers/Loggers.h>


int KerberosInit::init(const String & keytab_file, const String & principal, const String & cache_name)
{
    auto adqm_log = &Poco::Logger::get("ADQM");
    LOG_DEBUG(adqm_log,"KerberosInit: begin");

    krb5_error_code ret;

    // todo: use deftype
    //const char *deftype = nullptr;
    int flags = 0;
    principal_name = new char[256];
    std::copy(principal.begin(), principal.end(), principal_name);
    principal_name[principal.size()] = '\0';

    //memset(&k5, 0, sizeof(k5));
    memset(&k5d, 0, sizeof(k5d));
    k5 = &k5d;
    //memset(k5, 0, sizeof(k5_data));
    // begin
    ret = krb5_init_context(&k5->ctx);
    if (ret)
        throw DB::Exception(0, "Error while initializing Kerberos 5 library");

    if (!cache_name.empty())
    {
        ret = krb5_cc_resolve(k5->ctx, cache_name.c_str(), &k5->out_cc);
        // todo: analyze return code
        LOG_DEBUG(adqm_log,"Resolved cache");
    }
    else
    {
        // Resolve the default ccache and get its type and default principal (if it is initialized).
        ret = krb5_cc_default(k5->ctx, &defcache);
        if (ret)
            throw DB::Exception(0, "Error while getting default ccache");
        LOG_DEBUG(adqm_log,"Resolved default cache");
        // todo: deftype
        /*deftype = */krb5_cc_get_type(k5->ctx, defcache);
        if (krb5_cc_get_principal(k5->ctx, defcache, &defcache_princ) != 0)
            defcache_princ = nullptr;
    }

    // Use the specified principal name.
    ret = krb5_parse_name_flags(k5->ctx, principal_name, flags, &k5->me);
    if (ret)
        throw DB::Exception(0, "Error when parsing principal name " + String(principal_name));


    // to-do: add more cache init commands

    ret = krb5_unparse_name(k5->ctx, k5->me, &k5->name);
    if (ret)
        throw DB::Exception(0, "Error when unparsing name");

    LOG_DEBUG(adqm_log,"KerberosInit: Using principal: {}", k5->name);

    principal_name = k5->name;

    // init:
    memset(&my_creds, 0, sizeof(my_creds));

    ret = krb5_get_init_creds_opt_alloc(k5->ctx, &options);
    if (ret)
        throw DB::Exception(0, "Error in options allocation");

    // todo
/*
#ifndef _WIN32
    if (strncmp(opts->keytab_name, "KDB:", 4) == 0) {
        ret = kinit_kdb_init(&k5->ctx, k5->me->realm.data);
        errctx = k5->ctx;
        if (ret) {
            com_err(progname, ret,
                    _("while setting up KDB keytab for realm %s"),
                    k5->me->realm.data);
            goto cleanup;
        }
    }
#endif
*/
    // Resolve keytab
    ret = krb5_kt_resolve(k5->ctx, keytab_file.c_str(), &keytab);
    if (ret)
        throw DB::Exception(0, "Error resolving keytab "+keytab_file);

    LOG_DEBUG(adqm_log,"KerberosInit: Using keytab: {}", keytab_file);

    // todo: num_pa_opts


    // todo: in_cc / ccache

    // action: init or renew
    // todo: doing only init action:
    ret = krb5_get_init_creds_keytab(k5->ctx, &my_creds, k5->me, keytab, 0, nullptr, options);
    if (ret)
        LOG_DEBUG(adqm_log,"Getting initial credentials");

    // todo: implement renew action


    LOG_DEBUG(adqm_log,"Authenticated to Kerberos v5");
    LOG_DEBUG(adqm_log,"KerberosInit: end");
    return 0;
}

KerberosInit::~KerberosInit()
{
    if (k5->ctx)
    {
        //begin. cleanup:
        if (defcache)
            krb5_cc_close(k5->ctx, defcache);
        //todo
        krb5_free_principal(k5->ctx, defcache_princ);

        // init. cleanup:
        //todo:
        /*
    #ifndef _WIN32
        kinit_kdb_fini();
    #endif
        */
        if (options)
            krb5_get_init_creds_opt_free(k5->ctx, options);
        if (my_creds.client == k5->me)
            my_creds.client = nullptr;
        /*
        if (opts->pa_opts) {
            free(opts->pa_opts);
            opts->pa_opts = NULL;
            opts->num_pa_opts = 0;
        }
        */
        krb5_free_cred_contents(k5->ctx, &my_creds);
        if (keytab)
            krb5_kt_close(k5->ctx, keytab);


        // end:
        krb5_free_unparsed_name(k5->ctx, k5->name);
        krb5_free_principal(k5->ctx, k5->me);
        /*
        if (k5->in_cc != NULL)
            krb5_cc_close(k5->ctx, k5->in_cc);
        if (k5->out_cc != NULL)
            krb5_cc_close(k5->ctx, k5->out_cc);
        */
        krb5_free_context(k5->ctx);
    }
    memset(k5, 0, sizeof(*k5));

    delete[] principal_name;
}
