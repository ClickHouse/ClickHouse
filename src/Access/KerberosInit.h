#pragma once

#include "config_core.h"

#include <base/types.h>

//#include <k5-int.h>
//#include "k5-platform.h"
#include <krb5.h>
//#include <extern.h>

struct k5_data
{
    krb5_context ctx;
    krb5_ccache in_cc, out_cc;
    krb5_principal me;
    char *name;
    krb5_boolean switch_to_cache;
};

class KerberosInit
{
public:
    int init(const String & keytab_file, const String & principal, const String & cache_name = "");
    ~KerberosInit();
private:
    struct k5_data * k5 = nullptr;
    //struct k5_data k5;
    struct k5_data k5d;
    krb5_ccache defcache = nullptr;
    krb5_get_init_creds_opt *options = nullptr;
    krb5_creds my_creds;
    krb5_keytab keytab = nullptr;
    char * principal_name;
    krb5_principal defcache_princ = nullptr;
};

