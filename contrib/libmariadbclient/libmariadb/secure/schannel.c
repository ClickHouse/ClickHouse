/************************************************************************************
  Copyright (C) 2014 MariaDB Corporation Ab

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA

 *************************************************************************************/
#include "ma_schannel.h"

#pragma comment (lib, "crypt32.lib")
#pragma comment (lib, "secur32.lib")

//#define VOID void

extern my_bool ma_tls_initialized;

#define PROT_SSL3 1
#define PROT_TLS1_0 2
#define PROT_TLS1_2 4
#define PROT_TLS1_3 8

static struct
{
  DWORD cipher_id;
  DWORD protocol;
  const char *iana_name;
  const char *openssl_name;
  ALG_ID algs[4]; /* exchange, encryption, hash, signature */
}
cipher_map[] =
{
  {
    0x0002,
    PROT_TLS1_0 |  PROT_TLS1_2 | PROT_SSL3,
    "TLS_RSA_WITH_NULL_SHA", "NULL-SHA",
    { CALG_RSA_KEYX, 0, CALG_SHA1, CALG_RSA_SIGN }
  },
  {
    0x0004,
    PROT_TLS1_0 |  PROT_TLS1_2 | PROT_SSL3,
    "TLS_RSA_WITH_RC4_128_MD5", "RC4-MD5",
    { CALG_RSA_KEYX, CALG_RC4, CALG_MD5, CALG_RSA_SIGN }
  },
  {
    0x0005,
    PROT_TLS1_0 |  PROT_TLS1_2 | PROT_SSL3,
    "TLS_RSA_WITH_RC4_128_SHA", "RC4-SHA",
    { CALG_RSA_KEYX, CALG_RC4, CALG_SHA1, CALG_RSA_SIGN }
  },
  {
    0x000A,
    PROT_SSL3,
    "TLS_RSA_WITH_3DES_EDE_CBC_SHA", "DES-CBC3-SHA",
    {CALG_RSA_KEYX, CALG_3DES, CALG_SHA1, CALG_DSS_SIGN}
  },
  {
    0x0013,
    PROT_TLS1_0 |  PROT_TLS1_2 | PROT_SSL3,
    "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA", "EDH-DSS-DES-CBC3-SHA",
    { CALG_DH_EPHEM, CALG_3DES, CALG_SHA1, CALG_DSS_SIGN }
  },
  {
    0x002F,
    PROT_SSL3 | PROT_TLS1_0 | PROT_TLS1_2,
    "TLS_RSA_WITH_AES_128_CBC_SHA", "AES128-SHA",
    { CALG_RSA_KEYX, CALG_AES_128, CALG_SHA, CALG_RSA_SIGN}
  },
  {
    0x0032,
    PROT_TLS1_0 |  PROT_TLS1_2,
    "TLS_DHE_DSS_WITH_AES_128_CBC_SHA", "DHE-DSS-AES128-SHA",
    { CALG_DH_EPHEM, CALG_AES_128, CALG_SHA1, CALG_RSA_SIGN }
  },
  {
    0x0033,
    PROT_TLS1_0 |  PROT_TLS1_2,
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "DHE-RSA-AES128-SHA",
    { CALG_DH_EPHEM, CALG_AES_128, CALG_SHA1, CALG_RSA_SIGN }
  },
  {
    0x0035,
    PROT_TLS1_0 |  PROT_TLS1_2,
    "TLS_RSA_WITH_AES_256_CBC_SHA", "AES256-SHA",
    { CALG_RSA_KEYX, CALG_AES_256, CALG_SHA1, CALG_RSA_SIGN }
  },
  {
    0x0038,
    PROT_TLS1_0 |  PROT_TLS1_2,
    "TLS_DHE_DSS_WITH_AES_256_CBC_SHA", "DHE-DSS-AES256-SHA",
    { CALG_DH_EPHEM, CALG_AES_256, CALG_SHA1, CALG_DSS_SIGN }
  },
  {
    0x0039,
    PROT_TLS1_0 |  PROT_TLS1_2,
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA", "DHE-RSA-AES256-SHA",
    { CALG_DH_EPHEM, CALG_AES_256, CALG_SHA1, CALG_RSA_SIGN }
  },
  {
    0x003B,
    PROT_TLS1_2,
    "TLS_RSA_WITH_NULL_SHA256", "NULL-SHA256",
    { CALG_RSA_KEYX, 0, CALG_SHA_256, CALG_RSA_SIGN }
  },
  {
    0x003C,
    PROT_TLS1_2,
    "TLS_RSA_WITH_AES_128_CBC_SHA256", "AES128-SHA256",
    { CALG_RSA_KEYX, CALG_AES_128, CALG_SHA_256, CALG_RSA_SIGN }
  },
  {
    0x003D,
    PROT_TLS1_2,
    "TLS_RSA_WITH_AES_256_CBC_SHA256", "AES256-SHA256",
    { CALG_RSA_KEYX, CALG_AES_256, CALG_SHA_256, CALG_RSA_SIGN }
  },
  {
    0x0040,
    PROT_TLS1_2,
    "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256", "DHE-DSS-AES128-SHA256",
    { CALG_DH_EPHEM, CALG_AES_128, CALG_SHA_256, CALG_DSS_SIGN }
  },
  {
    0x009C,
    PROT_TLS1_2,
    "TLS_RSA_WITH_AES_128_GCM_SHA256", "AES128-GCM-SHA256",
    { CALG_RSA_KEYX, CALG_AES_128, CALG_SHA_256, CALG_RSA_SIGN }
  },
  {
    0x009D,
    PROT_TLS1_2,
    "TLS_RSA_WITH_AES_256_GCM_SHA384", "AES256-GCM-SHA384",
    { CALG_RSA_KEYX, CALG_AES_256, CALG_SHA_384, CALG_RSA_SIGN }
  },
  {
    0x009E,
    PROT_TLS1_2,
    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256", "DHE-RSA-AES128-GCM-SHA256",
    { CALG_DH_EPHEM, CALG_AES_128, CALG_SHA_256, CALG_RSA_SIGN }
  },
  {
    0x009F,
    PROT_TLS1_2,
    "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384", "DHE-RSA-AES256-GCM-SHA384",
    { CALG_DH_EPHEM, CALG_AES_256, CALG_SHA_384, CALG_RSA_SIGN }
  }

};

#define MAX_ALG_ID 50

void ma_schannel_set_sec_error(MARIADB_PVIO *pvio, DWORD ErrorNo);
void ma_schannel_set_win_error(MYSQL *mysql);

/*
  Initializes SSL and allocate global
  context SSL_context

  SYNOPSIS
    ma_tls_start

  RETURN VALUES
    0  success
    1  error
*/
int ma_tls_start(char *errmsg, size_t errmsg_len)
{

  ma_tls_initialized = TRUE;
  return 0;
}

/*
   Release SSL and free resources
   Will be automatically executed by 
   mysql_server_end() function

   SYNOPSIS
     ma_tls_end()
       void

   RETURN VALUES
     void
*/
void ma_tls_end()
{
  return;
}

/* {{{ static int ma_tls_set_client_certs(MARIADB_TLS *ctls) */
static int ma_tls_set_client_certs(MARIADB_TLS *ctls)
{
  MYSQL *mysql= ctls->pvio->mysql;
  char *certfile= mysql->options.ssl_cert,
       *keyfile= mysql->options.ssl_key;
  SC_CTX *sctx= (SC_CTX *)ctls->ssl;
  MARIADB_PVIO *pvio= ctls->pvio;

  sctx->client_cert_ctx= NULL;

  if (!certfile && keyfile)
    certfile= keyfile;
  if (!keyfile && certfile)
    keyfile= certfile;

  if (!certfile)
    return 0;

  if (!(sctx->client_cert_ctx = ma_schannel_create_cert_context(ctls->pvio, certfile)))
    return 1;

  if (!ma_schannel_load_private_key(pvio, sctx->client_cert_ctx, keyfile))
  {
    CertFreeCertificateContext(sctx->client_cert_ctx);
    sctx->client_cert_ctx= NULL;
    return 1;
  }
  return 0;
}
/* }}} */

/* {{{ void *ma_tls_init(MARIADB_TLS *ctls, MYSQL *mysql) */
void *ma_tls_init(MYSQL *mysql)
{
  SC_CTX *sctx= NULL;
  if ((sctx= (SC_CTX *)LocalAlloc(0, sizeof(SC_CTX))))
  {
    ZeroMemory(sctx, sizeof(SC_CTX));
    sctx->mysql= mysql;
  }
  return sctx;
}
/* }}} */


/* 
  Maps between openssl suite names and schannel alg_ids.
  Every suite has 4 algorithms (for exchange, encryption, hash and signing).
  
  The input string is a set of suite names (openssl),  separated 
  by ':'
  
  The output is written into the array 'arr' of size 'arr_size'
  The function returns number of elements written to the 'arr'.
*/

static struct _tls_version {
  const char *tls_version;
  DWORD protocol;
} tls_version[]= {
    {"TLSv1.0", PROT_TLS1_0},
    {"TLSv1.2", PROT_TLS1_2},
    {"TLSv1.3", PROT_TLS1_3},
    {"SSLv3",   PROT_SSL3}
};

static size_t set_cipher(char * cipher_str, DWORD protocol, ALG_ID *arr , size_t arr_size)
{
  char *token = strtok(cipher_str, ":");
  size_t pos = 0;

  while (token)
  {
    size_t i;

    for(i = 0; i < sizeof(cipher_map)/sizeof(cipher_map[0]) ; i++)
    {
      if(pos + 4 < arr_size && strcmp(cipher_map[i].openssl_name, token) == 0 ||
        (cipher_map[i].protocol <= protocol))
      {
        memcpy(arr + pos, cipher_map[i].algs, sizeof(ALG_ID)* 4);
        pos += 4;
        break;
      }
    }
    token = strtok(NULL, ":");
  }
  return pos;
}

my_bool ma_tls_connect(MARIADB_TLS *ctls)
{
  MYSQL *mysql;
  SCHANNEL_CRED Cred;
  MARIADB_PVIO *pvio;
  my_bool rc= 1;
  SC_CTX *sctx;
  SECURITY_STATUS sRet;
  ALG_ID AlgId[MAX_ALG_ID];
  WORD validTokens = 0;
  
  if (!ctls || !ctls->pvio)
    return 1;;
  
  pvio= ctls->pvio;
  sctx= (SC_CTX *)ctls->ssl;

  mysql= pvio->mysql;
 
  if (ma_tls_set_client_certs(ctls))
    goto end;

  ZeroMemory(&Cred, sizeof(SCHANNEL_CRED));

  /* Set cipher */
  if (mysql->options.ssl_cipher)
  {
    int i;
    DWORD protocol = 0;

    /* check if a protocol was specified as a cipher:
     * In this case don't allow cipher suites which belong to newer protocols
     * Please note: There are no cipher suites for TLS1.1
     */
    for (i = 0; i < sizeof(tls_version) / sizeof(tls_version[0]); i++)
    {
      if (!stricmp(mysql->options.ssl_cipher, tls_version[i].tls_version))
        protocol |= tls_version[i].protocol;
    }
    memset(AlgId, 0, MAX_ALG_ID * sizeof(ALG_ID));
    Cred.cSupportedAlgs = (DWORD)set_cipher(mysql->options.ssl_cipher, protocol, AlgId, MAX_ALG_ID);
    if (Cred.cSupportedAlgs)
    {
      Cred.palgSupportedAlgs = AlgId;
    }
    else if (!protocol)
    {
      ma_schannel_set_sec_error(pvio, SEC_E_ALGORITHM_MISMATCH);
      goto end;
    }
  }
  
  Cred.dwVersion= SCHANNEL_CRED_VERSION;

  Cred.dwFlags = SCH_CRED_NO_SERVERNAME_CHECK | SCH_CRED_NO_DEFAULT_CREDS | SCH_CRED_MANUAL_CRED_VALIDATION;

  if (sctx->client_cert_ctx)
  {
    Cred.cCreds = 1;
    Cred.paCred = &sctx->client_cert_ctx;
  }
  if (mysql->options.extension && mysql->options.extension->tls_version)
  {
    if (strstr("TLSv1.0", mysql->options.extension->tls_version))
      Cred.grbitEnabledProtocols|= SP_PROT_TLS1_0_CLIENT;
    if (strstr("TLSv1.1", mysql->options.extension->tls_version))
      Cred.grbitEnabledProtocols|= SP_PROT_TLS1_1_CLIENT;
    if (strstr("TLSv1.2", mysql->options.extension->tls_version))
      Cred.grbitEnabledProtocols|= SP_PROT_TLS1_2_CLIENT;
  }
  if (!Cred.grbitEnabledProtocols)
    Cred.grbitEnabledProtocols = SP_PROT_TLS1_0_CLIENT |  SP_PROT_TLS1_1_CLIENT;

  if ((sRet= AcquireCredentialsHandleA(NULL, UNISP_NAME_A, SECPKG_CRED_OUTBOUND,
                                       NULL, &Cred, NULL, NULL, &sctx->CredHdl, NULL)) != SEC_E_OK)
  {
    ma_schannel_set_sec_error(pvio, sRet);
    goto end;
  }
  sctx->FreeCredHdl= 1;

  if (ma_schannel_client_handshake(ctls) != SEC_E_OK)
    goto end;
  
  if (!ma_schannel_verify_certs(sctx))
    goto end;
  
  return 0;

end:
  if (rc && sctx->IoBufferSize)
    LocalFree(sctx->IoBuffer);
  sctx->IoBufferSize= 0;
  if (sctx->client_cert_ctx)
    CertFreeCertificateContext(sctx->client_cert_ctx);
  sctx->client_cert_ctx= 0;
  return 1;
}

ssize_t ma_tls_read(MARIADB_TLS *ctls, const uchar* buffer, size_t length)
{
  SC_CTX *sctx= (SC_CTX *)ctls->ssl;
  MARIADB_PVIO *pvio= sctx->mysql->net.pvio;
  DWORD dlength= 0;
  SECURITY_STATUS status = ma_schannel_read_decrypt(pvio, &sctx->CredHdl, &sctx->ctxt, &dlength, (uchar *)buffer, (DWORD)length);
  if (status == SEC_I_CONTEXT_EXPIRED)
    return 0; /* other side shut down the connection. */
  if (status == SEC_I_RENEGOTIATE)
    return -1; /* Do not handle renegotiate yet */

  return (status == SEC_E_OK)? (ssize_t)dlength : -1;
}

ssize_t ma_tls_write(MARIADB_TLS *ctls, const uchar* buffer, size_t length)
{ 
  SC_CTX *sctx= (SC_CTX *)ctls->ssl;
  MARIADB_PVIO *pvio= sctx->mysql->net.pvio;
  ssize_t rc, wlength= 0;
  ssize_t remain= length;

  while (remain > 0)
  {
    if ((rc= ma_schannel_write_encrypt(pvio, (uchar *)buffer + wlength, remain)) <= 0)
      return rc;
    wlength+= rc;
    remain-= rc;
  }
  return length;
}

/* {{{ my_bool ma_tls_close(MARIADB_PVIO *pvio) */
my_bool ma_tls_close(MARIADB_TLS *ctls)
{
  SC_CTX *sctx= (SC_CTX *)ctls->ssl; 
  
  if (sctx)
  {
    if (sctx->IoBufferSize)
      LocalFree(sctx->IoBuffer);
    if (sctx->client_cert_ctx)
      CertFreeCertificateContext(sctx->client_cert_ctx);
    FreeCredentialHandle(&sctx->CredHdl);
    DeleteSecurityContext(&sctx->ctxt);
  }
  LocalFree(sctx);
  return 0;
}
/* }}} */

int ma_tls_verify_server_cert(MARIADB_TLS *ctls)
{
  SC_CTX *sctx= (SC_CTX *)ctls->ssl;
  MARIADB_PVIO *pvio= ctls->pvio;
  int rc= 1;
  char *szName= NULL;
  char *pszServerName= pvio->mysql->host;
  PCCERT_CONTEXT pServerCert= NULL;

  /* check server name */
  if (pszServerName && (sctx->mysql->client_flag & CLIENT_SSL_VERIFY_SERVER_CERT))
  {
    DWORD NameSize= 0;
    char *p1;
    SECURITY_STATUS sRet;

    if ((sRet= QueryContextAttributes(&sctx->ctxt, SECPKG_ATTR_REMOTE_CERT_CONTEXT, (PVOID)&pServerCert)) != SEC_E_OK)
    {
      ma_schannel_set_sec_error(pvio, sRet);
      goto end;
    }

    if (!(NameSize= CertGetNameString(pServerCert,
                                      CERT_NAME_DNS_TYPE,
                                      CERT_NAME_SEARCH_ALL_NAMES_FLAG,
                                      NULL, NULL, 0)))
    {
      pvio->set_error(sctx->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error:  Can't retrieve name of server certificate");
      goto end;
    }

    if (!(szName= (char *)LocalAlloc(0, NameSize + 1)))
    {
      pvio->set_error(sctx->mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, NULL);
      goto end;
    }

    if (!CertGetNameString(pServerCert,
                           CERT_NAME_DNS_TYPE,
                           CERT_NAME_SEARCH_ALL_NAMES_FLAG,
                           NULL, szName, NameSize))

    {
      pvio->set_error(sctx->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "SSL connection error: Can't retrieve name of server certificate");
      goto end;
    }

    /* szName may contain multiple names: Each name is zero terminated, the last name is
       double zero terminated */

    
    p1 = szName;
    while (p1 && *p1 != 0)
    {
      DWORD len = strlen(p1);
      /* check if given name contains wildcard */
      if (len && *p1 == '*')
      {
        DWORD hostlen = strlen(pszServerName);
        if (hostlen < len)
          break;
        if (!stricmp(pszServerName + hostlen - len + 1, p1 + 1))
        {
          rc = 0;
          goto end;
        }
      }
      else if (!stricmp(pszServerName, p1))
      {
        rc = 0;
        goto end;
      }
      p1 += (len + 1);
    }
    pvio->set_error(pvio->mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                     "SSL connection error: Name of server certificate didn't match");
  }
end:
  if (szName)
    LocalFree(szName);
  if (pServerCert)
    CertFreeCertificateContext(pServerCert);
  return rc;
}

static const char *cipher_name(const SecPkgContext_CipherInfo *CipherInfo)
{
  int i;

  for(i = 0; i < sizeof(cipher_map)/sizeof(cipher_map[0]) ; i++)
  {
    if (CipherInfo->dwCipherSuite == cipher_map[i].cipher_id)
      return cipher_map[i].openssl_name;
  }
  return "";
};

const char *ma_tls_get_cipher(MARIADB_TLS *ctls)
{
  SecPkgContext_CipherInfo CipherInfo = { SECPKGCONTEXT_CIPHERINFO_V1 };
  SECURITY_STATUS sRet;
  SC_CTX *sctx;
  DWORD i= 0;

  if (!ctls || !ctls->ssl)
    return NULL;

  sctx= (SC_CTX *)ctls->ssl;

  sRet= QueryContextAttributes(&sctx->ctxt, SECPKG_ATTR_CIPHER_INFO, (PVOID)&CipherInfo);
  if (sRet != SEC_E_OK)
    return NULL;

  return cipher_name(&CipherInfo);
}

unsigned int ma_tls_get_finger_print(MARIADB_TLS *ctls, char *fp, unsigned int len)
{
  SC_CTX *sctx= (SC_CTX *)ctls->ssl;
  PCCERT_CONTEXT pRemoteCertContext = NULL;
  if (QueryContextAttributes(&sctx->ctxt, SECPKG_ATTR_REMOTE_CERT_CONTEXT, (PVOID)&pRemoteCertContext) != SEC_E_OK)
    return 0;
  CertGetCertificateContextProperty(pRemoteCertContext, CERT_HASH_PROP_ID, fp, (DWORD *)&len);
  CertFreeCertificateContext(pRemoteCertContext);
  return len;
}
