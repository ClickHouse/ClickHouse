/************************************************************************************
  Copyright (C) 2012 Monty Program AB

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
#include <ma_global.h>
#include <ma_sys.h>
#include <ma_common.h>
#include <ma_pvio.h>
#include <errmsg.h>
#include <string.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <openssl/ssl.h> /* SSL and SSL_CTX */
#include <openssl/err.h> /* error reporting */
#include <openssl/conf.h>
#include <openssl/md4.h>

#if OPENSSL_VERSION_NUMBER >= 0x10002000L && !defined(LIBRESSL_VERSION_NUMBER)
#include <openssl/x509v3.h>
#define HAVE_OPENSSL_CHECK_HOST 1
#endif

#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
#define HAVE_OPENSSL_1_1_API
#endif

#ifdef HAVE_TLS_SESSION_CACHE
#undef HAVE_TLS_SESSION_CACHE
#endif
#if OPENSSL_USE_BIOMETHOD
#undef OPENSSL_USE_BIOMETHOD
#endif
#ifndef HAVE_OPENSSL_DEFAULT
#include <memory.h>
#define ma_malloc(A,B) malloc((A))
#undef ma_free
#define ma_free(A) free((A))
#define ma_snprintf snprintf
#define ma_vsnprintf vsnprintf
#undef SAFE_MUTEX
#endif
#include <ma_pthread.h>

extern my_bool ma_tls_initialized;
extern unsigned int mariadb_deinitialize_ssl;

#define MAX_SSL_ERR_LEN 100

static pthread_mutex_t LOCK_openssl_config;
#ifndef HAVE_OPENSSL_1_1_API
static pthread_mutex_t *LOCK_crypto= NULL;
#endif
#if OPENSSL_USE_BIOMETHOD
static int ma_bio_read(BIO *h, char *buf, int size);
static int ma_bio_write(BIO *h, const char *buf, int size);
static BIO_METHOD ma_BIO_method;
#endif

static void ma_tls_set_error(MYSQL *mysql)
{
  ulong ssl_errno= ERR_get_error();
  char  ssl_error[MAX_SSL_ERR_LEN];
  const char *ssl_error_reason;
  MARIADB_PVIO *pvio= mysql->net.pvio;

  if (!ssl_errno)
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, "Unknown SSL error");
    return;
  }
  if ((ssl_error_reason= ERR_reason_error_string(ssl_errno)))
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 
                   0, ssl_error_reason);
    return;
  }
  snprintf(ssl_error, MAX_SSL_ERR_LEN, "SSL errno=%lu", ssl_errno);
  pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 0, ssl_error);
  return;
}

#ifndef HAVE_OPENSSL_1_1_API
/*
   thread safe callbacks for OpenSSL
   Crypto call back functions will be
   set during ssl_initialization
 */
#if OPENSSL_VERSION_NUMBER < 0x10000000L
static unsigned long my_cb_threadid(void)
{
  /* cast pthread_t to unsigned long */
  return (unsigned long) pthread_self();
}
#else
static void my_cb_threadid(CRYPTO_THREADID *id)
{
  CRYPTO_THREADID_set_numeric(id, (unsigned long)pthread_self());
}
#endif
#endif

#ifdef HAVE_TLS_SESSION_CACHE
typedef struct st_ma_tls_session {
  char md4_hash[17];
  SSL_SESSION *session;
} MA_SSL_SESSION;

MA_SSL_SESSION *ma_tls_sessions= NULL;
int ma_tls_session_cache_size= 128;

static char *ma_md4_hash(const char *host, const char *user, unsigned int port, char *md4)
{
  char buffer[195]; /* MAX_USERNAME_LEN + MAX_HOST_NAME_LEN + 2 + 5 */
  snprintf(buffer, 194, "%s@%s:%d", user ? user : "", host, port);
  buffer[194]= 0;
  MD4((unsigned char *)buffer, strlen(buffer), (unsigned char *)md4);
  return md4;
}

MA_SSL_SESSION *ma_tls_get_session(MYSQL *mysql)
{
  char md4[17];
  int i;

  if (!ma_tls_sessions)
    return NULL;

  memset(md4, 0, 16);
  ma_md4_hash(mysql->host, mysql->user, mysql->port, md4);
  for (i=0; i < ma_tls_session_cache_size; i++)
  {
    if (ma_tls_sessions[i].session &&
        !strncmp(ma_tls_sessions[i].md4_hash, md4, 16))
    {
      return &ma_tls_sessions[i];
    }
  }
  return NULL;
}


#if OPENSSL_USE_BIOMETHOD
static int ma_bio_read(BIO *bio, char *buf, int size)
{
  MARIADB_PVIO *pvio= (MARIADB_PVIO *)bio->ptr;
  size_t rc;

  rc= pvio->methods->read(pvio, buf, (size_t)size);
  BIO_clear_retry_flags(bio);
  return (int)rc;
}
static int ma_bio_write(BIO *bio, const char *buf, int size)
{
  MARIADB_PVIO *pvio= (MARIADB_PVIO *)bio->ptr;
  size_t rc;

  rc= pvio->methods->write(pvio, buf, (size_t)size);
  BIO_clear_retry_flags(bio);
  return (int)rc;
}
#endif

static int ma_tls_session_cb(SSL *ssl, SSL_SESSION *session)
{
  MYSQL *mysql;
  MA_SSL_SESSION *stored_session;
  int i;

  mysql= (MYSQL *)SSL_get_app_data(ssl);

  /* check if we already stored session key */
  if ((stored_session= ma_tls_get_session(mysql)))
  {
    SSL_SESSION_free(stored_session->session);
    stored_session->session= session;
    return 1;
  }

  for (i=0; i < ma_tls_session_cache_size; i++)
  {
    if (!ma_tls_sessions[i].session)
    {
      ma_md4_hash(mysql->host, mysql->user, mysql->port, ma_tls_sessions[i].md4_hash);
      ma_tls_sessions[i].session= session;
    }
    return 1;
  }
  return 0;
}

static void ma_tls_remove_session_cb(SSL_CTX* ctx __attribute__((unused)), 
                                     SSL_SESSION* session)
{
  int i;
  for (i=0; i < ma_tls_session_cache_size; i++)
    if (session == ma_tls_sessions[i].session)
    {
      ma_tls_sessions[i].md4_hash[0]= 0;
      SSL_SESSION_free(ma_tls_sessions[i].session);
      ma_tls_sessions[i].session= NULL;
    }
}
#endif

#ifndef HAVE_OPENSSL_1_1_API
static void my_cb_locking(int mode, int n,
                          const char *file __attribute__((unused)),
                          int line __attribute__((unused)))
{
  if (mode & CRYPTO_LOCK)
    pthread_mutex_lock(&LOCK_crypto[n]);
  else
    pthread_mutex_unlock(&LOCK_crypto[n]);
}

static int ssl_thread_init()
{
  int i, max= CRYPTO_num_locks();

  if (LOCK_crypto == NULL)
  {
    if (!(LOCK_crypto=
          (pthread_mutex_t *)ma_malloc(sizeof(pthread_mutex_t) * max, MYF(0))))
      return 1;

    for (i=0; i < max; i++)
      pthread_mutex_init(&LOCK_crypto[i], NULL);
  }

#if OPENSSL_VERSION_NUMBER < 0x10000000L
  CRYPTO_set_id_callback(my_cb_threadid);
#else
  CRYPTO_THREADID_set_callback(my_cb_threadid);
#endif
  CRYPTO_set_locking_callback(my_cb_locking);

  return 0;
}
#endif

#if defined(_WIN32) || !defined(DISABLE_SIGPIPE)
#define disable_sigpipe()
#else
#include  <signal.h>
static void ma_sigpipe_handler()
{
}

static void disable_sigpipe()
{
  struct sigaction old_handler, new_handler={NULL};
  if (!sigaction (SIGPIPE, NULL, &old_handler) &&
    !old_handler.sa_handler)
  {
    new_handler.sa_handler= ma_sigpipe_handler;
    new_handler.sa_flags= 0;
    if (!sigemptyset(&new_handler.sa_mask))
      sigaction(SIGPIPE, &new_handler, NULL);
  }
}
#endif

/*
  Initializes SSL

  SYNOPSIS
    my_ssl_start
      mysql        connection handle

  RETURN VALUES
    0  success
    1  error
*/
int ma_tls_start(char *errmsg __attribute__((unused)), size_t errmsg_len __attribute__((unused)))
{
  int rc= 1;
  if (ma_tls_initialized)
    return 0;

  /* lock mutex to prevent multiple initialization */
  pthread_mutex_init(&LOCK_openssl_config, NULL);
  pthread_mutex_lock(&LOCK_openssl_config);
#ifdef HAVE_OPENSSL_1_1_API
  if (!OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, NULL))
    goto end;
#else
  if (ssl_thread_init())
  {
    strncpy(errmsg, "Not enough memory", errmsg_len);
    goto end;
  }
  SSL_library_init();
#if SSLEAY_VERSION_NUMBER >= 0x00907000L
  OPENSSL_config(NULL);
#endif
#endif
#ifndef HAVE_OPENSSL_1_1_API
  /* load errors */
  SSL_load_error_strings();
  /* digests and ciphers */
  OpenSSL_add_all_algorithms();
#endif
  disable_sigpipe();
#if OPENSSL_USE_BIOMETHOD
  memcpy(&ma_BIO_method, BIO_s_socket(), sizeof(BIO_METHOD));
  ma_BIO_method.bread= ma_bio_read;
  ma_BIO_method.bwrite= ma_bio_write;
#endif
  rc= 0;
  ma_tls_initialized= TRUE;
end:
  pthread_mutex_unlock(&LOCK_openssl_config);
  return rc;
}

/*
   Release SSL and free resources
   Will be automatically executed by 
   mysql_server_end() function

   SYNOPSIS
     my_ssl_end()
       void

   RETURN VALUES
     void
*/
void ma_tls_end()
{
  if (ma_tls_initialized)
  {
    pthread_mutex_lock(&LOCK_openssl_config);
#ifndef HAVE_OPENSSL_1_1_API
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);
    {
      int i;
      for (i=0; i < CRYPTO_num_locks(); i++)
        pthread_mutex_destroy(&LOCK_crypto[i]);
    }
    ma_free((gptr)LOCK_crypto);
    LOCK_crypto= NULL;
#endif
    if (mariadb_deinitialize_ssl)
    {
#ifndef HAVE_OPENSSL_1_1_API
#if OPENSSL_VERSION_NUMBER < 0x10100000L
      ERR_remove_state(0);
#endif
      EVP_cleanup();
      CRYPTO_cleanup_all_ex_data();
      ERR_free_strings();
      CONF_modules_free();
      CONF_modules_unload(1);
#endif
    }
    ma_tls_initialized= FALSE;
    pthread_mutex_unlock(&LOCK_openssl_config);
    pthread_mutex_destroy(&LOCK_openssl_config);
  }
  return;
}

int ma_tls_get_password(char *buf, int size,
                        int rwflag __attribute__((unused)),
                        void *userdata)
{
  memset(buf, 0, size);
  if (userdata)
    strncpy(buf, (char *)userdata, size);
  return strlen(buf);
}


static int ma_tls_set_certs(MYSQL *mysql, SSL *ssl)
{
  char *certfile= mysql->options.ssl_cert,
       *keyfile= mysql->options.ssl_key;
  char *pw= (mysql->options.extension) ?
            mysql->options.extension->tls_pw : NULL;
  SSL_CTX *ctx= SSL_get_SSL_CTX(ssl);

  
  /* add cipher */
  if ((mysql->options.ssl_cipher &&
        mysql->options.ssl_cipher[0] != 0) &&
      SSL_set_cipher_list(ssl, mysql->options.ssl_cipher) == 0)
    goto error;

  /* ca_file and ca_path */
  if (!SSL_CTX_load_verify_locations(ctx,
                                    mysql->options.ssl_ca,
                                    mysql->options.ssl_capath))
  {
    if (mysql->options.ssl_ca || mysql->options.ssl_capath)
      goto error;
    if (SSL_CTX_set_default_verify_paths(ctx) == 0)
      goto error;
  }

  if (keyfile && !certfile)
    certfile= keyfile;
  if (certfile && !keyfile)
    keyfile= certfile;

  /* set cert */
  if (certfile  && certfile[0] != 0)
  {
    if (SSL_CTX_use_certificate_chain_file(ctx, certfile) != 1 ||
        SSL_use_certificate_file(ssl, certfile, SSL_FILETYPE_PEM) != 1)
      goto error; 
  }
  if (keyfile && keyfile[0])
  {
    FILE *fp;
    if ((fp= fopen(keyfile, "rb")))
    {
      EVP_PKEY *key= EVP_PKEY_new();
      PEM_read_PrivateKey(fp, &key, NULL, pw);
      fclose(fp);
      if (SSL_use_PrivateKey(ssl, key) != 1)
      {
        unsigned long err= ERR_peek_error();
        EVP_PKEY_free(key);
        if (!(ERR_GET_LIB(err) == ERR_LIB_X509 &&
	            ERR_GET_REASON(err) == X509_R_CERT_ALREADY_IN_HASH_TABLE))
          goto error;
      }
      EVP_PKEY_free(key);
    } else {
      my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 
                   CER(CR_FILE_NOT_FOUND), keyfile);
      return 1;
    }
  }
  /* verify key */
  if (certfile && !SSL_check_private_key(ssl))
    goto error;
  
  if (mysql->options.extension &&
      (mysql->options.extension->ssl_crl || mysql->options.extension->ssl_crlpath))
  {
    X509_STORE *certstore;

    if ((certstore= SSL_CTX_get_cert_store(ctx)))
    {
      if (X509_STORE_load_locations(certstore, mysql->options.extension->ssl_crl,
                                               mysql->options.extension->ssl_crlpath) == 0)
        goto error;

      X509_STORE_set_flags(certstore, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
    }
  }
  SSL_CTX_set_verify(ctx, (mysql->options.ssl_ca || mysql->options.ssl_capath)?
                     SSL_VERIFY_PEER : SSL_VERIFY_NONE, NULL);
  return 0;

error:
  ma_tls_set_error(mysql);
  return 1;
}

void *ma_tls_init(MYSQL *mysql)
{
  SSL *ssl= NULL;
  SSL_CTX *ctx= NULL;
#ifdef HAVE_TLS_SESSION_CACHE
  MA_SSL_SESSION *session= ma_tls_get_session(mysql);
#endif
  pthread_mutex_lock(&LOCK_openssl_config);

  #if OPENSSL_VERSION_NUMBER >= 0x10100000L
  if (!(ctx= SSL_CTX_new(TLS_client_method())))
#else
  if (!(ctx= SSL_CTX_new(SSLv23_client_method())))
#endif
    goto error;
  SSL_CTX_set_options(ctx, SSL_OP_ALL);
#ifdef HAVE_TLS_SESSION_CACHE
  SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_CLIENT);
  ma_tls_sessions= (MA_SSL_SESSION *)calloc(1, sizeof(struct st_ma_tls_session) * ma_tls_session_cache_size);
  SSL_CTX_sess_set_new_cb(ctx, ma_tls_session_cb);
  SSL_CTX_sess_set_remove_cb(ctx, ma_tls_remove_session_cb);
#endif

  if (!(ssl= SSL_new(ctx)))
    goto error;

  if (ma_tls_set_certs(mysql, ssl))
  {
    goto error;
  }

  if (!SSL_set_app_data(ssl, mysql))
    goto error;

#ifdef HAVE_TLS_SESSION_CACHE
  if (session)
    SSL_set_session(ssl, session->session);
#endif

  pthread_mutex_unlock(&LOCK_openssl_config);
  return (void *)ssl;
error:
  pthread_mutex_unlock(&LOCK_openssl_config);
  if (ctx)
    SSL_CTX_free(ctx);
  if (ssl)
    SSL_free(ssl);
  return NULL;
}

my_bool ma_tls_connect(MARIADB_TLS *ctls)
{
  SSL *ssl = (SSL *)ctls->ssl;
  my_bool blocking, try_connect= 1;
  MYSQL *mysql;
  MARIADB_PVIO *pvio;
  int rc;
#if OPENSSL_USE_BIOMETHOD
  BIO_METHOD *bio_method= NULL;
  BIO *bio;
#endif

  mysql= (MYSQL *)SSL_get_app_data(ssl);
  pvio= mysql->net.pvio;

  /* Set socket to non blocking if not already set */
  if (!(blocking= pvio->methods->is_blocking(pvio)))
    pvio->methods->blocking(pvio, FALSE, 0);

  SSL_clear(ssl);

#if OPENSSL_USE_BIOMETHOD
  bio= BIO_new(&ma_BIO_method);
  bio->ptr= pvio;
  SSL_set_bio(ssl, bio, bio);
  BIO_set_fd(bio, mysql_get_socket(mysql), BIO_NOCLOSE);
#else
  SSL_set_fd(ssl, mysql_get_socket(mysql));
#endif

  while (try_connect && (rc= SSL_connect(ssl)) == -1)
  {
    switch(SSL_get_error(ssl, rc)) {
    case SSL_ERROR_WANT_READ:
      if (pvio->methods->wait_io_or_timeout(pvio, TRUE, mysql->options.connect_timeout) < 1)
        try_connect= 0;
      break;
    case SSL_ERROR_WANT_WRITE:
      if (pvio->methods->wait_io_or_timeout(pvio, TRUE, mysql->options.connect_timeout) < 1)
        try_connect= 0;
      break;
    default:
      try_connect= 0;
    }
  }
  if (rc != 1)
  {
    ma_tls_set_error(mysql);
    /* restore blocking mode */
    if (!blocking)
      pvio->methods->blocking(pvio, FALSE, 0);
    return 1;
  }
  if ((mysql->client_flag & CLIENT_SSL_VERIFY_SERVER_CERT))
  {
    rc= SSL_get_verify_result(ssl);
    if (rc != X509_V_OK)
    {
      my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN, 
                   ER(CR_SSL_CONNECTION_ERROR), X509_verify_cert_error_string(rc));
      /* restore blocking mode */
      if (!blocking)
        pvio->methods->blocking(pvio, FALSE, 0);

      return 1;
    }
  }
  pvio->ctls->ssl= ctls->ssl= (void *)ssl;

  return 0;
}

ssize_t ma_tls_read(MARIADB_TLS *ctls, const uchar* buffer, size_t length)
{
  return SSL_read((SSL *)ctls->ssl, (void *)buffer, (int)length);
}

ssize_t ma_tls_write(MARIADB_TLS *ctls, const uchar* buffer, size_t length)
{ 
  return SSL_write((SSL *)ctls->ssl, (void *)buffer, (int)length);
}

my_bool ma_tls_close(MARIADB_TLS *ctls)
{
  int i, rc;
  SSL *ssl;
  SSL_CTX *ctx= NULL;

  if (!ctls || !ctls->ssl)
    return 1;
  ssl= (SSL *)ctls->ssl;
  ctx= SSL_get_SSL_CTX(ssl);
  if (ctx)
    SSL_CTX_free(ctx);

  SSL_set_quiet_shutdown(ssl, 1); 
  /* 2 x pending + 2 * data = 4 */ 
  for (i=0; i < 4; i++)
    if ((rc= SSL_shutdown(ssl)))
      break;

  /* Since we transferred ownership of BIO to ssl, BIO will
     automatically freed - no need for an explicit BIO_free_all */

  SSL_free(ssl);
  ctls->ssl= NULL;

  return rc;
}

int ma_tls_verify_server_cert(MARIADB_TLS *ctls)
{
  X509 *cert;
  MYSQL *mysql;
  SSL *ssl;
  MARIADB_PVIO *pvio;
#if !defined(HAVE_OPENSSL_CHECK_HOST)
  X509_NAME *x509sn;
  int cn_pos;
  X509_NAME_ENTRY *cn_entry;
  ASN1_STRING *cn_asn1;
  const char *cn_str;
#endif

  if (!ctls || !ctls->ssl)
    return 1;
  ssl= (SSL *)ctls->ssl;

  mysql= (MYSQL *)SSL_get_app_data(ssl);
  pvio= mysql->net.pvio;

  if (!mysql->host)
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                    ER(CR_SSL_CONNECTION_ERROR), "Invalid (empty) hostname");
    return 1;
  }

  if (!(cert= SSL_get_peer_certificate(ssl)))
  {
    pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                    ER(CR_SSL_CONNECTION_ERROR), "Unable to get server certificate");
    return 1;
  }
#ifdef HAVE_OPENSSL_CHECK_HOST
  if (X509_check_host(cert, mysql->host, 0, 0, 0) != 1)
    goto error;
#else
  x509sn= X509_get_subject_name(cert);

  if ((cn_pos= X509_NAME_get_index_by_NID(x509sn, NID_commonName, -1)) < 0)
    goto error;

  if (!(cn_entry= X509_NAME_get_entry(x509sn, cn_pos)))
    goto error;

  if (!(cn_asn1 = X509_NAME_ENTRY_get_data(cn_entry)))
    goto error;

  cn_str = (char *)ASN1_STRING_data(cn_asn1);

  /* Make sure there is no embedded \0 in the CN */
  if ((size_t)ASN1_STRING_length(cn_asn1) != strlen(cn_str))
    goto error;

  if (strcmp(cn_str, mysql->host))
    goto error;
#endif
  X509_free(cert);

  return 0;
error:
  X509_free(cert);

  pvio->set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                  ER(CR_SSL_CONNECTION_ERROR), "Validation of SSL server certificate failed");
  return 1;
}

const char *ma_tls_get_cipher(MARIADB_TLS *ctls)
{
  if (!ctls || !ctls->ssl)
    return NULL;
  return SSL_get_cipher_name(ctls->ssl);
}

unsigned int ma_tls_get_finger_print(MARIADB_TLS *ctls, char *fp, unsigned int len)
{
  X509 *cert= NULL;
  MYSQL *mysql;
  unsigned int fp_len;

  if (!ctls || !ctls->ssl)
    return 0;

  mysql= SSL_get_app_data(ctls->ssl);

  if (!(cert= SSL_get_peer_certificate(ctls->ssl)))
  {
    my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                        ER(CR_SSL_CONNECTION_ERROR), 
                        "Unable to get server certificate");
    goto end;
  }

  if (len < EVP_MAX_MD_SIZE)
  {
    my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                        ER(CR_SSL_CONNECTION_ERROR), 
                        "Finger print buffer too small");
    goto end;
  }
  if (!X509_digest(cert, EVP_sha1(), (unsigned char *)fp, &fp_len))
  {
    my_set_error(mysql, CR_SSL_CONNECTION_ERROR, SQLSTATE_UNKNOWN,
                        ER(CR_SSL_CONNECTION_ERROR), 
                        "invalid finger print of server certificate");
    goto end;
  }
  
  X509_free(cert);
  return (fp_len);
end:  
  X509_free(cert);
  return 0;
}


extern char *ssl_protocol_version[5];

my_bool ma_tls_get_protocol_version(MARIADB_TLS *ctls, struct st_ssl_version *version)
{
  SSL *ssl;

  if (!ctls || !ctls->ssl)
    return 1;

  ssl = (SSL *)ctls->ssl;
  version->iversion= SSL_version(ssl) - TLS1_VERSION;
  version->cversion= ssl_protocol_version[version->iversion];
  return 0;  
}

