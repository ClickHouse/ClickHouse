#ifndef _ma_tls_h_
#define _ma_tls_h_

enum enum_pvio_tls_type {
  SSL_TYPE_DEFAULT=0,
#ifdef _WIN32
  SSL_TYPE_SCHANNEL,
#endif
  SSL_TYPE_OPENSSL,
  SSL_TYPE_GNUTLS
};

typedef struct st_ma_pvio_tls {
  void *data;
  MARIADB_PVIO *pvio;
  void *ssl;
} MARIADB_TLS;

struct st_ssl_version {
  unsigned int iversion;
  char *cversion;
};

/* Function prototypes */

/* ma_tls_start
   initializes the ssl library
   Parameter:
     errmsg      pointer to error message buffer
     errmsg_len  length of error message buffer
   Returns:
     0           success
     1           if an error occured
   Notes:
     On success the global variable ma_tls_initialized will be set to 1
*/
int ma_tls_start(char *errmsg, size_t errmsg_len);

/* ma_tls_end
   unloads/deinitializes ssl library and unsets global variable
   ma_tls_initialized
*/
void ma_tls_end(void);

/* ma_tls_init
   creates a new SSL structure for a SSL connection and loads
   client certificates

   Parameters:
     MYSQL        a mysql structure
   Returns:
     void *       a pointer to internal SSL structure
*/
void * ma_tls_init(MYSQL *mysql);

/* ma_tls_connect
   performs SSL handshake
   Parameters:
     MARIADB_TLS   MariaDB SSL container
   Returns:
     0             success
     1             error
*/
my_bool ma_tls_connect(MARIADB_TLS *ctls);

/* ma_tls_read
   reads up to length bytes from socket
   Parameters:
     ctls         MariaDB SSL container
     buffer       read buffer
     length       buffer length
   Returns:
     0-n          bytes read
     -1           if an error occured
*/
ssize_t ma_tls_read(MARIADB_TLS *ctls, const uchar* buffer, size_t length);

/* ma_tls_write
   write buffer to socket
   Parameters:
     ctls         MariaDB SSL container
     buffer       write buffer
     length       buffer length
   Returns:
     0-n          bytes written
     -1           if an error occured
*/
ssize_t ma_tls_write(MARIADB_TLS *ctls, const uchar* buffer, size_t length);

/* ma_tls_close
   closes SSL connection and frees SSL structure which was previously
   created by ma_tls_init call
   Parameters:
     MARIADB_TLS  MariaDB SSL container
   Returns:
     0            success
     1            error
*/
my_bool ma_tls_close(MARIADB_TLS *ctls);

/* ma_tls_verify_server_cert
   validation check of server certificate
   Parameter:
     MARIADB_TLS  MariaDB SSL container
   Returns:
     ß            success
     1            error
*/
int ma_tls_verify_server_cert(MARIADB_TLS *ctls);

/* ma_tls_get_cipher
   returns cipher for current ssl connection
   Parameter:
     MARIADB_TLS  MariaDB SSL container
   Returns: 
     cipher in use or
     NULL on error
*/
const char *ma_tls_get_cipher(MARIADB_TLS *ssl);

/* ma_tls_get_finger_print
   returns SHA1 finger print of server certificate
   Parameter:
     MARIADB_TLS  MariaDB SSL container
     fp           buffer for fingerprint
     fp_len       buffer length
   Returns:
     actual size of finger print
*/
unsigned int ma_tls_get_finger_print(MARIADB_TLS *ctls, char *fp, unsigned int fp_len);

/* ma_tls_get_protocol_version 
   returns protocol version in use
   Parameter:
     MARIADB_TLS    MariaDB SSL container
     version        pointer to ssl version info
   Returns:
     0              success
     1              error
*/
my_bool ma_tls_get_protocol_version(MARIADB_TLS *ctls, struct st_ssl_version *version);

/* Function prototypes */
MARIADB_TLS *ma_pvio_tls_init(MYSQL *mysql);
my_bool ma_pvio_tls_connect(MARIADB_TLS *ctls);
ssize_t ma_pvio_tls_read(MARIADB_TLS *ctls, const uchar *buffer, size_t length);
ssize_t ma_pvio_tls_write(MARIADB_TLS *ctls, const uchar *buffer, size_t length);
my_bool ma_pvio_tls_close(MARIADB_TLS *ctls);
int ma_pvio_tls_verify_server_cert(MARIADB_TLS *ctls);
const char *ma_pvio_tls_cipher(MARIADB_TLS *ctls);
my_bool ma_pvio_tls_check_fp(MARIADB_TLS *ctls, const char *fp, const char *fp_list);
my_bool ma_pvio_start_ssl(MARIADB_PVIO *pvio);
my_bool ma_pvio_tls_get_protocol_version(MARIADB_TLS *ctls, struct st_ssl_version *version);
void ma_pvio_tls_end();

#endif /* _ma_tls_h_ */
