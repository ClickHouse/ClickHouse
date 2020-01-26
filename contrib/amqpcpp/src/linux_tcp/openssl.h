/**
 *  OpenSSL.h
 * 
 *  Header file in which we list all openssl functions in our own namespace 
 *  that we call instead of the actual openssl functions. This allows us to 
 *  intercept the calls and forward them to a dynamically loaded namespace
 *  
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <openssl/ssl.h>
#include <openssl/err.h>

/**
 *  Begin of namespace
 */
namespace AMQP { namespace OpenSSL {

/**
 *  Function to check if openssl is loaded
 *  @return bool
 */
bool valid();

/**
 *  List of all wrapper methods that are in use inside AMQP-CPP
 */
const SSL_METHOD *TLS_client_method();
SSL_CTX *SSL_CTX_new(const SSL_METHOD *method);
SSL     *SSL_new(SSL_CTX *ctx);
int      SSL_do_handshake(SSL *ssl);
int      SSL_read(SSL *ssl, void *buf, int num);
int      SSL_write(SSL *ssl, const void *buf, int num);
int      SSL_shutdown(SSL *ssl);
int      SSL_pending(const SSL *ssl);
int      SSL_set_fd(SSL *ssl, int fd);
int      SSL_get_shutdown(const SSL *ssl);
int      SSL_get_error(const SSL *ssl, int ret);
int      SSL_use_certificate_file(SSL *ssl, const char *file, int type);
void     SSL_set_connect_state(SSL *ssl);
void     SSL_CTX_free(SSL_CTX *ctx);
void     SSL_free(SSL *ssl);
long     SSL_ctrl(SSL *ssl, int cmd, long larg, void *parg);
long     SSL_CTX_ctrl(SSL_CTX *ctx, int cmd, long larg, void *parg);
void     ERR_clear_error(void);

/**
 *  End of namespace
 */
}}

