/**
 * C interface to the Poco-based HTTP(S) transport used by rdhttp_poco.c,
 * the ClickHouse replacement for the libcurl-based rdhttp.c.
 */

#ifndef _RDHTTP_POCO_TRANSPORT_H_
#define _RDHTTP_POCO_TRANSPORT_H_

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef size_t (*rd_http_poco_write_cb_t)(char *ptr,
                                          size_t size,
                                          size_t nmemb,
                                          void *userdata);

/**
 * Performs a blocking HTTP(S) request without following redirects,
 * mirroring the behavior of the libcurl-based `rd_http_req_perform_sync`.
 *
 * @param headers Array of "Name: value" strings.
 * @param post_fields When NULL, a GET request is performed, otherwise a POST
 *                    with this body.
 * @param timeout_s Total request timeout, 0 means a default timeout.
 * @param ca_location CA certificate file or directory, NULL or "probe" means
 *                    the default verification paths.
 * @param ca_pem CA certificates in PEM format, used when \p ca_location
 *               is not set.
 * @param content_type (out) The response `Content-Type`, allocated with
 *                     malloc, set to NULL when the header is missing.
 *
 * @returns 0 on transport success with \p response_code set (any HTTP status,
 *          including errors), -1 on transport failure with \p errstr set.
 */
int rd_http_poco_perform(const char *url,
                         const char **headers,
                         size_t headers_cnt,
                         const char *post_fields,
                         size_t post_fields_size,
                         int timeout_s,
                         const char *ca_location,
                         const char *ca_pem,
                         rd_http_poco_write_cb_t write_cb,
                         void *write_opaque,
                         long *response_code,
                         char **content_type,
                         char *errstr,
                         size_t errstr_size);

#ifdef __cplusplus
}
#endif

#endif /* _RDHTTP_POCO_TRANSPORT_H_ */
