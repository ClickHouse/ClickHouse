/**
 * Port of librdkafka's HTTP client (rdhttp.c) on top of a Poco-based
 * transport (rdhttp_poco_transport.cpp) instead of libcurl.
 *
 * Also implements the few libcurl functions that
 * rdkafka_sasl_oauthbearer_oidc.c calls directly (curl_slist_append,
 * curl_slist_free_all, curl_easy_escape): their declarations come from
 * rdhttp_curl_compat.h (via rdhttp.h), libcurl is not included nor linked.
 */

#include "rdkafka_int.h"
#include "rdunittest.h"

#include <ctype.h>
#include <stdarg.h>

#include "rdhttp.h"

#include "rdhttp_poco_transport.h"

/** Maximum response size, increase as necessary. */
#define RD_HTTP_RESPONSE_SIZE_MAX 1024 * 1024 * 500 /* 500kb */


/** Per-request state, stored in rd_http_req_t.hreq_curl, which is opaque
 *  to the users of this API. */
typedef struct rd_http_state_s {
        char *url;
        char **headers; /**< "Name: value" strings */
        size_t headers_cnt;
        char *post_fields;
        size_t post_fields_size;
        int timeout_s;
        const char *ca_location; /**< Points into rk_conf */
        const char *ca_pem;      /**< Points into rk_conf */
        char *content_type;      /**< Response content type */
} rd_http_state_t;

#define RD_HTTP_STATE(hreq) ((rd_http_state_t *)(hreq)->hreq_curl)


void rd_http_error_destroy(rd_http_error_t *herr) {
        rd_free(herr);
}

static rd_http_error_t *rd_http_error_new(int code, const char *fmt, ...)
    RD_FORMAT(printf, 2, 3);
static rd_http_error_t *rd_http_error_new(int code, const char *fmt, ...) {
        size_t len = 0;
        rd_http_error_t *herr;
        va_list ap;

        va_start(ap, fmt);

        if (fmt && *fmt) {
                va_list ap2;
                va_copy(ap2, ap);
                len = rd_vsnprintf(NULL, 0, fmt, ap2);
                va_end(ap2);
        }

        /* Use single allocation for both herr and the error string */
        herr         = rd_malloc(sizeof(*herr) + len + 1);
        herr->code   = code;
        herr->errstr = herr->data;

        if (len > 0)
                rd_vsnprintf(herr->errstr, len + 1, fmt, ap);
        else
                herr->errstr[0] = '\0';

        va_end(ap);

        return herr;
}

/**
 * @brief Same as rd_http_error_new() but reads the error string from the
 *        provided buffer.
 */
static rd_http_error_t *rd_http_error_new_from_buf(int code,
                                                   const rd_buf_t *rbuf) {
        rd_http_error_t *herr;
        rd_slice_t slice;
        size_t len = rd_buf_len(rbuf);

        if (len == 0)
                return rd_http_error_new(
                    code, "Server did not provide an error string");


        /* Use single allocation for both herr and the error string */
        herr         = rd_malloc(sizeof(*herr) + len + 1);
        herr->code   = code;
        herr->errstr = herr->data;
        rd_slice_init_full(&slice, rbuf);
        rd_slice_read(&slice, herr->errstr, len);
        herr->errstr[len] = '\0';

        return herr;
}

void rd_http_req_destroy(rd_http_req_t *hreq) {
        rd_http_state_t *state = RD_HTTP_STATE(hreq);

        if (state) {
                size_t i;
                for (i = 0; i < state->headers_cnt; i++)
                        rd_free(state->headers[i]);
                RD_IF_FREE(state->headers, rd_free);
                RD_IF_FREE(state->post_fields, rd_free);
                RD_IF_FREE(state->content_type, rd_free);
                RD_IF_FREE(state->url, rd_free);
                rd_free(state);
                hreq->hreq_curl = NULL;
        }
        RD_IF_FREE(hreq->hreq_buf, rd_buf_destroy_free);
}


/**
 * @brief Transport write callback. Writes the received bytes
 *        to the hreq's buffer.
 */
static size_t
rd_http_req_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
        rd_http_req_t *hreq = (rd_http_req_t *)userdata;

        if (unlikely(rd_buf_len(hreq->hreq_buf) + nmemb >
                     RD_HTTP_RESPONSE_SIZE_MAX))
                return 0;

        rd_buf_write(hreq->hreq_buf, ptr, nmemb);

        return nmemb;
}

static void rd_http_req_add_header(rd_http_req_t *hreq, const char *header) {
        rd_http_state_t *state = RD_HTTP_STATE(hreq);

        state->headers =
            rd_realloc(state->headers,
                       sizeof(*state->headers) * (state->headers_cnt + 1));
        state->headers[state->headers_cnt++] = rd_strdup(header);
}

/**
 * @brief Check if the error returned from HTTP(S) is temporary or not.
 *
 * @returns If the \p error_code is temporary, return rd_true,
 *          otherwise return rd_false.
 *
 * @locality Any thread.
 * @locks None.
 * @locks_acquired None.
 */
static rd_bool_t rd_http_is_failure_temporary(int error_code) {
        switch (error_code) {
        case 408: /**< Request timeout */
        case 425: /**< Too early */
        case 429: /**< Too many requests */
        case 500: /**< Internal server error */
        case 502: /**< Bad gateway */
        case 503: /**< Service unavailable */
        case 504: /**< Gateway timeout */
                return rd_true;

        default:
                return rd_false;
        }
}

rd_http_error_t *
rd_http_req_init(rd_kafka_t *rk, rd_http_req_t *hreq, const char *url) {
        rd_http_state_t *state;

        memset(hreq, 0, sizeof(*hreq));

        state            = rd_calloc(1, sizeof(*state));
        state->url       = rd_strdup(url);
        state->timeout_s = 30;
        if (rk) {
                state->ca_location = rk->rk_conf.https.ca_location;
                state->ca_pem      = rk->rk_conf.https.ca_pem;
        }

        hreq->hreq_curl = (CURL *)state;
        hreq->hreq_buf  = rd_buf_new(1, 1024);

        return NULL;
}

/**
 * @brief Synchronously (blockingly) perform the HTTP operation.
 */
rd_http_error_t *rd_http_req_perform_sync(rd_http_req_t *hreq) {
        rd_http_state_t *state = RD_HTTP_STATE(hreq);
        long code              = 0;
        int r;

        /* Discard the response of a previous (failed) attempt, if any. */
        if (rd_buf_len(hreq->hreq_buf) > 0) {
                rd_buf_destroy_free(hreq->hreq_buf);
                hreq->hreq_buf = rd_buf_new(1, 1024);
        }
        RD_IF_FREE(state->content_type, rd_free);
        state->content_type = NULL;

        r = rd_http_poco_perform(
            state->url, (const char **)state->headers, state->headers_cnt,
            state->post_fields, state->post_fields_size, state->timeout_s,
            state->ca_location, state->ca_pem, rd_http_req_write_cb, hreq,
            &code, &state->content_type, hreq->hreq_curl_errstr,
            sizeof(hreq->hreq_curl_errstr));
        if (unlikely(r != 0))
                return rd_http_error_new(-1, "%s", hreq->hreq_curl_errstr);

        hreq->hreq_code = (int)code;
        if (hreq->hreq_code >= 400)
                return rd_http_error_new_from_buf(hreq->hreq_code,
                                                  hreq->hreq_buf);

        return NULL;
}


int rd_http_req_get_code(const rd_http_req_t *hreq) {
        return hreq->hreq_code;
}

const char *rd_http_req_get_content_type(rd_http_req_t *hreq) {
        return RD_HTTP_STATE(hreq)->content_type;
}


/**
 * @brief Perform a blocking HTTP(S) request to \p url.
 *        Retries the request \p retries times with linear backoff.
 *        Interval of \p retry_ms milliseconds is used between retries.
 *
 * @param url The URL to perform the request to.
 * @param headers_array Array of HTTP(S) headers to set, each element
 *                      is a string in the form "key: value"
 * @param headers_array_cnt Number of elements in \p headers_array.
 * @param timeout_s Timeout in seconds for the request, 0 means default
 *                  `rd_http_req_init()` timeout.
 * @param retries Number of retries to perform on failure.
 * @param retry_ms Milliseconds to wait between retries.
 * @param rbufp (out) Pointer to a buffer that will be filled with the response.
 * @param content_type (out, optional) Pointer to a string that will be filled
 * with the content type of the response, if not NULL.
 * @param response_code (out, optional) Pointer to an integer that will be
 * filled with the HTTP response code, if not NULL.
 *
 * @return Returns NULL on success (HTTP response code < 400), or an error
 * object on transport or HTTP error.
 *
 * @remark Returned error object, when non-NULL, must be destroyed
 *         by calling rd_http_error_destroy().
 *
 * @locality Any thread.
 * @locks None.
 * @locks_acquired None.
 */
rd_http_error_t *rd_http_get(rd_kafka_t *rk,
                             const char *url,
                             char **headers_array,
                             size_t headers_array_cnt,
                             int timeout_s,
                             int retries,
                             int retry_ms,
                             rd_buf_t **rbufp,
                             char **content_type,
                             int *response_code) {
        rd_http_req_t hreq;
        rd_http_error_t *herr = NULL;
        char *header;
        int i;
        size_t len, j;

        *rbufp = NULL;
        if (content_type)
                *content_type = NULL;
        if (response_code)
                *response_code = -1;

        herr = rd_http_req_init(rk, &hreq, url);
        if (unlikely(herr != NULL))
                return herr;

        for (j = 0; j < headers_array_cnt; j++) {
                header = headers_array[j];
                if (header && *header)
                        rd_http_req_add_header(&hreq, header);
        }
        if (timeout_s > 0)
                RD_HTTP_STATE(&hreq)->timeout_s = timeout_s;

        for (i = 0; i <= retries; i++) {
                if (rd_kafka_terminating(rk)) {
                        herr = rd_http_error_new(-1, "Terminating");
                        goto done;
                }

                herr = rd_http_req_perform_sync(&hreq);
                len  = rd_buf_len(hreq.hreq_buf);

                if (!herr) {
                        if (len > 0)
                                break; /* Success */
                        /* Empty response */
                        goto done;
                }

                /* Retry if HTTP(S) request returns temporary error and there
                 * are remaining retries, else fail. */
                if (i == retries || !rd_http_is_failure_temporary(herr->code)) {
                        goto done;
                }

                /* Retry */
                rd_http_error_destroy(herr);
                rd_usleep(retry_ms * 1000 * (i + 1), &rk->rk_terminate);
        }

        *rbufp        = hreq.hreq_buf;
        hreq.hreq_buf = NULL;

        if (content_type) {
                const char *ct = rd_http_req_get_content_type(&hreq);
                if (ct && *ct)
                        *content_type = rd_strdup(ct);
                else
                        *content_type = NULL;
        }
        if (response_code)
                *response_code = hreq.hreq_code;

done:
        rd_http_req_destroy(&hreq);
        return herr;
}


/**
 * @brief Extract the JSON object from \p hreq and return it in \p *jsonp.
 *
 * @returns Returns NULL on success, or an JSON parsing error - this
 *          error object must be destroyed by calling rd_http_error_destroy().
 */
rd_http_error_t *rd_http_parse_json(rd_http_req_t *hreq, cJSON **jsonp) {
        size_t len;
        char *raw_json;
        const char *end = NULL;
        rd_slice_t slice;
        rd_http_error_t *herr = NULL;

        /* cJSON requires the entire input to parse in contiguous memory. */
        rd_slice_init_full(&slice, hreq->hreq_buf);
        len = rd_buf_len(hreq->hreq_buf);

        raw_json = rd_malloc(len + 1);
        rd_slice_read(&slice, raw_json, len);
        raw_json[len] = '\0';

        /* Parse JSON */
        *jsonp = kafka_cJSON_ParseWithOpts(raw_json, &end, 0);

        if (!*jsonp)
                herr = rd_http_error_new(hreq->hreq_code,
                                         "Failed to parse JSON response "
                                         "at %" PRIusz "/%" PRIusz,
                                         (size_t)(end - raw_json), len);
        rd_free(raw_json);
        return herr;
}


/**
 * @brief Perform a blocking HTTP(S) request to \p url with
 *        HTTP(S) headers and data with \p timeout_s.
 *        If the HTTP(S) request fails, will retry another \p retries times
 *        with multiplying backoff \p retry_ms.
 *
 * @returns The result will be returned in \p *jsonp.
 *          Returns NULL on success (HTTP response code < 400), or an error
 *          object on transport, HTTP error or a JSON parsing error - this
 *          error object must be destroyed by calling rd_http_error_destroy().
 *
 * @locality Any thread.
 */
rd_http_error_t *rd_http_post_expect_json(rd_kafka_t *rk,
                                          const char *url,
                                          const struct curl_slist *headers,
                                          const char *post_fields,
                                          size_t post_fields_size,
                                          int timeout_s,
                                          int retries,
                                          int retry_ms,
                                          cJSON **jsonp) {
        rd_http_error_t *herr;
        rd_http_req_t hreq;
        rd_http_state_t *state;
        const struct curl_slist *hdr;
        int i;
        size_t len;
        const char *content_type;

        herr = rd_http_req_init(rk, &hreq, url);
        if (unlikely(herr != NULL))
                return herr;

        for (hdr = headers; hdr; hdr = hdr->next)
                rd_http_req_add_header(&hreq, hdr->data);

        state            = RD_HTTP_STATE(&hreq);
        state->timeout_s = timeout_s;

        state->post_fields = rd_malloc(post_fields_size > 0 ? post_fields_size
                                                            : 1);
        memcpy(state->post_fields, post_fields, post_fields_size);
        state->post_fields_size = post_fields_size;

        for (i = 0; i <= retries; i++) {
                if (rd_kafka_terminating(rk)) {
                        rd_http_req_destroy(&hreq);
                        return rd_http_error_new(-1, "Terminating");
                }

                herr = rd_http_req_perform_sync(&hreq);
                len  = rd_buf_len(hreq.hreq_buf);

                if (!herr) {
                        if (len > 0)
                                break; /* Success */
                        /* Empty response */
                        rd_http_req_destroy(&hreq);
                        return NULL;
                }
                /* Retry if HTTP(S) request returns temporary error and there
                 * are remaining retries, else fail. */
                if (i == retries || !rd_http_is_failure_temporary(herr->code)) {
                        rd_http_req_destroy(&hreq);
                        return herr;
                }

                /* Retry */
                rd_http_error_destroy(herr);
                rd_usleep(retry_ms * 1000 * (i + 1), &rk->rk_terminate);
        }

        content_type = rd_http_req_get_content_type(&hreq);

        if (!content_type || rd_strncasecmp(content_type, "application/json",
                                            strlen("application/json"))) {
                if (!herr)
                        herr = rd_http_error_new(
                            hreq.hreq_code, "Response is not JSON encoded: %s",
                            content_type ? content_type : "(n/a)");
                rd_http_req_destroy(&hreq);
                return herr;
        }

        herr = rd_http_parse_json(&hreq, jsonp);

        rd_http_req_destroy(&hreq);

        return herr;
}

/**
 * @brief Append \p params to \p url, taking care of existing query parameters
 *        and hash fragments. \p params must be already URL encoded.
 *
 * @returns A newly allocated string with the appended parameters or NULL
 *          on error.
 */
char *rd_http_get_params_append(const char *url, const char *params) {
        const char *host, *p, *query;
        const char *hash;
        size_t url_len, base_len, query_len, params_len;
        rd_bool_t need_slash = rd_true;
        char *result, *w;

        p = strstr(url, "://");
        if (!p || p == url)
                return NULL;
        host = p + 3;
        if (!*host || *host == '/' || *host == '?' || *host == '#')
                return NULL;

        /* The hash fragment, if any, is dropped. */
        hash    = strchr(url, '#');
        url_len = hash ? (size_t)(hash - url) : strlen(url);

        /* The base part ends where the query string starts and includes
         * the path, if any. */
        query    = NULL;
        base_len = url_len;
        for (p = host; p < url + url_len; p++) {
                if (*p == '/')
                        need_slash = rd_false;
                if (*p == '?') {
                        query    = p + 1;
                        base_len = (size_t)(p - url);
                        break;
                }
        }

        query_len = query ? (size_t)(url + url_len - query) : 0;
        /* Drop trailing '&' of the existing query string. */
        while (query_len > 0 && query[query_len - 1] == '&')
                query_len--;
        params_len = strlen(params);

        result = w = rd_malloc(base_len + 4 + query_len + params_len);
        memcpy(w, url, base_len);
        w += base_len;
        if (need_slash)
                *(w++) = '/';
        if (query_len > 0 || params_len > 0)
                *(w++) = '?';
        memcpy(w, query, query_len);
        w += query_len;
        if (query_len > 0 && params_len > 0)
                *(w++) = '&';
        memcpy(w, params, params_len);
        w += params_len;
        *w = '\0';

        return result;
}

/**
 * @brief Same as rd_http_get() but requires a JSON response.
 *        The response is parsed and a JSON object is returned in \p *jsonp.
 *
 * Same error semantics as rd_http_get().
 */
rd_http_error_t *rd_http_get_json(rd_kafka_t *rk,
                                  const char *url,
                                  char **headers_array,
                                  size_t headers_array_cnt,
                                  int timeout_s,
                                  int retries,
                                  int retry_ms,
                                  cJSON **jsonp) {
        rd_http_error_t *herr;
        int response_code;
        char *content_type;
        rd_buf_t *rbuf;
        char **headers_array_new =
            rd_calloc(headers_array_cnt + 1, sizeof(*headers_array_new));
        rd_http_req_t hreq;

        *jsonp = NULL;

        memcpy(headers_array_new, headers_array,
               headers_array_cnt * sizeof(*headers_array_new));
        headers_array_new[headers_array_cnt++] = "Accept: application/json";

        herr = rd_http_get(rk, url, headers_array_new, headers_array_cnt,
                           timeout_s, retries, retry_ms, &rbuf, &content_type,
                           &response_code);
        rd_free(headers_array_new);

        if (unlikely(herr != NULL))
                return herr;

        if (!content_type || rd_strncasecmp(content_type, "application/json",
                                            strlen("application/json"))) {
                herr = rd_http_error_new(response_code,
                                         "Response is not JSON encoded: %s",
                                         content_type ? content_type : "(n/a)");
                RD_IF_FREE(rbuf, rd_buf_destroy_free);
                RD_IF_FREE(content_type, rd_free);
                return herr;
        }

        memset(&hreq, 0, sizeof(hreq));
        hreq.hreq_buf  = rbuf;
        hreq.hreq_code = response_code;
        herr           = rd_http_parse_json(&hreq, jsonp);
        RD_IF_FREE(rbuf, rd_buf_destroy_free);
        RD_IF_FREE(content_type, rd_free);

        return herr;
}


void rd_http_global_init(void) {
}


/**
 * @name Implementation of the libcurl functions used by
 *       rdkafka_sasl_oauthbearer_oidc.c.
 *
 * The returned memory is allocated with malloc: rd_free and curl_free
 * are both compatible with it.
 */

struct curl_slist *curl_slist_append(struct curl_slist *list,
                                     const char *data) {
        struct curl_slist *item, *last;

        item = malloc(sizeof(*item));
        if (!item)
                return NULL;
        item->data = strdup(data);
        if (!item->data) {
                free(item);
                return NULL;
        }
        item->next = NULL;

        if (!list)
                return item;
        last = list;
        while (last->next)
                last = last->next;
        last->next = item;
        return list;
}

void curl_slist_free_all(struct curl_slist *list) {
        while (list) {
                struct curl_slist *next = list->next;
                free(list->data);
                free(list);
                list = next;
        }
}

char *curl_easy_escape(CURL *handle, const char *string, int length) {
        static const char hex[] = "0123456789ABCDEF";
        size_t len = length > 0 ? (size_t)length : strlen(string);
        char *result, *w;
        size_t i;

        (void)handle;

        result = w = malloc(len * 3 + 1);
        if (!result)
                return NULL;
        for (i = 0; i < len; i++) {
                unsigned char c = (unsigned char)string[i];
                if (isalnum(c) || c == '-' || c == '.' || c == '_' ||
                    c == '~') {
                        *(w++) = (char)c;
                } else {
                        *(w++) = '%';
                        *(w++) = hex[c >> 4];
                        *(w++) = hex[c & 0xF];
                }
        }
        *w = '\0';
        return result;
}

void curl_free(void *p) {
        free(p);
}


int unittest_http_get(void) {
        const char *base_url = rd_getenv("RD_UT_HTTP_URL", NULL);
        char *error_url;
        size_t error_url_size;
        cJSON *json, *jval;
        rd_http_error_t *herr;
        rd_bool_t empty;
        rd_kafka_t *rk;

        if (!base_url || !*base_url)
                RD_UT_SKIP("RD_UT_HTTP_URL environment variable not set");

        RD_UT_BEGIN();

        rk             = rd_calloc(1, sizeof(*rk));
        error_url_size = strlen(base_url) + strlen("/error") + 1;
        error_url      = rd_alloca(error_url_size);
        rd_snprintf(error_url, error_url_size, "%s/error", base_url);

        /* Try the base url first, parse its JSON and extract a key-value. */
        json = NULL;
        herr = rd_http_get_json(rk, base_url, NULL, 0, 5, 1, 1000, &json);
        RD_UT_ASSERT(!herr, "Expected get_json(%s) to succeed, got: %s",
                     base_url, herr->errstr);

        empty = rd_true;
        kafka_cJSON_ArrayForEach(jval, json) {
                empty = rd_false;
                break;
        }
        RD_UT_ASSERT(!empty, "Expected non-empty JSON response from %s",
                     base_url);
        RD_UT_SAY(
            "URL %s returned no error and a non-empty "
            "JSON object/array as expected",
            base_url);
        kafka_cJSON_Delete(json);


        /* Try the error URL, verify error code. */
        json = NULL;
        herr = rd_http_get_json(rk, error_url, NULL, 0, 5, 1, 1000, &json);
        RD_UT_ASSERT(herr != NULL, "Expected get_json(%s) to fail", error_url);
        RD_UT_ASSERT(herr->code >= 400,
                     "Expected get_json(%s) error code >= "
                     "400, got %d",
                     error_url, herr->code);
        RD_UT_SAY(
            "Error URL %s returned code %d, errstr \"%s\" "
            "and %s JSON object as expected",
            error_url, herr->code, herr->errstr, json ? "a" : "no");
        /* Check if there's a JSON document returned */
        if (json)
                kafka_cJSON_Delete(json);
        rd_http_error_destroy(herr);
        rd_free(rk);

        RD_UT_PASS();
}

int unittest_http_get_params_append(void) {
        rd_kafka_t *rk;
        char *res;
        RD_UT_BEGIN();
        char *tests[] = {"http://localhost:1234",
                         "",
                         "http://localhost:1234/",

                         "http://localhost:1234/",
                         "a=1",
                         "http://localhost:1234/?a=1",

                         "https://localhost:1234/",
                         "a=1&b=2",
                         "https://localhost:1234/?a=1&b=2",

                         "http://mydomain.com/?a=1",
                         "c=hi",
                         "http://mydomain.com/?a=1&c=hi",

                         "https://mydomain.com/?",
                         "c=hi",
                         "https://mydomain.com/?c=hi",

                         "http://localhost:1234/path?a=1&b=2#&c=3",
                         "c=hi",
                         "http://localhost:1234/path?a=1&b=2&c=hi",

                         "http://localhost:1234#?c=3",
                         "a=1",
                         "http://localhost:1234/?a=1",

                         "https://otherdomain.io/path?a=1&#c=3",
                         "b=2",
                         "https://otherdomain.io/path?a=1&b=2",
                         NULL};


        res = rd_http_get_params_append("", "");
        RD_UT_ASSERT(!res, "Expected NULL result, got: \"%s\"", res);
        res = rd_http_get_params_append("", "a=2&b=3");
        RD_UT_ASSERT(!res, "Expected NULL result, got: \"%s\"", res);

        char **test = tests;
        rk          = rd_calloc(1, sizeof(*rk));
        while (test[0]) {
                res = rd_http_get_params_append(test[0], test[1]);
                RD_UT_ASSERT(!strcmp(res, test[2]),
                             "Expected \"%s\", got: \"%s\"", test[2], res);
                rd_free(res);
                test += 3;
        }
        rd_free(rk);

        RD_UT_PASS();
}

/**
 * @brief Unittest. Requires a (local) webserver to be set with env var
 *        RD_UT_HTTP_URL=http://localhost:1234/some-path
 *
 * This server must return a JSON object or array containing at least one
 * object on the main URL with a 2xx response code,
 * and 4xx response on $RD_UT_HTTP_URL/error (with whatever type of body).
 */

int unittest_http(void) {
        int fails = 0;

        fails += unittest_http_get();
        fails += unittest_http_get_params_append();

        return fails;
}
