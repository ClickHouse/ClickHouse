#pragma once

/** A minimal stub of the libcurl API for building pulsar-client-cpp without curl
  * (ClickHouse removed the `curl` submodule, see PR #108296).
  *
  * The Pulsar client uses curl only for:
  *   - `TopicName` percent-encoding (`curl_easy_escape`) — implemented for real below;
  *   - HTTP topic lookup (`http://`/`https://` service URLs) — fails at runtime
  *     with `CURLE_COULDNT_CONNECT`; use the binary protocol (`pulsar://`) instead;
  *   - OAuth2 authentication — fails at runtime for the same reason.
  */

#include <cstdarg>
#include <cstdlib>

using CURL = void;

using CURLcode = enum
{
    CURLE_OK = 0,
    CURLE_COULDNT_RESOLVE_PROXY = 5,
    CURLE_COULDNT_RESOLVE_HOST = 6,
    CURLE_COULDNT_CONNECT = 7,
    CURLE_HTTP_RETURNED_ERROR = 22,
    CURLE_READ_ERROR = 26,
    CURLE_OPERATION_TIMEDOUT = 28,
    CURLE_TOO_MANY_REDIRECTS = 47,
};

using CURLoption = enum
{
    CURLOPT_URL = 1,
    CURLOPT_TIMEOUT,
    CURLOPT_WRITEFUNCTION,
    CURLOPT_WRITEDATA,
    CURLOPT_USERAGENT,
    CURLOPT_CUSTOMREQUEST,
    CURLOPT_POSTFIELDS,
    CURLOPT_HTTPHEADER,
    CURLOPT_FAILONERROR,
    CURLOPT_FOLLOWLOCATION,
    CURLOPT_MAXREDIRS,
    CURLOPT_FRESH_CONNECT,
    CURLOPT_FORBID_REUSE,
    CURLOPT_NOSIGNAL,
    CURLOPT_ERRORBUFFER,
    CURLOPT_CAINFO,
    CURLOPT_SSL_VERIFYPEER,
    CURLOPT_SSL_VERIFYHOST,
    CURLOPT_SSLCERT,
    CURLOPT_SSLKEY,
    CURLOPT_SSLENGINE,
    CURLOPT_SSLENGINE_DEFAULT,
};

using CURLINFO = enum
{
    CURLINFO_RESPONSE_CODE = 1,
    CURLINFO_REDIRECT_URL,
};

#define CURL_GLOBAL_ALL 0
#define CURL_ERROR_SIZE 256

struct curl_slist
{
    char * data;
    struct curl_slist * next;
};

inline CURLcode curl_global_init(long)
{
    return CURLE_OK;
}

inline void curl_global_cleanup()
{
}

inline CURL * curl_easy_init()
{
    /// Return a non-null dummy handle so that graceful runtime error paths are taken
    /// (instead of "cannot initialize curl" failures during client construction).
    return malloc(1);
}

inline void curl_easy_cleanup(CURL * handle)
{
    free(handle);
}

inline CURLcode curl_easy_setopt(CURL *, CURLoption, ...)
{
    return CURLE_OK;
}

inline CURLcode curl_easy_perform(CURL *)
{
    return CURLE_COULDNT_CONNECT;
}

inline CURLcode curl_easy_getinfo(CURL *, CURLINFO info, ...)
{
    va_list args;
    va_start(args, info);
    switch (info)
    {
        case CURLINFO_RESPONSE_CODE:
            *va_arg(args, long *) = 0;
            break;
        case CURLINFO_REDIRECT_URL:
            *va_arg(args, char **) = nullptr;
            break;
    }
    va_end(args);
    return CURLE_OK;
}

inline const char * curl_easy_strerror(CURLcode)
{
    return "HTTP requests are not supported: pulsar-client-cpp was built without curl, use the pulsar:// binary protocol";
}

/// RFC 3986 percent-encoding. This is the only piece of curl functionality
/// that the Pulsar client needs for its normal (binary protocol) operation.
inline char * curl_easy_escape(CURL *, const char * string, int length)
{
    static const char hex_digits[] = "0123456789ABCDEF";

    if (length == 0)
    {
        int real_length = 0;
        while (string[real_length] != '\0')
            ++real_length;
        length = real_length;
    }

    char * result = static_cast<char *>(malloc(static_cast<size_t>(length) * 3 + 1));
    if (!result)
        return nullptr;

    char * out = result;
    for (int i = 0; i < length; ++i)
    {
        char c = string[i];
        bool unreserved = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
            || c == '-' || c == '.' || c == '_' || c == '~';
        if (unreserved)
        {
            *out++ = c;
        }
        else
        {
            *out++ = '%';
            *out++ = hex_digits[(static_cast<unsigned char>(c) >> 4) & 0xF];
            *out++ = hex_digits[static_cast<unsigned char>(c) & 0xF];
        }
    }
    *out = '\0';
    return result;
}

inline void curl_free(void * ptr)
{
    free(ptr);
}

inline struct curl_slist * curl_slist_append(struct curl_slist * list, const char * data)
{
    auto * new_item = static_cast<struct curl_slist *>(malloc(sizeof(struct curl_slist)));
    if (!new_item)
        return nullptr;

    size_t data_size = 0;
    while (data[data_size] != '\0')
        ++data_size;
    ++data_size;

    new_item->data = static_cast<char *>(malloc(data_size));
    if (!new_item->data)
    {
        free(new_item);
        return nullptr;
    }
    for (size_t i = 0; i < data_size; ++i)
        new_item->data[i] = data[i];
    new_item->next = nullptr;

    if (!list)
        return new_item;

    struct curl_slist * last = list;
    while (last->next)
        last = last->next;
    last->next = new_item;
    return list;
}

inline void curl_slist_free_all(struct curl_slist * list)
{
    while (list)
    {
        struct curl_slist * next = list->next;
        free(list->data);
        free(list);
        list = next;
    }
}
