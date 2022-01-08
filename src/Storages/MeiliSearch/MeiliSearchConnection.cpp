#include "MeiliSearchConnection.h"
#include <iostream>
#include <curl/curl.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

MeiliSearchConnection::MeiliSearchConnection(const MeiliConfig & conf) : config{conf}
{
}

MeiliSearchConnection::MeiliSearchConnection(MeiliConfig && conf) : config{std::move(conf)}
{
}

static size_t writeCallback(void * contents, size_t size, size_t nmemb, void * userp)
{
    (static_cast<std::string *>(userp))->append(static_cast<char *>(contents), size * nmemb);
    return size * nmemb;
}

String MeiliSearchConnection::searchQuery(const std::unordered_map<String, String> & query_params) const
{
    CURLcode ret_code;
    CURL * hnd;
    struct curl_slist * slist1;

    slist1 = nullptr;
    slist1 = curl_slist_append(slist1, "Content-Type: application/json");
    slist1 = curl_slist_append(slist1, config.key.c_str());
    std::string response_buffer;

    std::string post_fields = "{";

    for (const auto & q_attr : query_params)
        post_fields += q_attr.first + ":" + q_attr.second + ",";

    post_fields.back() = '}';

    std::string url = config.connection_string + "search";

    hnd = curl_easy_init();
    curl_easy_setopt(hnd, CURLOPT_BUFFERSIZE, 102400L);
    curl_easy_setopt(hnd, CURLOPT_URL, url.c_str());
    curl_easy_setopt(hnd, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, post_fields.c_str());
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDSIZE_LARGE, post_fields.size());
    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, slist1);
    curl_easy_setopt(hnd, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(hnd, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(hnd, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, &response_buffer);

    ret_code = curl_easy_perform(hnd);

    curl_easy_cleanup(hnd);
    curl_slist_free_all(slist1);

    if (ret_code != 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, curl_easy_strerror(ret_code));

    return response_buffer;
}

String MeiliSearchConnection::updateQuery(std::string_view data) const
{
    CURLcode ret_code;
    CURL * hnd;
    struct curl_slist * slist1;

    slist1 = nullptr;
    slist1 = curl_slist_append(slist1, "Content-Type: application/json");
    slist1 = curl_slist_append(slist1, config.key.c_str());
    std::string response_buffer;

    std::string url = config.connection_string + "documents";

    hnd = curl_easy_init();
    curl_easy_setopt(hnd, CURLOPT_BUFFERSIZE, 102400L);
    curl_easy_setopt(hnd, CURLOPT_URL, url.c_str());
    curl_easy_setopt(hnd, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, data.data());
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDSIZE_LARGE, data.size());
    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, slist1);
    curl_easy_setopt(hnd, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(hnd, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(hnd, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, &response_buffer);

    ret_code = curl_easy_perform(hnd);

    curl_easy_cleanup(hnd);
    curl_slist_free_all(slist1);

    if (ret_code != 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, curl_easy_strerror(ret_code));

    return response_buffer;
}

}
