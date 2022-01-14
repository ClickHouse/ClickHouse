#include "MeiliSearchConnection.h"
#include <string_view>
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

static size_t writeCallback(void * contents, size_t size, size_t nmemb, void * userp)
{
    (static_cast<std::string *>(userp))->append(static_cast<char *>(contents), size * nmemb);
    return size * nmemb;
}

CURLcode MeiliSearchConnection::execQuery(
    std::string_view url, 
    std::string_view post_fields,
    std::string& response_buffer) const 
{
    CURLcode ret_code;
    CURL * handle;
    struct curl_slist * headers_list;

    headers_list = nullptr;
    headers_list = curl_slist_append(headers_list, "Content-Type: application/json");
    headers_list = curl_slist_append(headers_list, config.key.c_str());

    handle = curl_easy_init();
    curl_easy_setopt(handle, CURLOPT_URL, url.data());
    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, post_fields.data());
    curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE_LARGE, post_fields.size());
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers_list);
    curl_easy_setopt(handle, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &response_buffer);

    ret_code = curl_easy_perform(handle);

    curl_easy_cleanup(handle);
    curl_slist_free_all(headers_list);
    return ret_code;
}

String MeiliSearchConnection::searchQuery(const std::unordered_map<String, String> & query_params) const
{
    std::string response_buffer;

    std::string post_fields = "{";
    for (const auto & q_attr : query_params)
        post_fields += q_attr.first + ":" + q_attr.second + ",";

    post_fields.back() = '}';

    std::string url = config.connection_string + "search";

    CURLcode ret_code = execQuery(url, post_fields, response_buffer);

    if (ret_code != CURLE_OK)
        throw Exception(ErrorCodes::NETWORK_ERROR, curl_easy_strerror(ret_code));

    return response_buffer;
}

String MeiliSearchConnection::updateQuery(std::string_view data) const
{
    std::string response_buffer;

    std::string url = config.connection_string + "documents";

    CURLcode ret_code = execQuery(url, data, response_buffer);

    if (ret_code != CURLE_OK)
        throw Exception(ErrorCodes::NETWORK_ERROR, curl_easy_strerror(ret_code));

    return response_buffer;
}

}
