#pragma once

#include <Interpreters/ClientInfo.h>
#include <Poco/Net/HTTPServerRequest.h>

namespace DB
{

class ExtractorClientInfo
{
public:
    ExtractorClientInfo(ClientInfo & info_) : client_info(info_) {}

    void extract(Poco::Net::HTTPServerRequest & request)
    {
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::HTTP;

        /// Query sent through HTTP interface is initial.
        client_info.initial_user = client_info.current_user;
        client_info.initial_query_id = client_info.current_query_id;
        client_info.initial_address = client_info.current_address;

        ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
        if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
            http_method = ClientInfo::HTTPMethod::GET;
        else if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_POST)
            http_method = ClientInfo::HTTPMethod::POST;

        client_info.http_method = http_method;
        client_info.http_user_agent = request.get("User-Agent", "");
    }

private:
    ClientInfo & client_info;
};

}
