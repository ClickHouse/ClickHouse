#pragma once

#include <IO/ReadWriteBufferFromHTTP.h>
#include <functional>
#include <Poco/JSON/Parser.h>

namespace DataLake
{

DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
    const std::string & endpoint,
    DB::ContextPtr context,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const Poco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
    std::function<void(std::ostream &)> out_stream_callaback = {});

std::pair<Poco::Dynamic::Var, std::string> makeHTTPRequestAndReadJSON(
    const std::string & endpoint,
    DB::ContextPtr context,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const Poco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
    std::function<void(std::ostream &)> out_stream_callaback = {});

}
