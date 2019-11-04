#pragma once

#include <Core/Types.h>
#include <Common/HTMLForm.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class Context;
class CustomExecutor;
struct HTTPInputStreams;
struct HTTPOutputStreams;

using duration = std::chrono::steady_clock::duration;
using HTTPMatchExecutorPtr = std::shared_ptr<CustomExecutor>;

class CustomExecutor
{
public:
    using HTTPServerRequest = Poco::Net::HTTPServerRequest;
    using HTTPServerResponse = Poco::Net::HTTPServerResponse;

    virtual ~CustomExecutor() = default;

    virtual bool isQueryParam(const String & param_name) const  = 0;

    virtual bool match(HTTPServerRequest & request, HTMLForm & params) const = 0;

    virtual bool canBeParseRequestBody(HTTPServerRequest & request, HTMLForm & params) = 0;

    using QueryExecutor = std::function<void(HTTPOutputStreams &, HTTPServerResponse &)>;
    using QueryExecutors = std::vector<QueryExecutor>;
    virtual QueryExecutors getQueryExecutor(Context & context, HTTPServerRequest & request, HTMLForm & params, const HTTPInputStreams & input_streams) const = 0;
};

}
