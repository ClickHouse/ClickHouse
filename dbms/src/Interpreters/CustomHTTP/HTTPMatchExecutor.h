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
class HTTPMatchExecutor;
struct HTTPStreamsWithInput;
struct HTTPStreamsWithOutput;

using duration = std::chrono::steady_clock::duration;
using HTTPMatchExecutorPtr = std::shared_ptr<HTTPMatchExecutor>;

class HTTPMatchExecutor
{
public:
    using HTTPServerRequest = Poco::Net::HTTPServerRequest;
    using HTTPServerResponse = Poco::Net::HTTPServerResponse;

    bool match(HTTPServerRequest & request, HTMLForm & params) const { return matchImpl(request, params); };

    void execute(Context & context, HTTPServerRequest & request, HTTPServerResponse & response, HTMLForm & params, HTTPStreamsWithOutput & used_output) const;

    virtual ~HTTPMatchExecutor() = default;
protected:

    virtual bool matchImpl(HTTPServerRequest & request, HTMLForm & params) const = 0;

    virtual String getExecuteQuery(HTMLForm & params) const = 0;

    virtual bool needParsePostBody(HTTPServerRequest & request, HTMLForm & params) const = 0;

    virtual bool acceptQueryParam(Context & context, const String & key, const String & value) const = 0;

    void initClientInfo(Context & context, HTTPServerRequest & request) const;

    void authentication(Context & context, HTTPServerRequest & request, HTMLForm & params) const;

    void detachSessionContext(std::shared_ptr<Context> & context, const String & session_id, const duration & session_timeout) const;

    std::shared_ptr<Context> attachSessionContext(Context & context, HTMLForm & params, const String & session_id, const duration & session_timeout) const;

    void collectParamsAndApplySettings(HTTPServerRequest & request, HTMLForm & params, Context & context) const;

};

}
