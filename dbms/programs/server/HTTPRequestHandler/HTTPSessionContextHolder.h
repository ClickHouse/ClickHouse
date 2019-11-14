#pragma once

#include <Common/HTMLForm.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPServerRequest.h>

namespace DB
{

struct HTTPSessionContextHolder
{
    ~HTTPSessionContextHolder();

    void authentication(Poco::Net::HTTPServerRequest & request, HTMLForm & params);

    HTTPSessionContextHolder(Context & query_context_, Poco::Net::HTTPServerRequest & request, HTMLForm & params);

    String session_id;
    Context & query_context;
    std::shared_ptr<Context> session_context = nullptr;
    std::chrono::steady_clock::duration session_timeout;
};

}
