#pragma once

#include <Core/Types.h>
#include <Common/config.h>
#include <Common/HTMLForm.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/HTTPInputStreams.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Poco/Net/HTTPServerRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

class QueryExecutor
{
public:
    virtual ~QueryExecutor() = default;

    virtual bool isQueryParam(const String &) const = 0;
    virtual bool canBeParseRequestBody() const = 0;

    virtual void executeQueryImpl(
        Context & context, Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const = 0;
};

using QueryExecutorPtr = std::shared_ptr<QueryExecutor>;


class QueryMatcher
{
public:
    virtual ~QueryMatcher() = default;

    virtual bool checkQueryExecutors(const std::vector<QueryExecutorPtr> &check_executors) const = 0;

    virtual bool match(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params) const = 0;
};

using QueryMatcherPtr = std::shared_ptr<QueryMatcher>;

}
