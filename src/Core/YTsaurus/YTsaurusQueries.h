#pragma once

#if USE_YTSAURUS

#include <Core/Types.h>
#include <fmt/format.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{


struct QueryParameter
{
    String name;
    String value;
};
using QueryParameters = std::vector<QueryParameter>;


struct IYTsaurusQuery
{
    virtual String getQueryName() const = 0;
    virtual QueryParameters getQueryParameters() const = 0;
    virtual ~IYTsaurusQuery() = default;
    /// Follows: https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#http_method
    virtual String getHTTPMethod() const = 0;
};

struct YTsaurusReadTableQuery : public IYTsaurusQuery
{
    explicit YTsaurusReadTableQuery(const String & cypress_path_) : cypress_path(cypress_path_) {}

    String getQueryName() const override
    {
        return "read_table";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_GET;
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=cypress_path}};
    }
    String cypress_path;
};


struct YTsaurusGetQuery : public IYTsaurusQuery
{
    explicit YTsaurusGetQuery(const String & cypress_path_) : cypress_path(cypress_path_) {}

    String getQueryName() const override
    {
        return "get";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_GET;
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=cypress_path}};
    }
    String cypress_path;
};


struct YTsaurusSelectRowsQuery : public IYTsaurusQuery
{
    explicit YTsaurusSelectRowsQuery(const String & table_path_) : table_path(table_path_) {}

    String getQueryName() const override
    {
        return "select_rows";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_GET;
    }

    String constructQuery() const
    {
        return fmt::format("* from [{}]", table_path);
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="query", .value=constructQuery()}};
    }
    String table_path;
};

struct YTsaurusLookupRows : public IYTsaurusQuery
{
    explicit YTsaurusLookupRows(const String & cypress_path_) : cypress_path(cypress_path_) {}

    String getQueryName() const override
    {
        return "lookup_rows";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_PUT;
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=cypress_path}};
    }
    String cypress_path;
};

using YTsaurusQueryPtr = std::shared_ptr<IYTsaurusQuery>;

}
#endif
