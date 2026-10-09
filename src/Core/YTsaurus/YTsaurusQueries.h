#pragma once

#if USE_YTSAURUS

#include <Core/Types.h>
#include <fmt/format.h>
#include <Poco/Net/HTTPRequest.h>
#include <Core/ColumnsWithTypeAndName.h>

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
    virtual bool isHeavyQuery() { return false; }
};

// Follow up: https://ytsaurus.tech/docs/en/api/commands
struct IYTsaurusHeavyQuery : public IYTsaurusQuery
{
    bool isHeavyQuery() override { return true; }
};

// https://ytsaurus.tech/docs/en/api/commands#read_table
struct YTsaurusReadTableQuery : public IYTsaurusHeavyQuery
{
    explicit YTsaurusReadTableQuery(const String & cypress_path_, const std::pair<size_t, size_t>& row_range)
        : cypress_path(fmt::format("{}[#{}:#{}]", cypress_path_, row_range.first, row_range.second))
        {}

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

// https://ytsaurus.tech/docs/en/api/commands#get
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

// https://ytsaurus.tech/docs/en/api/commands#select_rows
struct YTsaurusSelectRowsQuery : public IYTsaurusHeavyQuery
{
    explicit YTsaurusSelectRowsQuery(const String & table_path_, const String & columns_str_)
        : table_path(table_path_) , column_names_str(columns_str_) {}

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
        return fmt::format("{} from [{}]", column_names_str, table_path);
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="query", .value=constructQuery()}};
    }
    String table_path;
    String column_names_str;
};

// https://ytsaurus.tech/docs/en/api/commands#lookup_rows
struct YTsaurusLookupRows : public IYTsaurusHeavyQuery
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


// https://ytsaurus.tech/docs/en/api/commands#start_tx
struct YTsaurusStartTxQuery : public IYTsaurusQuery
{
    explicit YTsaurusStartTxQuery(size_t timeout_) : timeout(timeout_) {}

    String getQueryName() const override
    {
        return "start_tx";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_POST;
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="timeout", .value=std::to_string(timeout) }};
    }

    size_t timeout;
};

// https://ytsaurus.tech/docs/en/api/commands#commit_tx
struct YTsaurusCommitTxQuery : public IYTsaurusQuery
{
    explicit YTsaurusCommitTxQuery(const String& transaction_id_)
        : transaction_id(transaction_id_)
    {
    }

    String getQueryName() const override
    {
        return "commit_tx";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_POST;
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="transaction_id", .value=transaction_id}};
    }

    String transaction_id;
};

// https://ytsaurus.tech/docs/en/api/commands#lock
struct YTsaurusLockQuery : public IYTsaurusQuery
{
    explicit YTsaurusLockQuery(const String& path_, const String& transaction_id_)
        : path(path_)
        , transaction_id(transaction_id_) {}

    String getQueryName() const override
    {
        return "lock";
    }

    String getHTTPMethod() const override
    {
        return Poco::Net::HTTPRequest::HTTP_POST;
    }

    QueryParameters getQueryParameters() const override
    {
        return {
            {.name="path", .value=path},
            {.name="transaction_id", .value=transaction_id},
            {.name="mode", .value="snapshot"},
        };
    }

    String path;
    String transaction_id;
};


using YTsaurusQueryPtr = std::shared_ptr<IYTsaurusQuery>;

}
#endif
