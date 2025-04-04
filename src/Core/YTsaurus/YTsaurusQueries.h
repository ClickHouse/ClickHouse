#pragma once

#if USE_YTSAURUS

#include <Core/Types.h>
#include <fmt/format.h>


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
};

struct YTsaurusReadTableQuery : public IYTsaurusQuery
{
    explicit YTsaurusReadTableQuery(const String& cypress_path_) : cypress_path(cypress_path_) {}

    String getQueryName() const override
    {
        return "read_table";
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=cypress_path}};
    }
    String cypress_path;
};


struct YTsaurusGetQuery : public IYTsaurusQuery
{
    explicit YTsaurusGetQuery(const String& cypress_path_) : cypress_path(cypress_path_) {}

    String getQueryName() const override
    {
        return "get";
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=cypress_path}};
    }
    String cypress_path;
};


struct YTsaurusSelectRowsQuery : public IYTsaurusQuery
{
    explicit YTsaurusSelectRowsQuery(const String& table_path_) : table_path(table_path_) {}

    String getQueryName() const override
    {
        return "select_rows";
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

using YTsaurusQueryPtr = std::shared_ptr<IYTsaurusQuery>;

}
#endif
