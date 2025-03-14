#pragma once

#if USE_YTSAURUS

#include <Core/Types.h>

namespace ytsaurus
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
    explicit YTsaurusReadTableQuery(const String& path_) : path(path_) {}

    String getQueryName() const override
    {
        return "read_table";
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=path}};
    }
    String path;
};


struct YTsaurusGetQuery : public IYTsaurusQuery
{
    explicit YTsaurusGetQuery(const String& path_) : path(path_) {}

    String getQueryName() const override
    {
        return "get";
    }

    QueryParameters getQueryParameters() const override
    {
        return {{.name="path", .value=path}};
    }
    String path;
};

using YTsaurusQueryPtr = std::shared_ptr<IYTsaurusQuery>;

}
#endif
