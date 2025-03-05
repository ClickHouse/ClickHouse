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


struct IYtsaurusQuery
{
    virtual String getQueryName() const = 0;
    virtual QueryParameters getQueryParameters() const = 0;
    virtual ~IYtsaurusQuery() = default;
};

struct YtsaurusReadTableQuery : public IYtsaurusQuery
{
    YtsaurusReadTableQuery(const String& path_) : path(path_) {}

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


struct YtsaurusGetQuery : public IYtsaurusQuery
{   
    YtsaurusGetQuery(const String& path_) : path(path_) {}

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

using YtsaurusQueryPtr = std::shared_ptr<IYtsaurusQuery>;

}
#endif
