#pragma once

#include "../Document.h"
#include "../Element.h"

namespace DB
{
namespace MongoDB
{

namespace
{
enum IncludeStatus
{
    Include,
    NotInclude,
    Unspecified
};
}


class ProjectionMap
{
    struct Column
    {
        Column(std::string && name_, size_t order_, IncludeStatus status_) : name(std::move(name_)), order(order_), status(status_) { }

        std::string name;
        size_t order;
        IncludeStatus status;

        bool operator<(const Column & other) const { return name < other.name; }
    };

public:
    ProjectionMap(std::vector<std::string> && columns_)
    {
        columns.reserve(columns_.size());
        for (size_t i = 0; i < columns_.size(); i++)
            columns.emplace_back(std::move(columns_[i]), i, IncludeStatus::Unspecified);
        std::sort(columns.begin(), columns.end());
    }
    ProjectionMap(BSON::Document::Ptr projection);
    void add(const std::string & column_name, bool status);

    bool getDefaultStatus() const { return default_status; }
    void setDefaultStatus(bool status) { default_status = status; }
    void resetStatuses();

    std::vector<std::string> getNamesByStatus(bool status) &&;

private:
    std::vector<Column> columns;
    bool default_status = true; // include by default

    bool isIncluded(const std::string & column_name) const;
    bool isIncluded(size_t ind) const;
};

inline bool ProjectionMap::isIncluded(size_t ind) const
{
    auto status = columns[ind].status;
    return status == IncludeStatus::Unspecified ? default_status : status == IncludeStatus::Include;
}

}
} // namespace DB::MongoDB
