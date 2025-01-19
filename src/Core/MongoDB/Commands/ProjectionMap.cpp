#include "ProjectionMap.h"


namespace DB
{
namespace MongoDB
{

ProjectionMap::ProjectionMap(BSON::Document::Ptr projection)
{
    if (!projection)
    {
        default_status = true;
        return;
    }
    const auto & element_names = projection->elementNames();
    for (const auto & name : element_names)
    {
        BSON::Element::Ptr elem = projection->get(name);
        if (elem->getType() == BSON::ElementTraits<BSON::Document::Ptr>::TypeId)
        {
            LOG_WARNING(getLogger("MongoDB::ProjectionMap"), "Nested queries are not supported");
            continue;
        }
        auto tmp = elem.cast<BSON::ConcreteElement<Int32>>();
        Int32 value = tmp->getValue();
        add(name, value > 0);
    }
}


bool ProjectionMap::isIncluded(const std::string & column_name) const
{
    auto it = std::find_if(columns.begin(), columns.end(), [&column_name](const auto & column) { return column.name == column_name; });
    if (it == columns.end())
        throw Poco::RuntimeException(fmt::format("ProjectionMap: couldn't find column_name {}", column_name));
    return isIncluded(it - columns.begin());
}

std::vector<std::string> ProjectionMap::getNamesByStatus(bool status_) &&
{
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t i = 0; i < columns.size(); i++)
        if (isIncluded(i) == status_)
            result.emplace_back(columns[i].order, i);
    std::sort(result.begin(), result.end());
    std::vector<std::string> str_result;
    str_result.reserve(result.size());
    for (const auto & [_, i] : result)
        str_result.emplace_back(std::move(columns[i].name));
    return str_result;
}
void ProjectionMap::resetStatuses()
{
    for (auto & column : columns)
        column.status = Unspecified;
}

void ProjectionMap::add(const std::string & column_name, bool status)
{
    // TODO corner case '_id'
    if (status)
        default_status = false;
    auto it = std::find_if(columns.begin(), columns.end(), [&column_name](const auto & column) { return column.name == column_name; });
    (*it).status = status ? IncludeStatus::Include : IncludeStatus::NotInclude;
}


}
}
