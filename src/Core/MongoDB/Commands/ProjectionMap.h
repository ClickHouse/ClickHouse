#pragma once

#include "../Document.h"
#include "../Element.h"

namespace DB
{
namespace MongoDB
{

class ProjectionMap
{
public:
    ProjectionMap() { }
    ProjectionMap(BSON::Document::Ptr projection);

    bool include(const std::string & column_name) const;

    void add(const std::string & column_name, bool status);

    bool getDefaultStatus() const { return default_status; }

    std::vector<std::string> getNamesByStatus(bool status) const;

private:
    std::unordered_map<std::string, bool> status_map;
    bool default_status = true; // include by default
};

inline std::vector<std::string> ProjectionMap::getNamesByStatus(bool status_) const
{
    std::vector<std::string> names;
    for (const auto & [name, status] : status_map)
        if (status == status_)
            names.push_back(name);
    return names;
}

inline void ProjectionMap::add(const std::string & column_name, bool status)
{
    if (status)
        default_status = false;
    status_map[column_name] = status;
}

inline bool ProjectionMap::include(const std::string & column_name) const
{
    if (status_map.contains(column_name))
        return status_map.at(column_name);
    return default_status;
}


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

}
} // namespace DB::MongoDB
