#pragma once

#include "../Element.h"
#include "../Document.h"

namespace DB {
namespace MongoDB {

class ProjectionMap {
    public:

    ProjectionMap(BSON::Document::Ptr projection);

    bool include(const std::string& column_name);

    private:
        std::unordered_map<std::string, bool> status_map;
        bool default_status = true; // include by default
};


inline bool ProjectionMap::include(const std::string& column_name) {
    if (status_map.contains(column_name)) {
        return status_map[column_name];
    }
    return default_status;
}


ProjectionMap::ProjectionMap(BSON::Document::Ptr projection) {
    if (!projection) {
        default_status = true;
        return;
    }
    const auto& element_names = projection->elementNames();
    for (const auto& name: element_names) {
        BSON::Element::Ptr elem = projection->get(name);
        if (elem->getType() == BSON::ElementTraits<BSON::Document::Ptr>::TypeId) {
            LOG_WARNING(getLogger("MongoDB::ProjectionMap"), "Nested queries are not supported");
            continue;
        }
        auto tmp = elem.cast<BSON::ConcreteElement<Int32>>();
        Int32 value = tmp->getValue();
        if (value > 0) { // include
            default_status = false;
        }
        status_map[name] = value > 0;
    }
}

}} // namespace DB::MongoDB
