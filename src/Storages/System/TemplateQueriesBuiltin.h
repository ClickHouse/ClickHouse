#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

/// A ready-to-run, read-only administration query shipped with the server.
struct TemplateQuery
{
    String name;
    String category;
    String description;
    String query;
};

/// Returns the list of built-in template queries. Names are unique.
const std::vector<TemplateQuery> & getBuiltinTemplateQueries();

}
