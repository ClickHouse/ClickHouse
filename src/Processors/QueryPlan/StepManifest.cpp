#include <Processors/QueryPlan/StepManifest.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_QUERY_PLAN;
    extern const int LOGICAL_ERROR;
}

namespace WireDetail
{

void throwCannotParse(const char * what)
{
    throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN, "Query plan step payload is malformed: {}", what);
}

}

StepManifestCatalog & StepManifestCatalog::instance()
{
    static StepManifestCatalog catalog;
    return catalog;
}

void StepManifestCatalog::add(const String & name, String description)
{
    std::lock_guard lock(mutex);
    auto [it, inserted] = descriptions.try_emplace(name, std::move(description));
    if (!inserted && it->second != description)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two different manifests are registered under the step name '{}'", name);
}

String StepManifestCatalog::dump() const
{
    std::lock_guard lock(mutex);
    String result;
    for (const auto & [name, description] : descriptions)
        result += description;
    return result;
}

}
