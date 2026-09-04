#include <Processors/QueryPlan/StepManifest.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_QUERY_PLAN;
}

namespace WireDetail
{

void throwCannotParse(const char * what)
{
    throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN, "Query plan step payload is malformed: {}", what);
}

}

}
