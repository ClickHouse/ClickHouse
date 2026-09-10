#include <Processors/QueryPlan/StepManifest.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace WireDetail
{

void throwCannotParse(const char * what)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Query plan step payload is malformed: {}", what);
}

}

}
