#include <Processors/Transforms/MergeJoinTransform.h>
#include <base/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergeJoinTransform::MergeJoinTransform(
        const Blocks & input_headers,
        const Block & output_header,
        UInt64 limit_hint)
    : IMergingTransform<MergeJoinAlgorithm>(input_headers, output_header, true, limit_hint)
    , log(&Poco::Logger::get("MergeJoinTransform"))
{
    LOG_TRACE(log, "Will use MergeJoinTransform");
}

void MergeJoinTransform::onFinish()
{
    LOG_TRACE(log, "MergeJoinTransform::onFinish");
}


}
