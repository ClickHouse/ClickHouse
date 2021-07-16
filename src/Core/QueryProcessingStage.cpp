#include <Core/QueryProcessingStage.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace QueryProcessingStage
{

    Enum fromString(const std::string & stage_string)
    {
        Enum stage;

        if (stage_string == "complete")
            stage = Complete;
        else if (stage_string == "fetch_columns")
            stage = FetchColumns;
        else if (stage_string == "with_mergeable_state")
            stage = WithMergeableState;
        else if (stage_string == "with_mergeable_state_after_aggregation")
            stage = WithMergeableStateAfterAggregation;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown query processing stage: {}", stage_string);

        return stage;
    }

}

}
