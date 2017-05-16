#pragma once

#include <Core/Types.h>


namespace DB
{

/// Up to what stage the SELECT query is executed or needs to be executed.
namespace QueryProcessingStage
{
    /// Numbers matter - the later stage has a larger number.
    enum Enum
    {
        FetchColumns        = 0,    /// Only read/have been read the columns specified in the query.
        WithMergeableState  = 1,    /// Until the stage where the results of processing on different servers can be combined.
        Complete            = 2,    /// Completely.
    };

    inline const char * toString(UInt64 stage)
    {
        static const char * data[] = { "FetchColumns", "WithMergeableState", "Complete" };
        return stage < 3
            ? data[stage]
            : "Unknown stage";
    }
}

}
