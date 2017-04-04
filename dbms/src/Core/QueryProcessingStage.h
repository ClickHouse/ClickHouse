#pragma once

#include <Core/Types.h>


namespace DB
{

/// До какой стадии выполнен или нужно выполнить SELECT запрос.
namespace QueryProcessingStage
{
    /// Номера имеют значение - более поздняя стадия имеет больший номер.
    enum Enum
    {
        FetchColumns        = 0,    /// Только прочитать/прочитаны указанные в запросе столбцы.
        WithMergeableState     = 1,    /// До стадии, когда результаты обработки на разных серверах можно объединить.
        Complete             = 2,    /// Полностью.
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
