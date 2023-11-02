#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct SortProp
{
    SortDescription sort_description = {};
    DataStream::SortScope sort_scope = DataStream::SortScope::None;

    String toString() const
    {
        String res;
        for (const auto & sort_column : sort_description)
        {
            res += sort_column.column_name + ", ";
        }

        switch (sort_scope)
        {
            case DataStream::SortScope::None:
                res += "None";
                break;
            case DataStream::SortScope::Stream:
                res += "Stream";
                break;
            case DataStream::SortScope::Global:
                res += "Global";
                break;
            case DataStream::SortScope::Chunk:
                res += "Chunk";
                break;
        }

        return res;
    }
};

}
