#pragma once

#include <Columns/IColumn.h>

namespace DB
{

struct ServerSettingColumnsParams
{
    MutableColumns & res_columns;
    ContextPtr context;

    ServerSettingColumnsParams(MutableColumns & res_columns_, ContextPtr context_)
        : res_columns(res_columns_), context(context_)
    {
    }
};
}
