#pragma once

#include "Columns/ColumnLazy.h"
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>

#include <memory>


namespace DB
{

class IColumnLazyHelper
{
public:
    virtual void transformLazyColumns(
        const ColumnLazy & column_lazy,
        ColumnsWithTypeAndName & res_columns) = 0;

    virtual ~IColumnLazyHelper() = default;
};

using ColumnLazyHelperPtr = std::shared_ptr<IColumnLazyHelper>;

}
