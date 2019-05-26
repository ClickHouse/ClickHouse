#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const Context & context);

}
