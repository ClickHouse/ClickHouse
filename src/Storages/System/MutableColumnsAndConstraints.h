#pragma once

#include <Access/SettingsConstraints.h>
#include <Columns/IColumn_fwd.h>

namespace DB
{

struct MutableColumnsAndConstraints
{
    MutableColumns & res_columns;
    const SettingsConstraints & constraints;

    MutableColumnsAndConstraints(MutableColumns & res_columns_, const SettingsConstraints & constraints_)
        : res_columns(res_columns_), constraints(constraints_)
    {
    }
};
}
