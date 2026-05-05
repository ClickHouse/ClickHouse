#include <Storages/System/StorageSystemFailPoints.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Common/FailPoint.h>


namespace DB
{

ColumnsDescription StorageSystemFailPoints::getColumnsDescription()
{
    return ColumnsDescription{
        {"name", std::make_shared<DataTypeString>(), "Name of the failpoint."},
        {"type",
         std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
             {"once", 0},
             {"regular", 1},
             {"pauseable_once", 2},
             {"pauseable", 3},
         }),
         "Type of failpoint: 'once' fires a single time then auto-disables, "
         "'regular' fires every time, "
         "'pauseable_once' blocks execution once, "
         "'pauseable' blocks execution every time until resumed."},
        {"enabled", std::make_shared<DataTypeUInt8>(),
         "Whether the failpoint is currently active (1) or not (0). "
         "For 'regular' and 'pauseable' types this is always accurate. "
         "For 'pauseable_once' this resets to 0 automatically after the failpoint fires and the blocked thread resumes. "
         "For 'once' (non-pauseable) this remains 1 until SYSTEM DISABLE FAILPOINT is called, even if the failpoint has already fired naturally."},
    };
}

void StorageSystemFailPoints::fillData(
    MutableColumns & res_columns, ContextPtr /* context */, const ActionsDAG::Node * /* predicate */, std::vector<UInt8> /* columns_mask */) const
{
    /// Get all available failpoints from the FailPointInjection registry.
    /// getFailPoints() returns a vector of {name, type, enabled} tuples
    /// covering all four categories: once, regular, pauseable_once, pauseable.
    const auto & fail_points = FailPointInjection::getFailPoints();

    for (const auto & [name, type, enabled] : fail_points)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(static_cast<Int8>(type)); /// 0=once, 1=regular, 2=pauseable_once, 3=pauseable
        res_columns[2]->insert(static_cast<UInt8>(enabled ? 1 : 0));
    }
}

}
