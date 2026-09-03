#include <Storages/TableSetting.h>

namespace DB
{

std::string_view toString(TableSettingOrigin origin)
{
    switch (origin)
    {
        case TableSettingOrigin::Default: return "default";
        case TableSettingOrigin::Config: return "config";
        case TableSettingOrigin::Compatibility: return "compatibility";
        case TableSettingOrigin::Definition: return "definition";
        case TableSettingOrigin::NamedCollection: return "named_collection";
        case TableSettingOrigin::SharedMetadata: return "shared_metadata";
        case TableSettingOrigin::Runtime: return "runtime";
        case TableSettingOrigin::Other: return "other";
    }
}

}
