#include <Storages/SettingDescription.h>

namespace DB
{

std::string_view toString(SettingOrigin origin)
{
    switch (origin)
    {
        case SettingOrigin::Default: return "default";
        case SettingOrigin::Config: return "config";
        case SettingOrigin::Compatibility: return "compatibility";
        case SettingOrigin::Definition: return "definition";
        case SettingOrigin::NamedCollection: return "named_collection";
        case SettingOrigin::SharedMetadata: return "shared_metadata";
        case SettingOrigin::Runtime: return "runtime";
        case SettingOrigin::Other: return "other";
    }
}

}
