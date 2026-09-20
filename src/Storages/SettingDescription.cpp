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
        case SettingOrigin::NamedCollection: return "named_collection";
        case SettingOrigin::Definition: return "definition";
        case SettingOrigin::SharedMetadata: return "shared_metadata";
        case SettingOrigin::Other: return "other";
    }
}

}
