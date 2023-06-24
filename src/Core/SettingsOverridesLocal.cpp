#include <Core/SettingsOverridesLocal.h>
#include <Core/Settings.h>

namespace DB
{

void applySettingsOverridesForLocal(Settings & settings)
{
    settings.allow_introspection_functions = true;
    settings.storage_file_read_method = LocalFSReadMethod::mmap;
}

}
