#include <Backups/BackupPathUtils.h>

namespace DB
{

String joinBackupPath(std::string_view parent, std::string_view child)
{
    if (parent.empty())
        return String(child);
    if (child.empty())
        return String(parent);
    if (parent.ends_with('/'))
    {
        if (child.starts_with('/'))
            child.remove_prefix(1);
        return String(parent) + String(child);
    }
    if (child.starts_with('/'))
        return String(parent) + String(child);
    return String(parent) + "/" + String(child);
}

String backupPathBaseName(std::string_view path)
{
    const auto pos = path.rfind('/');
    if (pos == std::string_view::npos)
        return String(path);
    return String(path.substr(pos + 1));
}

String backupPathParent(std::string_view path)
{
    const auto pos = path.rfind('/');
    if (pos == std::string_view::npos)
        return {};
    return String(path.substr(0, pos));
}

}
