#include <Common/assertProcessUserMatchesDataOwner.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_GETPWUID;
    extern const int MISMATCHING_USERS_FOR_PROCESS_AND_DATA;
}

namespace
{
    std::string getUserName(uid_t user_id)
    {
        /// Try to convert user id into user name.
        auto buffer_size = sysconf(_SC_GETPW_R_SIZE_MAX);
        if (buffer_size <= 0)
            buffer_size = 1024;
        std::string buffer;
        buffer.reserve(buffer_size);

        struct passwd passwd_entry;
        struct passwd * result = nullptr;
        const auto error = getpwuid_r(user_id, &passwd_entry, buffer.data(), buffer_size, &result);

        if (error)
            ErrnoException::throwWithErrno(
                ErrorCodes::FAILED_TO_GETPWUID, error, "Failed to find user name for {}", std::to_string(user_id));
        else if (result)
            return result->pw_name;
        return std::to_string(user_id);
    }
}

void assertProcessUserMatchesDataOwner(const std::string & path, std::function<void(const std::string &)> on_warning)
{
    /// Check that the process user id matches the owner of the data.
    const auto effective_user_id = geteuid();
    struct stat statbuf;
    if (stat(path.c_str(), &statbuf) == 0 && effective_user_id != statbuf.st_uid)
    {
        const auto effective_user = getUserName(effective_user_id);
        const auto data_owner = getUserName(statbuf.st_uid);
        std::string message = fmt::format(
            "Effective user of the process ({}) does not match the owner of the data ({}).",
            effective_user, data_owner);

        if (effective_user_id == 0)
        {
            message += fmt::format(" Run under 'sudo -u {}'.", data_owner);
            throw Exception(ErrorCodes::MISMATCHING_USERS_FOR_PROCESS_AND_DATA, "{}", message);
        }

        on_warning(message);
    }
}

}
