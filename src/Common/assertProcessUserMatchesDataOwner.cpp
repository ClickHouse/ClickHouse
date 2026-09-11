/// Compares the process's effective user against the owner of the data directory, to warn about a
/// server started as the wrong user.
#if defined(OS_WINDOWS)

#include <Common/assertProcessUserMatchesDataOwner.h>

namespace DB
{

void assertProcessUserMatchesDataOwner(const std::string &, std::function<void(const PreformattedMessage &)>)
{
    /// Windows has no uid: `_stat` reports `st_uid` as 0 for everything, and ownership there is a
    /// SID in an ACL rather than a number to compare against `geteuid`. Making the same check would
    /// mean `GetSecurityInfo` on the directory and `GetTokenInformation` on the process, which is a
    /// different piece of work; until then there is nothing to warn about.
}

}

#else

#include <Common/assertProcessUserMatchesDataOwner.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
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

        struct passwd passwd_entry{};
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

void assertProcessUserMatchesDataOwner(const std::string & path, std::function<void(const PreformattedMessage &)> on_warning)
{
    /// Check that the process user id matches the owner of the data.
    const auto effective_user_id = geteuid();
    struct stat statbuf{};
    if (stat(path.c_str(), &statbuf) == 0 && effective_user_id != statbuf.st_uid)
    {
        auto effective_user = getUserName(effective_user_id);
        auto data_owner = getUserName(statbuf.st_uid);
        constexpr auto message_format_string = "Effective user of the process ({}) does not match the owner of the data ({}).";
        auto formatted_msg = PreformattedMessage::create(message_format_string, effective_user, data_owner);
        if (effective_user_id == 0)
        {
            auto message = formatted_msg.text + fmt::format(" Run under 'sudo -u {}'.", data_owner);
            throw Exception(ErrorCodes::MISMATCHING_USERS_FOR_PROCESS_AND_DATA, "{}", message);
        }

        on_warning(formatted_msg);
    }
}

}

#endif
