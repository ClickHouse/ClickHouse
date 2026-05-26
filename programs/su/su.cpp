#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <IO/ReadHelpers.h>
#include <fmt/format.h>

#include <iostream>
#include <vector>

#include <sys/types.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>


/// "su" means "set user"
/// In fact, this program can set Unix user and group.
///
/// Usage:
/// clickhouse su user[:group] args...
///
/// - will set user and, optionally, group and exec the remaining args.
///   user and group can be numeric identifiers or strings.
///
/// The motivation for this tool is very obscure and idiosyncratic. It is needed for Docker.
/// People want to run programs inside Docker with dropped privileges (less than root).
/// But the standard Linux "su" program is not suitable for usage inside Docker,
/// because it is creating pseudoterminals to avoid hijacking input from the terminal, for security,
/// but Docker is also doing something with the terminal and it is incompatible.
/// For this reason, people use alternative and less "secure" versions of "su" tools like "gosu" or "su-exec".
/// But it would be very strange to use 3rd-party software only to do two-three syscalls.
/// That's why we provide this tool.
///
/// Note: ClickHouse does not need Docker at all and works better without Docker.
/// ClickHouse has no dependencies, it is packaged and distributed in single binary.
/// There is no reason to use Docker unless you are already running all your software in Docker.

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYSTEM_ERROR;
}

static void setUserAndGroup(std::string arg_uid, std::string arg_gid)
{
    static constexpr size_t buf_size = 16384; /// Linux man page says it is enough. Nevertheless, we will check if it's not enough and throw.
    std::unique_ptr<char[]> buf(new char[buf_size]);

    /// Resolve the target group GID first, while we still have privileges.
    /// The actual setgid() call is deferred until after initgroups() so we
    /// can reset the supplementary group list before dropping privileges.

    bool has_gid = false;
    gid_t gid = 0;
    if (!arg_gid.empty())
    {
        bool parsed_numeric = tryParse(gid, arg_gid);
        if (!parsed_numeric || gid == 0)
        {
            group entry{};
            group * result{};

            if (0 != getgrnam_r(arg_gid.data(), &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getgrnam_r' to obtain gid from group name ({})", arg_gid);

            if (!result)
            {
                /// Only retry as a numeric gid when the input actually parsed as a
                /// number. Otherwise `gid` is still 0 and `getgrgid_r(0)` would
                /// silently resolve to the root group, masking a typo in the
                /// requested group name.
                if (parsed_numeric && 0 != getgrgid_r(gid, &entry, buf.get(), buf_size, &result))
                    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getgrgid_r' to obtain gid ({})", gid);

                if (!result)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group {} is not found in the system", arg_gid);
            }

            gid = entry.gr_gid;
        }

        if (gid == 0 && getgid() != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group has id 0, but dropping privileges to gid 0 does not make sense");

        has_gid = true;
    }

    /// Resolve the target user UID and, where possible, the user name and primary
    /// GID. We always consult the passwd database (even when arg_uid is numeric) so
    /// that initgroups() can look up the user's supplementary group memberships.

    bool has_uid = false;
    uid_t uid = 0;
    std::string user_name;
    gid_t user_primary_gid = 0;
    if (!arg_uid.empty())
    {
        passwd entry{};
        passwd * result{};

        bool parsed_numeric = tryParse(uid, arg_uid);
        if (!parsed_numeric || uid == 0)
        {
            if (0 != getpwnam_r(arg_uid.data(), &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getpwnam_r' to obtain uid from user name ({})", arg_uid);

            if (!result)
            {
                /// Only retry as a numeric uid when the input actually parsed as a
                /// number. Otherwise `uid` is still 0 and `getpwuid_r(0)` would
                /// silently resolve to root, defeating the requested privilege drop.
                if (parsed_numeric && 0 != getpwuid_r(uid, &entry, buf.get(), buf_size, &result))
                    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getpwuid_r' to obtain user name from uid ({})", uid);

                if (!result)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "User {} is not found in the system", arg_uid);
            }

            uid = entry.pw_uid;
            user_name = entry.pw_name;
            user_primary_gid = entry.pw_gid;
        }
        else
        {
            /// Numeric, non-zero UID. Look up the passwd entry to obtain the user
            /// name needed by initgroups(). A nonzero return is a real NSS error
            /// (ERANGE / ENOMEM / backend failure) and must be surfaced. The
            /// "no entry" case (rc == 0, result == nullptr) is allowed: the
            /// supplementary list will be cleared rather than populated from
            /// /etc/group.
            if (0 != getpwuid_r(uid, &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getpwuid_r' to obtain user name from uid ({})", uid);

            if (result)
            {
                user_name = entry.pw_name;
                user_primary_gid = entry.pw_gid;
            }
        }

        if (uid == 0 && getuid() != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "User has id 0, but dropping privileges to uid 0 does not make sense");

        has_uid = true;
    }

    /// Reset the supplementary group list before dropping privileges. Otherwise
    /// the dropped process silently inherits the caller's supplementary groups
    /// (typically root's), defeating the intent of the privilege drop.
    /// initgroups()/setgroups() require CAP_SETGID, which a non-root caller does
    /// not have; skip the reset in that case so same-identity no-op invocations
    /// (e.g. `clickhouse su user:group` from inside a Docker `--user` container)
    /// keep working.
    if (has_uid && geteuid() == 0)
    {
        gid_t group_for_initgroups = has_gid ? gid : user_primary_gid;

        if (!user_name.empty())
        {
            if (0 != initgroups(user_name.c_str(), group_for_initgroups))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'initgroups' for user ({})", user_name);
        }
        else
        {
            /// No passwd entry for this UID; clear supplementary groups entirely.
            if (0 != setgroups(0, nullptr))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'setgroups' to clear supplementary groups");
        }
    }

    /// Now drop privileges.
    if (has_gid)
    {
        if (0 != setgid(gid))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'setgid' to user ({})", arg_gid);
    }

    if (has_uid)
    {
        if (0 != setuid(uid))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'setuid' to user ({})", arg_uid);
    }
}

}


int mainEntryClickHouseSU(int argc, char ** argv)
try
{
    using namespace DB;

    if (argc < 3)
    {
        std::cout << "A tool similar to 'su'" << std::endl;
        std::cout << "Usage: clickhouse su user:group ..." << std::endl;
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    std::string_view user_and_group = argv[1];

    std::string user;
    std::string group;

    auto pos = user_and_group.find(':');
    if (pos == std::string_view::npos)
    {
        user = user_and_group;
    }
    else
    {
        user = user_and_group.substr(0, pos);
        group = user_and_group.substr(pos + 1);
    }

    setUserAndGroup(std::move(user), std::move(group));

    std::vector<char *> new_argv;
    new_argv.reserve(argc - 1);
    new_argv.insert(new_argv.begin(), argv + 2, argv + argc);
    new_argv.push_back(nullptr);

    execvp(new_argv.front(), new_argv.data());

    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot execvp");
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(false) << '\n';
    return 1;
}
