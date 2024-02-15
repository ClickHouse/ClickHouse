#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <fmt/format.h>
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

void setUserAndGroup(std::string arg_uid, std::string arg_gid)
{
    static constexpr size_t buf_size = 16384; /// Linux man page says it is enough. Nevertheless, we will check if it's not enough and throw.
    std::unique_ptr<char[]> buf(new char[buf_size]);

    /// Set the group first, because if we set user, the privileges will be already dropped and we will not be able to set the group later.

    if (!arg_gid.empty())
    {
        gid_t gid = 0;
        if (!tryParse(gid, arg_gid) || gid == 0)
        {
            group entry{};
            group * result{};

            if (0 != getgrnam_r(arg_gid.data(), &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getgrnam_r' to obtain gid from group name ({})", arg_gid);

            if (!result)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group {} is not found in the system", arg_gid);

            gid = entry.gr_gid;
        }

        if (gid == 0 && getgid() != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group has id 0, but dropping privileges to gid 0 does not make sense");

        if (0 != setgid(gid))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'setgid' to user ({})", arg_gid);
    }

    if (!arg_uid.empty())
    {
        /// Is it numeric id or name?
        uid_t uid = 0;
        if (!tryParse(uid, arg_uid) || uid == 0)
        {
            passwd entry{};
            passwd * result{};

            if (0 != getpwnam_r(arg_uid.data(), &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getpwnam_r' to obtain uid from user name ({})", arg_uid);

            if (!result)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "User {} is not found in the system", arg_uid);

            uid = entry.pw_uid;
        }

        if (uid == 0 && getuid() != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "User has id 0, but dropping privileges to uid 0 does not make sense");

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
        std::cout << "Usage: ./clickhouse su user:group ..." << std::endl;
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
