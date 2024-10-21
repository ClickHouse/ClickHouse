#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <fmt/format.h>
#include <vector>

#include <sys/types.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>

#include <string>
#include <unordered_map>
#include <bitset>
#include <linux/capability.h>
#include <sys/prctl.h>
#include <sys/syscall.h>

/// "su" means "set user"
/// In fact, this program can set Unix user and group.
///
/// Usage:
/// clickhouse su [--preserve-cap=sys_nice,ipc_lock,sys_ptrace,net_admin] user[:group] args...
///
/// - will set user and, optionally, group and exec the remaining args.
///   user and group can be numeric identifiers or strings.
///
/// if preserve-cap is set then it will also try to preserve the linux capabilities after
/// switching UID and staring the command (does nothing if those capabilities are not permitted to use).
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

// The total number of capabilities (32 bits for _LINUX_CAPABILITY_VERSION_1)
const int NUM_CAPS = 32;

// Capability names are the same as in libcap-ng and setpriv tool
const std::unordered_map<std::string, int> cap_map =
{
    {"chown",            CAP_CHOWN},             //  0
    {"dac_override",     CAP_DAC_OVERRIDE},      //  1
    {"dac_read_search",  CAP_DAC_READ_SEARCH},   //  2
    {"fowner",           CAP_FOWNER},            //  3
    {"fsetid",           CAP_FSETID},            //  4
    {"kill",             CAP_KILL},              //  5
    {"setgid",           CAP_SETGID},            //  6
    {"setuid",           CAP_SETUID},            //  7
    {"setpcap",          CAP_SETPCAP},           //  8
    {"linux_immutable",  CAP_LINUX_IMMUTABLE},   //  9
    {"net_bind_service", CAP_NET_BIND_SERVICE},  // 10
    {"net_broadcast",    CAP_NET_BROADCAST},     // 11
    {"net_admin",        CAP_NET_ADMIN},         // 12
    {"net_raw",          CAP_NET_RAW},           // 13
    {"ipc_lock",         CAP_IPC_LOCK},          // 14
    {"ipc_owner",        CAP_IPC_OWNER},         // 15
    {"sys_module",       CAP_SYS_MODULE},        // 16
    {"sys_rawio",        CAP_SYS_RAWIO},         // 17
    {"sys_chroot",       CAP_SYS_CHROOT},        // 18
    {"sys_ptrace",       CAP_SYS_PTRACE},        // 19
    {"sys_pacct",        CAP_SYS_PACCT},         // 20
    {"sys_admin",        CAP_SYS_ADMIN},         // 21
    {"sys_boot",         CAP_SYS_BOOT},          // 22
    {"sys_nice",         CAP_SYS_NICE},          // 23
    {"sys_resource",     CAP_SYS_RESOURCE},      // 24
    {"sys_time",         CAP_SYS_TIME},          // 25
    {"sys_tty_config",   CAP_SYS_TTY_CONFIG},    // 26
    {"mknod",            CAP_MKNOD},             // 27
    {"lease",            CAP_LEASE},             // 28
    {"audit_write",      CAP_AUDIT_WRITE},       // 29
    {"audit_control",    CAP_AUDIT_CONTROL},     // 30
    {"setfcap",          CAP_SETFCAP}            // 31
};

// Function to parse the CSV string with list of capabilities and return the bitmask
std::bitset<NUM_CAPS> parseCapabilities(const std::string &csv)
{
    std::bitset<NUM_CAPS> bitmask;
    size_t start = 0;
    size_t end;

    while (start < csv.size())
    {
        end = csv.find(',', start);

        // If no more commas, treat this as the last capability
        if (end == std::string::npos)
            end = csv.size();

        std::string cap = csv.substr(start, end - start);

        if (!cap.empty())
        {
            auto it = cap_map.find(cap);

            if (it == cap_map.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown capability: {}", cap);

            bitmask.set(it->second);
        }

        start = end + 1;
    }

    return bitmask;
}

// takes the bitset of capabilities, and tries to make them effective & inheritable and then ambient.
void adjustCaps(std::bitset<NUM_CAPS>& caps)
{
    struct __user_cap_data_struct cap_data[2];
    struct __user_cap_header_struct cap_hdr;

    cap_hdr.version = _LINUX_CAPABILITY_VERSION_1;
    cap_hdr.pid = 0;  // current process

    // Get current capabilities (Permitted, Effective, Inheritable)
    if (syscall(SYS_capget, &cap_hdr, &cap_data) != 0)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to get current capabilities");
    }

    // Modify caps: only retain capabilities that are permitted
    for (const auto& cap_pair : cap_map)
    {
        int cap_bit = cap_pair.second;
        if (caps.test(cap_bit))
        {
            // Check if the capability is permitted
            if (cap_data[0].permitted & (1 << cap_bit))
            {
                // Set the effective and inheritable bits if permitted
                cap_data[0].effective   |= (1 << cap_bit);
                cap_data[0].inheritable |= (1 << cap_bit);
            }
            else
            {
                // Capability not permitted, clear it from the caps bitmask (so we will not try to set it ambient either).
                caps.reset(cap_bit);
                // std::cerr << "Warning: Capability " << cap_pair.first << " is not permitted. Skipping." << std::endl;
            }
        }
    }

    if (syscall(SYS_capset, &cap_hdr, &cap_data) != 0)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to set capabilities");
    }

    // Ambient capabilities will be preserved as effective & permitted across execve() calls,
    // but that requires the capability to be inheritable & effective.
    for (const auto& cap_pair : cap_map)
    {
        int cap_bit = cap_pair.second;
        if (caps.test(cap_bit))
        {
            if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, cap_bit, 0, 0) != 0)
            {
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to set ambient capability for cap: {}", cap_pair.first);
            }
        }
    }
}

// Converts a bitset representing capabilities into a CSV of capability names
std::string capabilitiesToCSV(const std::bitset<NUM_CAPS>& bitmask)
{
    std::string csv;
    bool first = true;

    for (const auto& cap_pair : cap_map)
    {
        int cap_bit = cap_pair.second;
        if (bitmask.test(cap_bit))
        {
            if (!first)
            {
                csv += ",";
            }
            csv += cap_pair.first;
            first = false;
        }
    }

    return csv.empty() ? "none" : csv;
}

// for debug, from command line you can do
// cat /proc/$(pidof -s clickhouse-server)/status | grep Cap
void printAllCapabilities()
{
    std::cout << "=== printAllCapabilities ===\n";
    struct __user_cap_data_struct cap_data[2];
    struct __user_cap_header_struct cap_hdr;

    cap_hdr.version = _LINUX_CAPABILITY_VERSION_1;
    cap_hdr.pid = 0;  // current process

    // Get current capabilities (Permitted, Effective, Inheritable)
    if (syscall(SYS_capget, &cap_hdr, &cap_data) != 0)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to get current capabilities");
    }

    std::bitset<NUM_CAPS> permitted(cap_data[0].permitted);
    std::bitset<NUM_CAPS> inheritable(cap_data[0].inheritable);
    std::bitset<NUM_CAPS> effective(cap_data[0].effective);

    std::bitset<NUM_CAPS> ambient;
    for (const auto& cap_pair : cap_map)
    {
        int cap_bit = cap_pair.second;
        int ret = prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_IS_SET, cap_bit, 0, 0);
        if (ret > 0)
        {
            ambient.set(cap_bit); // Ambient capability is set
        }
    }

    std::cout << "CapInh: " << std::setfill('0') << std::setw(16) << std::hex << inheritable.to_ulong() << " (" << capabilitiesToCSV(inheritable) << ")\n";
    std::cout << "CapPrm: " << std::setfill('0') << std::setw(16) << std::hex << permitted.to_ulong()   << " (" << capabilitiesToCSV(permitted) << ")\n";
    std::cout << "CapEff: " << std::setfill('0') << std::setw(16) << std::hex << effective.to_ulong()   << " (" << capabilitiesToCSV(effective) << ")\n";
    std::cout << "CapAmb: " << std::setfill('0') << std::setw(16) << std::hex << ambient.to_ulong()     << " (" << capabilitiesToCSV(ambient) << ")\n";
}

/// Linux man page says it is enough. Nevertheless, we will check if it's not enough and throw.
constexpr size_t buf_size = 16384;

std::optional<gid_t> parseGroupID(const std::string& arg_gid)
{
    if (!arg_gid.empty())
    {
        gid_t gid = 0;
        if (!tryParse(gid, arg_gid) || gid == 0)
        {
            std::unique_ptr<char[]> buf(new char[buf_size]);
            group entry{};
            group * result{};

            if (0 != getgrnam_r(arg_gid.data(), &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getgrnam_r' to obtain gid from group name ({})", arg_gid);

            if (!result)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group {} is not found in the system", arg_gid);

            gid = entry.gr_gid;
        }
        return gid;
    }
    return std::nullopt;
}

std::optional<uid_t> parseUserID(const std::string& arg_uid)
{
    if (!arg_uid.empty())
    {
        /// Is it numeric id or name?
        uid_t uid = 0;

        if (!tryParse(uid, arg_uid) || uid == 0)
        {
            std::unique_ptr<char[]> buf(new char[buf_size]);
            passwd entry{};
            passwd * result{};
            if (0 != getpwnam_r(arg_uid.data(), &entry, buf.get(), buf_size, &result))
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'getpwnam_r' to obtain uid from user name ({})", arg_uid);

            if (!result)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "User {} is not found in the system", arg_uid);

            uid = entry.pw_uid;
        }
        return uid;
    }
    return std::nullopt;
}

void setUserAndGroup(std::optional<uid_t> uid, std::optional<gid_t> gid, std::bitset<NUM_CAPS> caps)
{
    gid_t current_gid = getgid();
    uid_t current_uid = getuid();

    // std::cerr << "Will try to preserve: " << capabilitiesToCSV(caps) << "\n";

    if (uid.has_value() && *uid == 0 && current_uid != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dropping privileges to uid 0 does not make sense");
    }

    if (gid.has_value() && *gid == 0 && current_gid != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dropping privileges to gid 0 does not make sense");
    }

    /// Set the group first, because if we set user, the privileges will be already dropped and we will not be able to set the group later.
    if (gid.has_value() && *gid != current_gid && setgid(*gid) != 0)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'setgid' to group ({})", *gid);
    }

    // Set uid at the end
    if (uid.has_value() && *uid != current_uid)
    {
        // this allows to preserve the permitted capabilities after setuid
        if (caps.any() && prctl(PR_SET_KEEPCAPS, 1L) != 0)
        {
            std::cerr << "Failed to set PR_SET_KEEPCAPS" << '\n';
        }

        if (setuid(*uid) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot do 'setuid' to user ({})", *uid);
    }

    // PR_SET_KEEPCAPS allows the process to retain permitted capabilities after setuid,
    // but effective capabilities are reset to zero after setuid anyway.
    //
    // We need to set the effective capabilities again.
    //
    // Additionally, we need to prepare for execvp:
    // To preserve capabilities during an execvp call, they must be inheritable
    // (so that they remain permitted after execvp).
    //
    // Finally, once a capability is both effective and inheritable, we can
    // set it as ambient, ensuring that execvp retains it as effective.

    try
    {
        if (caps.any())
            adjustCaps(caps);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(false) << '\n';
    }

    // printAllCapabilities(); // pay attention that the capabilies may be changed once again after execvp
}

}


int mainEntryClickHouseSU(int argc, char ** argv)
try
{
    // disable buffering in stdout, as after setuid stdout may misbehave otherwise.
    setvbuf(stdout, nullptr, _IONBF, 0);  // _IONBF means "no buffering"

    using namespace DB;

    if (argc < 3)
    {
        std::cout << "A tool similar to 'su'" << std::endl;
        std::cout << "Usage: ./clickhouse su [--preserve-cap=sys_nice,ipc_lock,sys_ptrace,net_admin] user:group ..." << std::endl;
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }


    int arg_read_offset = 1;
    std::string capabilities;
    std::string user;
    std::string group;

    // Check if the first argument is the --preserve-cap option
    if (std::string(argv[arg_read_offset]).find("--preserve-cap=") == 0)
    {
        capabilities = std::string(argv[arg_read_offset]).substr(15);  // Extract the capability string after `--preserve-cap=`
        arg_read_offset++;  // Move to the next argument for user:group
    }

    std::string_view user_and_group = argv[arg_read_offset];

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

    arg_read_offset++;  // Move to the next argument for the command

    setUserAndGroup(parseUserID(user), parseGroupID(group), parseCapabilities(capabilities));

    std::vector<char *> new_argv;
    new_argv.reserve(argc + 1 - arg_read_offset);
    new_argv.insert(new_argv.begin(), argv + arg_read_offset, argv + argc);
    new_argv.push_back(nullptr);

    execvp(new_argv.front(), new_argv.data());

    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot execvp");
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(false) << '\n';
    return 1;
}
