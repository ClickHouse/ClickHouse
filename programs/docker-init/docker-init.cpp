/// clickhouse docker-init — Docker entrypoint for distroless ClickHouse images.
/// Replaces entrypoint.sh in shell-free environments (no bash, no coreutils).
///
/// Usage:
///   clickhouse docker-init [--keeper] [-- <extra-server-args>...]
///
/// Environment variables (same as entrypoint.sh):
///   CLICKHOUSE_CONFIG, CLICKHOUSE_RUN_AS_ROOT, CLICKHOUSE_DO_NOT_CHOWN,
///   CLICKHOUSE_UID, CLICKHOUSE_GID, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
///   CLICKHOUSE_PASSWORD_FILE, CLICKHOUSE_DB, CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT,
///   CLICKHOUSE_SKIP_USER_SETUP, CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS,
///   CLICKHOUSE_INIT_TIMEOUT, CLICKHOUSE_WATCHDOG_ENABLE, KEEPER_CONFIG

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace
{

/// Path to the clickhouse multi-tool binary, derived from argv[0].
/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::string g_clickhouse_binary;

/// Set by the SIGTERM/SIGINT handler during init to request graceful shutdown.
/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
volatile sig_atomic_t g_shutdown_requested = 0;

/// PID of the temporary init server, so the signal handler can forward SIGTERM.
/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
volatile pid_t g_init_server_pid = 0;

void shutdownHandler(int sig)
{
    g_shutdown_requested = 1;

    /// Forward the signal to the temporary server if one is running.
    pid_t pid = g_init_server_pid;
    if (pid > 0)
        kill(pid, sig);
}

/// Get an environment variable value, returning default_value if not set.
std::string getEnv(const char * name, const std::string & default_value = "")
{
    const char * val = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return val ? std::string(val) : default_value;
}

/// Build an execvp-compatible argv array from a vector of strings.
std::vector<char *> buildArgv(const std::vector<std::string> & args)
{
    std::vector<char *> argv;
    argv.reserve(args.size() + 1);
    for (const auto & a : args)
        argv.push_back(const_cast<char *>(a.c_str())); // NOLINT(cppcoreguidelines-pro-type-const-cast)
    argv.push_back(nullptr);
    return argv;
}

/// Run a command and wait for it. Returns the exit code (or -1 on error).
int runCommand(const std::vector<std::string> & args)
{
    pid_t pid = fork();
    if (pid < 0)
        return -1;

    if (pid == 0)
    {
        auto argv = buildArgv(args);
        execvp(argv[0], argv.data());
        _exit(127);
    }

    int status = 0;
    while (waitpid(pid, &status, 0) < 0)
        if (errno != EINTR)
            return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/// Run a command, capture its stdout, return {exit_code, output_lines}.
std::pair<int, std::vector<std::string>> captureCommand(const std::vector<std::string> & args)
{
    int pipefd[2];
    if (pipe(pipefd) < 0)
        return {-1, {}};

    pid_t pid = fork();
    if (pid < 0)
    {
        (void)close(pipefd[0]);
        (void)close(pipefd[1]);
        return {-1, {}};
    }

    if (pid == 0)
    {
        (void)close(pipefd[0]);
        if (dup2(pipefd[1], STDOUT_FILENO) < 0)
            _exit(127);
        (void)close(pipefd[1]);

        /// Suppress stderr to avoid noise from --try extractions.
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0)
        {
            (void)dup2(devnull, STDERR_FILENO);
            (void)close(devnull);
        }

        auto argv = buildArgv(args);
        execvp(argv[0], argv.data());
        _exit(127);
    }

    (void)close(pipefd[1]);

    std::string output;
    char buf[4096];
    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof(buf))) > 0)
        output.append(buf, static_cast<size_t>(n));
    (void)close(pipefd[0]);

    int status = 0;
    while (waitpid(pid, &status, 0) < 0)
        if (errno != EINTR)
            return {-1, {}};

    /// Split output into non-empty lines.
    std::vector<std::string> lines;
    {
        size_t pos = 0;
        while (pos < output.size())
        {
            size_t found = output.find('\n', pos);
            if (found == std::string::npos)
                found = output.size();
            std::string line = output.substr(pos, found - pos);
            if (!line.empty() && line.back() == '\r')
                line.pop_back();
            if (!line.empty())
                lines.push_back(std::move(line));
            pos = found + 1;
        }
    }

    return {WIFEXITED(status) ? WEXITSTATUS(status) : -1, std::move(lines)};
}

/// Run two commands connected by a pipe: lhs | rhs.
/// Returns the exit code of the rhs process.
int runPipeline(const std::vector<std::string> & lhs, const std::vector<std::string> & rhs)
{
    int pipefd[2];
    if (pipe(pipefd) < 0)
        return -1;

    pid_t lhs_pid = fork();
    if (lhs_pid < 0)
    {
        (void)close(pipefd[0]);
        (void)close(pipefd[1]);
        return -1;
    }
    if (lhs_pid == 0)
    {
        (void)close(pipefd[0]);
        (void)dup2(pipefd[1], STDOUT_FILENO);
        (void)close(pipefd[1]);
        auto argv = buildArgv(lhs);
        execvp(argv[0], argv.data());
        _exit(127);
    }

    pid_t rhs_pid = fork();
    if (rhs_pid < 0)
    {
        (void)close(pipefd[0]);
        (void)close(pipefd[1]);
        kill(lhs_pid, SIGTERM);
        while (waitpid(lhs_pid, nullptr, 0) < 0 && errno == EINTR) {}
        return -1;
    }
    if (rhs_pid == 0)
    {
        (void)close(pipefd[1]);
        (void)dup2(pipefd[0], STDIN_FILENO);
        (void)close(pipefd[0]);
        auto argv = buildArgv(rhs);
        execvp(argv[0], argv.data());
        _exit(127);
    }

    (void)close(pipefd[0]);
    (void)close(pipefd[1]);

    int lhs_status = 0;
    int rhs_status = 0;
    while (waitpid(lhs_pid, &lhs_status, 0) < 0 && errno == EINTR) {}
    while (waitpid(rhs_pid, &rhs_status, 0) < 0 && errno == EINTR) {}

    return WIFEXITED(rhs_status) ? WEXITSTATUS(rhs_status) : -1;
}

/// Returns true if the string is a safe ClickHouse identifier:
/// alphanumeric + underscore, not starting with a digit.
/// Used to validate CLICKHOUSE_USER and CLICKHOUSE_DB before embedding in SQL/XML.
bool isValidIdentifier(const std::string & s)
{
    if (s.empty())
        return false;
    if (std::isdigit(static_cast<unsigned char>(s[0])))
        return false;
    for (unsigned char c : s)
        if (!std::isalnum(c) && c != '_')
            return false;
    return true;
}

/// Extract a single value from a ClickHouse config key via `clickhouse extract-from-config`.
/// Returns an empty string if the key is absent (--try flag suppresses errors).
std::string extractConfigValue(const std::string & config_file, const std::string & key, bool use_users = false)
{
    std::vector<std::string> args = {
        g_clickhouse_binary, "extract-from-config",
        "--config-file", config_file,
        "--key", key,
        "--try",
    };
    if (use_users)
        args.emplace_back("--users");

    auto [code, lines] = captureCommand(args);
    return (code == 0 && !lines.empty()) ? lines[0] : std::string{};
}

/// Extract multiple values from a ClickHouse config key (wildcard patterns return multiple lines).
std::vector<std::string> extractConfigValues(const std::string & config_file, const std::string & key)
{
    auto [code, lines] = captureCommand({
        g_clickhouse_binary, "extract-from-config",
        "--config-file", config_file,
        "--key", key,
        "--try",
    });
    return (code == 0) ? std::move(lines) : std::vector<std::string>{};
}

/// Recursively chown a path. Logs warnings but does not abort on failure.
void recursiveChown(const std::string & path_str, uid_t uid, gid_t gid)
{
    if (lchown(path_str.c_str(), uid, gid) < 0)
        std::cerr << "docker-init: warning: lchown " << path_str << ": " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)

    std::error_code ec;
    if (!fs::is_directory(path_str, ec))
        return;

    for (const auto & entry : fs::recursive_directory_iterator(path_str, fs::directory_options::skip_permission_denied, ec))
    {
        if (lchown(entry.path().c_str(), uid, gid) < 0)
            std::cerr << "docker-init: warning: lchown " << entry.path() << ": " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)
    }
}

/// Create a directory (and all parents) and optionally chown it.
///
/// Three cases:
///   1. do_chown=true (root, normal mode): create with fs::create_directories, then chown.
///   2. do_chown=false, running as root (CLICKHOUSE_DO_NOT_CHOWN=1): delegate to
///      `clickhouse su UID:GID` so the directory is created as the target user.
///      This handles NFS mounts where root is mapped to nobody.
///   3. do_chown=false, running as non-root: create directly — we are already the target user.
bool createDirectoryAndChown(const std::string & dir, uid_t uid, gid_t gid, bool do_chown)
{
    if (dir.empty())
        return true;

    if (do_chown)
    {
        std::error_code ec;
        fs::create_directories(dir, ec);
        if (ec)
        {
            std::cerr << "docker-init: couldn't create directory " << dir << ": " << ec.message() << "\n";
            return false;
        }

        /// Chown only if the owner needs to change (avoids slow recursive chown on already-correct dirs).
        struct stat st{};
        if (stat(dir.c_str(), &st) == 0 && (st.st_uid != uid || st.st_gid != gid))
            recursiveChown(dir, uid, gid);

        return true;
    }

    if (getuid() == 0)
    {
        /// Running as root with CLICKHOUSE_DO_NOT_CHOWN or CLICKHOUSE_RUN_AS_ROOT.
        /// On NFS mounts root may be remapped to nobody, so create the directory as
        /// the target user. Fork a child that drops privileges before calling
        /// fs::create_directories — distroless has no mkdir binary.
        pid_t pid = fork();
        if (pid < 0)
        {
            std::cerr << "docker-init: fork failed for directory creation: " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)
            return false;
        }
        if (pid == 0)
        {
            if (setgroups(0, nullptr) < 0 || setgid(gid) < 0 || setuid(uid) < 0)
                _exit(1);
            std::error_code ec;
            fs::create_directories(dir, ec);
            _exit(ec ? 1 : 0);
        }
        int status = 0;
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        {
            /// Fallback: try direct creation (works when root is not remapped).
            std::error_code ec;
            fs::create_directories(dir, ec);
            if (ec)
            {
                std::cerr << "docker-init: couldn't create directory " << dir << ": " << ec.message() << "\n";
                return false;
            }
        }
        return true;
    }

    /// Non-root: we are already running as UID:GID, so create the directory directly.
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec)
    {
        std::cerr << "docker-init: couldn't create directory " << dir << ": " << ec.message() << "\n";
        return false;
    }
    return true;
}

/// Write the user management XML to `/etc/clickhouse-server/users.d/default-user.xml`.
/// Returns false if a user-requested setup (CLICKHOUSE_USER/PASSWORD/ACCESS_MANAGEMENT)
/// is invalid — caller should treat this as fatal.
bool manageClickHouseUser(
    const std::string & config_file,
    const std::string & clickhouse_user,
    const std::string & clickhouse_password,
    const std::string & access_management,
    bool skip_user_setup)
{
    if (skip_user_setup)
    {
        std::cerr << "docker-init: explicitly skip changing user 'default'\n";
        return true;
    }

    const std::string users_d_dir = "/etc/clickhouse-server/users.d";
    const std::string default_user_xml = users_d_dir + "/default-user.xml";

    std::error_code ec;
    fs::create_directories(users_d_dir, ec);

    /// Detect whether the default user was customised via a mounted config file.
    bool clickhouse_default_changed = false;
    std::string users_xml_path = extractConfigValue(config_file, "user_directories.users_xml.path");
    if (!users_xml_path.empty())
    {
        std::string abs_users_xml = (users_xml_path[0] == '/')
            ? users_xml_path
            : (fs::path(config_file).parent_path() / users_xml_path).string();

        auto join = [](const std::vector<std::string> & v)
        {
            std::string s;
            for (const auto & line : v)
                s += line + "\n";
            return s;
        };

        auto [c1, original] = captureCommand({
            g_clickhouse_binary, "extract-from-config",
            "--config-file", abs_users_xml,
            "--key", "users.default", "--try",
        });
        auto [c2, processed] = captureCommand({
            g_clickhouse_binary, "extract-from-config",
            "--config-file", config_file,
            "--users", "--key", "users.default", "--try",
        });

        if (c1 == 0 && c2 == 0 && join(original) != join(processed))
            clickhouse_default_changed = true;
    }

    bool has_custom_user = !clickhouse_user.empty() && clickhouse_user != "default";
    bool has_password = !clickhouse_password.empty();
    bool has_access_mgmt = access_management != "0";

    if (has_custom_user || has_password || has_access_mgmt)
    {
        if (access_management != "0" && access_management != "1")
        {
            std::cerr << "docker-init: error: CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT must be '0' or '1', got '"
                      << access_management << "'\n";
            return false;
        }

        if (!isValidIdentifier(clickhouse_user))
        {
            std::cerr << "docker-init: error: CLICKHOUSE_USER '" << clickhouse_user
                      << "' contains characters not allowed in an XML element name; "
                         "use only alphanumeric characters and underscores\n";
            return false;
        }

        std::cerr << "docker-init: create new user '" << clickhouse_user << "' instead 'default'\n";

        /// Escape CDATA end marker: ]]> → ]]]]><![CDATA[>
        std::string escaped_password;
        {
            std::string_view src = clickhouse_password;
            const std::string_view needle = "]]>";
            const std::string_view replacement = "]]]]><![CDATA[>";
            size_t pos = 0;
            size_t found;
            while ((found = src.find(needle, pos)) != std::string_view::npos)
            {
                escaped_password.append(src, pos, found - pos);
                escaped_password += replacement;
                pos = found + needle.size();
            }
            escaped_password.append(src, pos, src.size() - pos);
        }

        {
            std::ofstream f(default_user_xml);
            f << "<clickhouse>\n"
              << "  <!-- Docs: <https://clickhouse.com/docs/operations/settings/settings_users/> -->\n"
              << "  <users>\n"
              << "    <!-- Remove default user -->\n"
              << "    <default remove=\"remove\">\n"
              << "    </default>\n"
              << "\n"
              << "    <" << clickhouse_user << ">\n"
              << "      <profile>default</profile>\n"
              << "      <networks>\n"
              << "        <ip>::/0</ip>\n"
              << "      </networks>\n"
              << "      <password><![CDATA[" << escaped_password << "]]></password>\n"
              << "      <quota>default</quota>\n"
              << "      <access_management>" << access_management << "</access_management>\n"
              << "    </" << clickhouse_user << ">\n"
              << "  </users>\n"
              << "</clickhouse>\n";
            if (!f.good())
                std::cerr << "docker-init: error: failed to write " << default_user_xml << "\n";
        }
    }
    else if (clickhouse_default_changed)
    {
        /// A mounted config already customised the user — leave it as-is.
    }
    else
    {
        std::cerr << "docker-init: neither CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set, "
                     "disabling network access for user 'default'\n";
        {
            std::ofstream f(default_user_xml);
            f << "<clickhouse>\n"
              << "  <!-- Docs: <https://clickhouse.com/docs/operations/settings/settings_users/> -->\n"
              << "  <users>\n"
              << "    <default>\n"
              << "      <!-- User default is available only locally -->\n"
              << "      <networks>\n"
              << "        <ip>::1</ip>\n"
              << "        <ip>127.0.0.1</ip>\n"
              << "      </networks>\n"
              << "    </default>\n"
              << "  </users>\n"
              << "</clickhouse>\n";
            if (!f.good())
                std::cerr << "docker-init: error: failed to write " << default_user_xml << "\n";
        }
    }
    return true;
}

/// Returns false on first error (fail-fast, matches shell `set -e`).
bool createClickHouseDatabase(
    const std::vector<std::string> & client_base,
    const std::string & clickhouse_db)
{
    if (clickhouse_db.empty())
        return true;
    if (!isValidIdentifier(clickhouse_db))
    {
        std::cerr << "docker-init: error: CLICKHOUSE_DB '" << clickhouse_db
                  << "' contains characters not safe for use in SQL; "
                     "use only alphanumeric characters and underscores\n";
        return false;
    }
    std::cerr << "docker-init: create database '" << clickhouse_db << "'\n";
    std::vector<std::string> args = client_base;
    args.insert(args.end(), {"-q", "CREATE DATABASE IF NOT EXISTS " + clickhouse_db});
    if (runCommand(args) != 0)
    {
        std::cerr << "docker-init: error: failed to create database '" << clickhouse_db << "'\n";
        return false;
    }
    return true;
}

/// Returns false on first script failure (fail-fast, matches shell `set -e`).
bool runInitScripts(const std::vector<std::string> & client_base)
{
    std::error_code ec;
    if (!fs::is_directory("/docker-entrypoint-initdb.d", ec))
        return true;
    std::vector<fs::path> init_files;
    for (const auto & entry : fs::directory_iterator("/docker-entrypoint-initdb.d", ec))
        init_files.push_back(entry.path());
    std::sort(init_files.begin(), init_files.end());

    for (const auto & path : init_files)
    {
        std::string filename = path.filename().string();

        if (filename.ends_with(".sql") && !filename.ends_with(".sql.gz"))
        {
            std::cerr << "docker-init: running " << path << "\n";
            std::vector<std::string> args = client_base;
            args.emplace_back("--queries-file");
            args.push_back(path.string());
            if (runCommand(args) != 0)
            {
                std::cerr << "docker-init: error: init script " << path << " failed\n";
                return false;
            }
        }
        else if (filename.ends_with(".sql.gz"))
        {
            std::cerr << "docker-init: running " << path << " (decompressing)\n";
            /// Decompress via clickhouse-local (auto-detects .gz) and pipe to clickhouse-client.
            /// Escape single quotes in the path to prevent SQL injection via crafted filenames.
            std::string escaped_path;
            for (char c : path.string())
            {
                if (c == '\'')
                    escaped_path += "''";
                else
                    escaped_path += c;
            }
            std::vector<std::string> decompress_args = {
                g_clickhouse_binary, "local",
                "--query",
                "SELECT * FROM file('" + escaped_path + "', RawBLOB) FORMAT RawBLOB",
            };
            if (runPipeline(decompress_args, client_base) != 0)
            {
                std::cerr << "docker-init: error: init script " << path << " failed\n";
                return false;
            }
        }
        else if (filename.ends_with(".sh"))
        {
            std::cerr << "docker-init: WARNING: shell scripts cannot run in a distroless "
                         "environment, skipping " << path << "\n";
        }
        else
        {
            std::cerr << "docker-init: ignoring " << path << "\n";
        }
    }
    return true;
}

/// Start a temporary ClickHouse server, run init scripts, then stop it.
bool initClickHouseDB(
    const std::string & config_file,
    const std::string & data_dir,
    const std::string & clickhouse_user,
    const std::string & clickhouse_password,
    uid_t run_uid,
    gid_t run_gid,
    const std::vector<std::string> & extra_server_args,
    bool always_run_initdb)
{
    /// Skip if data directory is already initialised and CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS is unset.
    bool database_exists = fs::is_directory(data_dir + "/data");
    if (!always_run_initdb && database_exists)
    {
        std::cerr << "docker-init: ClickHouse data directory appears to contain a database; "
                     "skipping initialization\n";
        return true;
    }

    std::string clickhouse_db = getEnv("CLICKHOUSE_DB");

    /// Check whether /docker-entrypoint-initdb.d has any files.
    std::error_code ec;
    bool has_init_files = fs::is_directory("/docker-entrypoint-initdb.d", ec)
        && fs::directory_iterator("/docker-entrypoint-initdb.d", ec) != fs::directory_iterator{};

    if (!has_init_files && clickhouse_db.empty())
        return true;

    std::string native_port = extractConfigValue(config_file, "tcp_port");
    if (native_port.empty())
        native_port = "9000";

    std::string run_as = std::to_string(run_uid) + ":" + std::to_string(run_gid);

    /// Start a temporary server bound only to localhost.
    /// The "--" separator is required: positional arguments after "--" override config.xml
    /// properties (e.g. --listen_host=127.0.0.1), while options before "--" are parsed by
    /// Poco and must be registered. Without "--", Poco rejects "--listen_host" as unknown.
    std::vector<std::string> server_args = {
        g_clickhouse_binary, "su", run_as, "clickhouse-server",
        "--config-file=" + config_file,
        "--", "--listen_host=127.0.0.1",
    };
    for (const auto & arg : extra_server_args)
        server_args.push_back(arg);

    if (g_shutdown_requested)
        return true;

    pid_t server_pid = fork();
    if (server_pid < 0)
    {
        std::cerr << "docker-init: failed to fork temporary server\n";
        return false;
    }

    if (server_pid == 0)
    {
        auto argv = buildArgv(server_args);
        execvp(argv[0], argv.data());
        _exit(127);
    }

    /// Allow the signal handler to forward SIGTERM to the temp server.
    g_init_server_pid = server_pid;

    /// Poll until the server accepts connections.
    /// This is a service-readiness wait, not a race condition workaround.
    int tries = 1000;
    {
        std::string timeout_str = getEnv("CLICKHOUSE_INIT_TIMEOUT", "1000");
        try
        {
            tries = std::stoi(timeout_str);
        }
        catch (const std::exception &)
        {
            std::cerr << "docker-init: warning: invalid CLICKHOUSE_INIT_TIMEOUT '"
                      << timeout_str << "', using default 1000\n";
        }
    }
    bool server_ready = false;

    while (tries > 0 && !server_ready && !g_shutdown_requested)
    {
        pid_t check_pid = fork();
        if (check_pid < 0)
        {
            /// fork failed — skip this iteration and retry.
            --tries;
            sleep(1); // NOLINT(concurrency-mt-unsafe)
            continue;
        }
        if (check_pid == 0)
        {
            int devnull = open("/dev/null", O_WRONLY);
            if (devnull >= 0)
            {
                dup2(devnull, STDOUT_FILENO);
                dup2(devnull, STDERR_FILENO);
                close(devnull);
            }
            /// Keep args in a named variable so the strings outlive argv.
            std::vector<std::string> check_args = {
                g_clickhouse_binary, "client",
                "--host", "127.0.0.1",
                "--port", native_port,
                "-u", clickhouse_user,
                "--password", clickhouse_password,
                "--query", "SELECT 1",
            };
            auto argv = buildArgv(check_args);
            execvp(argv[0], argv.data());
            _exit(127);
        }

        int check_status = 0;
        while (waitpid(check_pid, &check_status, 0) < 0 && errno == EINTR) {}
        if (WIFEXITED(check_status) && WEXITSTATUS(check_status) == 0)
        {
            server_ready = true;
        }
        else
        {
            --tries;
            sleep(1); // NOLINT(concurrency-mt-unsafe) -- Wait between health-check retries — not a race condition fix.
        }
    }

    if (!server_ready)
    {
        if (g_shutdown_requested)
            std::cerr << "docker-init: shutdown requested, stopping init server\n";
        else
            std::cerr << "docker-init: ClickHouse init process timed out\n";
        kill(server_pid, SIGTERM);
        while (waitpid(server_pid, nullptr, 0) < 0 && errno == EINTR) {}
        g_init_server_pid = 0;
        return false;
    }

    std::vector<std::string> client_base = {
        g_clickhouse_binary, "client",
        "--multiquery",
        "--host", "127.0.0.1",
        "--port", native_port,
        "-u", clickhouse_user,
        "--password", clickhouse_password,
    };

    const bool ok = createClickHouseDatabase(client_base, clickhouse_db)
                 && runInitScripts(client_base);

    /// Always stop the temporary server regardless of init result.
    kill(server_pid, SIGTERM);
    int server_status = 0;
    while (waitpid(server_pid, &server_status, 0) < 0 && errno == EINTR) {}
    g_init_server_pid = 0;
    if (!WIFEXITED(server_status) || WEXITSTATUS(server_status) != 0)
        std::cerr << "docker-init: warning: init server did not exit cleanly\n";
    return ok;
}

} // anonymous namespace


int mainEntryClickHouseDockerInit(int argc, char ** argv)
{
    g_clickhouse_binary = (argc > 0 && argv[0][0] != '\0') ? argv[0] : "clickhouse";

    bool keeper_mode = false;
    bool show_help = false;
    bool separator_seen = false;
    std::vector<std::string> extra_args;

    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg = argv[i];

        if (!separator_seen && (arg == "--help" || arg == "-h"))
        {
            show_help = true;
            break;
        }
        else if (!separator_seen && arg == "--keeper")
            keeper_mode = true;
        else if (!separator_seen && arg == "--")
            separator_seen = true;
        else
            extra_args.emplace_back(arg);
    }

    if (show_help)
    {
        std::cout
            << "Usage: clickhouse docker-init [--keeper] [-- <extra-args>...]\n"
               "Docker entrypoint for distroless ClickHouse images.\n"
               "\nOptions:\n"
               "  --keeper   Start ClickHouse Keeper instead of server\n"
               "  --help     Show this help message\n"
               "\nEnvironment variables (server mode):\n"
               "  CLICKHOUSE_CONFIG                   Path to config file "
               "(default: /etc/clickhouse-server/config.xml)\n"
               "  CLICKHOUSE_RUN_AS_ROOT              Run as root (0/1)\n"
               "  CLICKHOUSE_DO_NOT_CHOWN             Skip chown operations (0/1)\n"
               "  CLICKHOUSE_UID                      Override UID to run as\n"
               "  CLICKHOUSE_GID                      Override GID to run as\n"
               "  CLICKHOUSE_USER                     Default user name (default: default)\n"
               "  CLICKHOUSE_PASSWORD                 Default user password\n"
               "  CLICKHOUSE_PASSWORD_FILE            File containing password\n"
               "  CLICKHOUSE_DB                       Database to create on init\n"
               "  CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT  Enable access management (0/1)\n"
               "  CLICKHOUSE_SKIP_USER_SETUP          Skip user setup (0/1)\n"
               "  CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS  Always run init scripts\n"
               "  CLICKHOUSE_INIT_TIMEOUT             Max retries for server readiness (default: 1000)\n"
               "  CLICKHOUSE_WATCHDOG_ENABLE          Enable watchdog (default: 0)\n"
               "\nEnvironment variables (keeper mode):\n"
               "  KEEPER_CONFIG                       Path to keeper config file\n"
               "  CLICKHOUSE_DATA_DIR                 Data directory (default: /var/lib/clickhouse)\n"
               "  LOG_DIR                             Log directory (default: /var/log/clickhouse-keeper)\n";
        return 0;
    }

    /// --- Passthrough mode ---
    /// If the first extra argument does not start with '--', treat it as a command to exec
    /// directly without server startup. This mirrors entrypoint.sh:
    ///   if [[ "$1" == "--"* ]]; then start server; fi; exec "$@"
    ///
    /// For recognized ClickHouse subcommand names (client, local, etc.) resolve the path
    /// via bin_dir so that multi-tool dispatch (by argv[0] basename) works correctly.
    /// For everything else (echo, date, bash, ...) let PATH resolution handle it.
    if (!extra_args.empty() && !extra_args[0].starts_with("--"))
    {
        static constexpr std::array clickhouse_tools = {
            "clickhouse-client",
            "clickhouse-local",
            "clickhouse-keeper-client",
            "clickhouse-benchmark",
            "clickhouse-format",
            "clickhouse-compressor",
            "clickhouse-obfuscator",
            "clickhouse-extract-from-config",
            "clickhouse-disks",
            "client",
            "local",
            "keeper-client",
            "benchmark",
            "format",
            "compressor",
            "obfuscator",
            "extract-from-config",
            "disks",
        };

        std::string cmd = extra_args[0];
        if (std::find(clickhouse_tools.begin(), clickhouse_tools.end(), extra_args[0]) != clickhouse_tools.end())
        {
            /// Build the full path to the symlink (e.g. /usr/bin/clickhouse-client).
            /// The symlink points to the clickhouse binary; dispatching is done by argv[0].
            /// Short names like "client" must be resolved to "clickhouse-client" since the
            /// distroless image only has "clickhouse-*" symlinks (not bare "client", "local", etc.).
            fs::path bin_dir = fs::path(g_clickhouse_binary).parent_path();
            std::string link_name = extra_args[0];
            if (!link_name.starts_with("clickhouse-"))
                link_name = "clickhouse-" + link_name;
            cmd = (bin_dir / link_name).string();
        }

        std::vector<std::string> exec_cmd = {cmd};
        for (std::size_t i = 1; i < extra_args.size(); ++i)
            exec_cmd.push_back(extra_args[i]);

        auto exec_argv = buildArgv(exec_cmd);
        execvp(exec_argv[0], exec_argv.data());
        std::cerr << "docker-init: failed to exec '" << extra_args[0] << "': " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)
        return 1;
    }

    /// --- Resolve identity ---
    uid_t current_uid = getuid();
    uid_t run_uid;
    gid_t run_gid;
    bool do_chown = true;

    if (getEnv("CLICKHOUSE_RUN_AS_ROOT") == "1" || getEnv("CLICKHOUSE_DO_NOT_CHOWN") == "1")
        do_chown = false;

    if (current_uid == 0)
    {
        if (getEnv("CLICKHOUSE_RUN_AS_ROOT") == "1")
        {
            run_uid = 0;
            run_gid = 0;
        }
        else
        {
            /// Default to the `clickhouse` system user if it exists, otherwise fall back to UID 101.
            uid_t default_uid = 101;
            gid_t default_gid = 101;
            const passwd * pw = getpwnam("clickhouse"); // NOLINT(concurrency-mt-unsafe)
            if (pw)
            {
                default_uid = pw->pw_uid;
                default_gid = pw->pw_gid;
            }

            std::string uid_str = getEnv("CLICKHOUSE_UID");
            std::string gid_str = getEnv("CLICKHOUSE_GID");
            run_uid = default_uid;
            run_gid = default_gid;
            try
            {
                if (!uid_str.empty())
                    run_uid = static_cast<uid_t>(std::stoul(uid_str));
                if (!gid_str.empty())
                    run_gid = static_cast<gid_t>(std::stoul(gid_str));
            }
            catch (const std::exception &)
            {
                std::cerr << "docker-init: warning: invalid CLICKHOUSE_UID/GID values, "
                             "using defaults\n";
            }
        }
    }
    else
    {
        /// Non-root: cannot chown, run as current user.
        run_uid = current_uid;
        run_gid = getgid();
        do_chown = false;
    }

    std::string run_as = std::to_string(run_uid) + ":" + std::to_string(run_gid);

    /// --- Keeper mode ---
    if (keeper_mode)
    {
        std::string keeper_config = getEnv("KEEPER_CONFIG", "/etc/clickhouse-keeper/keeper_config.xml");
        std::string data_dir = getEnv("CLICKHOUSE_DATA_DIR", "/var/lib/clickhouse");
        std::string log_dir = getEnv("LOG_DIR", "/var/log/clickhouse-keeper");

        for (const auto & dir : {data_dir, log_dir,
                                  data_dir + "/coordination",
                                  data_dir + "/coordination/log",
                                  data_dir + "/coordination/snapshots"})
        {
            if (!createDirectoryAndChown(dir, run_uid, run_gid, do_chown))
                return 1;
        }

        /// Default to disabled so Ctrl+C works in Docker. Don't override if already set.
        setenv("CLICKHOUSE_WATCHDOG_ENABLE", "0", 0); // NOLINT(concurrency-mt-unsafe)

        chdir(data_dir.c_str()); // NOLINT(bugprone-unused-return-value)

        std::vector<std::string> exec_args = {
            g_clickhouse_binary, "su", run_as, "clickhouse-keeper",
        };

        std::error_code ec;
        if (!keeper_config.empty() && fs::exists(keeper_config, ec))
            exec_args.push_back("--config-file=" + keeper_config);

        for (const auto & arg : extra_args)
            exec_args.push_back(arg);

        auto exec_argv = buildArgv(exec_args);
        execvp(exec_argv[0], exec_argv.data());
        std::cerr << "docker-init: failed to exec clickhouse keeper: " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)
        return 1;
    }

    /// --- Server mode ---
    std::string config_file = getEnv("CLICKHOUSE_CONFIG", "/etc/clickhouse-server/config.xml");

    /// Extract all relevant paths from the config.
    std::string data_dir = extractConfigValue(config_file, "path");
    std::string tmp_dir = extractConfigValue(config_file, "tmp_path");
    std::string user_files_path = extractConfigValue(config_file, "user_files_path");
    std::string format_schema_path = extractConfigValue(config_file, "format_schema_path");

    std::string log_dir;
    std::string log_path = extractConfigValue(config_file, "logger.log");
    if (!log_path.empty())
        log_dir = fs::path(log_path).parent_path().string();

    std::string error_log_dir;
    std::string error_log_path = extractConfigValue(config_file, "logger.errorlog");
    if (!error_log_path.empty())
        error_log_dir = fs::path(error_log_path).parent_path().string();

    auto disk_paths = extractConfigValues(config_file, "storage_configuration.disks.*.path");
    auto disk_metadata_paths = extractConfigValues(config_file, "storage_configuration.disks.*.metadata_path");

    /// Create and chown data directory first, then cd into it.
    if (!data_dir.empty() && !createDirectoryAndChown(data_dir, run_uid, run_gid, do_chown))
        return 1;

    chdir(data_dir.empty() ? "/" : data_dir.c_str()); // NOLINT(bugprone-unused-return-value)

    for (const auto & dir : {error_log_dir, log_dir, tmp_dir, user_files_path, format_schema_path})
    {
        if (!dir.empty() && !createDirectoryAndChown(dir, run_uid, run_gid, do_chown))
            return 1;
    }
    for (const auto & dir : disk_paths)
        if (!createDirectoryAndChown(dir, run_uid, run_gid, do_chown))
            return 1;
    for (const auto & dir : disk_metadata_paths)
        if (!createDirectoryAndChown(dir, run_uid, run_gid, do_chown))
            return 1;

    /// Resolve password (from env or file).
    std::string clickhouse_user = getEnv("CLICKHOUSE_USER", "default");
    std::string clickhouse_password = getEnv("CLICKHOUSE_PASSWORD");
    std::string password_file = getEnv("CLICKHOUSE_PASSWORD_FILE");
    if (!password_file.empty())
    {
        std::ifstream pf(password_file);
        if (pf.is_open())
            std::getline(pf, clickhouse_password);
        else
            std::cerr << "docker-init: warning: cannot read CLICKHOUSE_PASSWORD_FILE '"
                      << password_file << "': " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)
    }

    std::string access_management = getEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "0");
    bool skip_user_setup = (getEnv("CLICKHOUSE_SKIP_USER_SETUP") == "1");

    if (!manageClickHouseUser(config_file, clickhouse_user, clickhouse_password, access_management, skip_user_setup))
        return 1;

    /// Install signal handlers so `docker stop` during init triggers graceful shutdown.
    /// As PID 1, signals without a handler are silently dropped by the kernel.
    {
        struct sigaction sa{};
        sa.sa_handler = shutdownHandler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESTART;
        sigaction(SIGTERM, &sa, nullptr);
        sigaction(SIGINT, &sa, nullptr);
    }

    bool always_run_initdb = !getEnv("CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS").empty();
    if (!initClickHouseDB(config_file, data_dir, clickhouse_user, clickhouse_password,
                          run_uid, run_gid, extra_args, always_run_initdb))
        return 1;

    if (g_shutdown_requested)
    {
        std::cerr << "docker-init: shutdown requested during initialization, exiting\n";
        return 1;
    }

    /// Reset signal handlers before exec — the server handles its own signals.
    signal(SIGTERM, SIG_DFL); // NOLINT(cert-err33-c)
    signal(SIGINT, SIG_DFL); // NOLINT(cert-err33-c)

    /// Set watchdog env — default to disabled so Ctrl+C works in Docker.
    if (std::getenv("CLICKHOUSE_WATCHDOG_ENABLE") == nullptr) // NOLINT(concurrency-mt-unsafe)
        setenv("CLICKHOUSE_WATCHDOG_ENABLE", "0", 0); // NOLINT(concurrency-mt-unsafe)

    /// Replace this process with clickhouse-server via `clickhouse su`.
    std::vector<std::string> exec_args = {
        g_clickhouse_binary, "su", run_as, "clickhouse-server",
        "--config-file=" + config_file,
    };
    for (const auto & arg : extra_args)
        exec_args.push_back(arg);

    auto exec_argv = buildArgv(exec_args);
    execvp(exec_argv[0], exec_argv.data());
    std::cerr << "docker-init: failed to exec clickhouse server: " << strerror(errno) << "\n"; // NOLINT(concurrency-mt-unsafe)
    return 1;
}
