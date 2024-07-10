#include "common/options.h"


std::vector<Option> get_global_options() {
  return std::vector<Option>({
    Option("host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("local hostname")
    .set_long_description("if blank, ceph assumes the short hostname (hostname -s)")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("common")
    .add_tag("network"),

    Option("fsid", Option::TYPE_UUID, Option::LEVEL_BASIC)
    .set_description("cluster fsid (uuid)")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_tag("service"),

    Option("public_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
    .set_description("public-facing address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"}),

    Option("public_addrv", Option::TYPE_ADDRVEC, Option::LEVEL_BASIC)
    .set_description("public-facing address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"}),

    Option("public_bind_addr", Option::TYPE_ADDR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mon"),

    Option("cluster_addr", Option::TYPE_ADDR, Option::LEVEL_BASIC)
    .set_description("cluster-facing address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("osd")
    .add_tag("network"),

    Option("public_network", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Network(s) from which to choose a public address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"})
    .add_tag("network"),

    Option("public_network_interface", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Interface name(s) from which to choose an address from a public_network to bind to; public_network must also be specified.")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"})
    .add_tag("network")
    .add_see_also({"public_network"}),

    Option("cluster_network", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Network(s) from which to choose a cluster address to bind to")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("osd")
    .add_tag("network"),

    Option("cluster_network_interface", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Interface name(s) from which to choose an address from a cluster_network to bind to; cluster_network must also be specified.")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mds", "osd", "mgr"})
    .add_tag("network")
    .add_see_also({"cluster_network"}),

    Option("monmap", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to MonMap file")
    .set_long_description("This option is normally used during mkfs, but can also be used to identify which monitors to connect to.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CREATE)
    .add_service("mon"),

    Option("mon_host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("list of hosts or addresses to search for a monitor")
    .set_long_description("This is a list of IP addresses or hostnames that are separated by commas, whitespace, or semicolons. Hostnames are resolved via DNS. All A and AAAA records are included in the search list.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common"),

    Option("mon_host_override", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("monitor(s) to use overriding the MonMap")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common"),

    Option("mon_dns_srv_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("name of DNS SRV record to check for monitor addresses")
    .set_default("ceph-mon")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_tag("network")
    .add_see_also({"mon_host"}),

    Option("container_image", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("container image (used by cephadm orchestrator)")
    .set_default("docker.io/ceph/daemon-base:latest-master-devel")
    .set_flag(Option::FLAG_STARTUP),

    Option("no_config_file", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("signal that we don't require a config file to be present")
    .set_long_description("When specified, we won't be looking for a configuration file, and will instead expect that whatever options or values are required for us to work will be passed as arguments.")
    .set_default(false)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_tag("config"),

    Option("lockdep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable lockdep lock dependency analyzer")
    .set_default(false)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common"),

    Option("lockdep_force_backtrace", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("always gather current backtrace at every lock")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_see_also({"lockdep"}),

    Option("run_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path for the 'run' directory for storing pid and socket files")
    .set_default("/var/run/ceph")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_see_also({"admin_socket"}),

    Option("admin_socket", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path for the runtime control socket file, used by the 'ceph daemon' command")
    .set_daemon_default("$run_dir/$cluster-$name.asok")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common"),

    Option("admin_socket_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("file mode to set for the admin socket file, e.g, '0755'")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("common")
    .add_see_also({"admin_socket"}),

    Option("daemonize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to daemonize (background) after startup")
    .set_default(false)
    .set_daemon_default(true)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"pid_file", "chdir"}),

    Option("setuser", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("uid or user name to switch to on startup")
    .set_long_description("This is normally specified by the systemd unit file.")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"setgroup"}),

    Option("setgroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("gid or group name to switch to on startup")
    .set_long_description("This is normally specified by the systemd unit file.")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"setuser"}),

    Option("setuser_match_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("if set, setuser/setgroup is condition on this path matching ownership")
    .set_long_description("If setuser or setgroup are specified, and this option is non-empty, then the uid/gid of the daemon will only be changed if the file or directory specified by this option has a matching uid and/or gid.  This exists primarily to allow switching to user ceph for OSDs to be conditional on whether the osd data contents have also been chowned after an upgrade.  This is normally specified by the systemd unit file.")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"setuser", "setgroup"}),

    Option("pid_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to write a pid file (if any)")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service"),

    Option("chdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to chdir(2) to after daemonizing")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service")
    .add_see_also({"daemonize"}),

    Option("fatal_signal_handlers", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to register signal handlers for SIGABRT etc that dump a stack trace")
    .set_long_description("This is normally true for daemons and values for libraries.")
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_tag("service"),

    Option("crash_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Directory where crash reports are archived")
    .set_default("/var/lib/ceph/crash")
    .set_flag(Option::FLAG_STARTUP),

    Option("restapi_log_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default set by python code"),

    Option("restapi_base_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default set by python code"),

    Option("erasure_code_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("directory where erasure-code plugins can be found")
    .set_default("/erasure-code")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "osd"}),

    Option("log_file", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("path to log file")
    .set_daemon_default("/var/log/ceph/$cluster-$name.log")
    .add_see_also({"log_to_file", "log_to_stderr", "err_to_stderr", "log_to_syslog", "err_to_syslog"}),

    Option("log_max_new", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max unwritten log entries to allow before waiting to flush to the log")
    .set_default(1000)
    .add_see_also({"log_max_recent"}),

    Option("log_max_recent", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("recent log entries to keep in memory to dump in the event of a crash")
    .set_long_description("The purpose of this option is to log at a higher debug level only to the in-memory buffer, and write out the detailed log messages only if there is a crash.  Only log entries below the lower log level will be written unconditionally to the log.  For example, debug_osd=1/5 will write everything <= 1 to the log unconditionally but keep entries at levels 2-5 in memory.  If there is a seg fault or assertion failure, all entries will be dumped to the log.")
    .set_default(500)
    .set_daemon_default(10000)
    .set_min(1),

    Option("log_to_file", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send log lines to a file")
    .set_default(true)
    .add_see_also({"log_file"}),

    Option("log_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send log lines to stderr")
    .set_default(true)
    .set_daemon_default(false),

    Option("err_to_stderr", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send critical error log lines to stderr")
    .set_default(false)
    .set_daemon_default(true),

    Option("log_stderr_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("String to prefix log messages with when sent to stderr")
    .set_long_description("This is useful in container environments when combined with mon_cluster_log_to_stderr.  The mon log prefixes each line with the channel name (e.g., 'default', 'audit'), while log_stderr_prefix can be set to 'debug '.")
    .add_see_also({"mon_cluster_log_to_stderr"}),

    Option("log_to_syslog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send log lines to syslog facility")
    .set_default(false),

    Option("err_to_syslog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send critical error log lines to syslog facility")
    .set_default(false),

    Option("log_flush_on_exit", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set a process exit handler to ensure the log is flushed on exit")
    .set_default(false),

    Option("log_stop_at_utilization", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_description("stop writing to the log file when device utilization reaches this ratio")
    .set_default(0.97)
    .set_min_max(0.0, 1.0)
    .add_see_also({"log_file"}),

    Option("log_to_graylog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send log lines to remote graylog server")
    .set_default(false)
    .add_see_also({"err_to_graylog", "log_graylog_host", "log_graylog_port"}),

    Option("err_to_graylog", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send critical error log lines to remote graylog server")
    .set_default(false)
    .add_see_also({"log_to_graylog", "log_graylog_host", "log_graylog_port"}),

    Option("log_graylog_host", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("address or hostname of graylog server to log to")
    .set_default("127.0.0.1")
    .add_see_also({"log_to_graylog", "err_to_graylog", "log_graylog_port"}),

    Option("log_graylog_port", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("port number for the remote graylog server")
    .set_default(12201)
    .add_see_also({"log_graylog_host"}),

    Option("log_to_journald", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send log lines to journald")
    .set_default(false)
    .add_see_also({"err_to_journald"}),

    Option("err_to_journald", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("send critical error log lines to journald")
    .set_default(false)
    .add_see_also({"log_to_journald"}),

    Option("log_coarse_timestamps", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("timestamp log entries from coarse system clock to improve performance")
    .set_default(true)
    .add_service("common")
    .add_tag("performance")
    .add_tag("service"),

    Option("clog_to_monitors", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Make daemons send cluster log messages to monitors")
    .set_default("default=true")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mgr", "osd", "mds"}),

    Option("clog_to_syslog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Make daemons send cluster log messages to syslog")
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mon", "mgr", "osd", "mds"}),

    Option("clog_to_syslog_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Syslog level for cluster log messages")
    .set_default("info")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_see_also({"clog_to_syslog"}),

    Option("clog_to_syslog_facility", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Syslog facility for cluster log messages")
    .set_default("default=daemon audit=local0")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_see_also({"clog_to_syslog"}),

    Option("clog_to_graylog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Make daemons send cluster log to graylog")
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mon", "mgr", "osd", "mds"}),

    Option("clog_to_graylog_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Graylog host to cluster log messages")
    .set_default("127.0.0.1")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_see_also({"clog_to_graylog"}),

    Option("clog_to_graylog_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Graylog port number for cluster log messages")
    .set_default("12201")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mon", "mgr", "osd", "mds"})
    .add_see_also({"clog_to_graylog"}),

    Option("enable_experimental_unrecoverable_data_corrupting_features", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Enable named (or all with '*') experimental features that may be untested, dangerous, and/or cause permanent data loss")
    .set_flag(Option::FLAG_RUNTIME),

    Option("plugin_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Base directory for dynamically loaded plugins")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "osd"}),

    Option("compressor_zlib_isal", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use Intel ISA-L accelerated zlib implementation if available")
    .set_default(false),

    Option("compressor_zlib_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Zlib compression level to use")
    .set_default(5),

    Option("compressor_zlib_winsize", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Zlib compression winsize to use")
    .set_default(-15)
    .set_min_max(-15, 32),

    Option("compressor_zstd_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Zstd compression level to use")
    .set_default(1),

    Option("qat_compressor_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable Intel QAT acceleration support for compression if available")
    .set_default(false),

    Option("qat_compressor_session_max_number", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Set the maximum number of session within Qatzip when using QAT compressor")
    .set_default(256),

    Option("plugin_crypto_accelerator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Crypto accelerator library to use")
    .set_default("crypto_isal"),

    Option("openssl_engine_opts", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Use engine for specific openssl algorithm")
    .set_long_description("Pass opts in this way: engine_id=engine1,dynamic_path=/some/path/engine1.so,default_algorithms=DIGESTS:engine_id=engine2,dynamic_path=/some/path/engine2.so,default_algorithms=CIPHERS,other_ctrl=other_value")
    .set_flag(Option::FLAG_STARTUP),

    Option("mempool_debug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_NO_MON_UPDATE),

    Option("thp", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable transparent huge page (THP) support")
    .set_long_description("Ceph is known to suffer from memory fragmentation due to THP use. This is indicated by RSS usage above configured memory targets. Enabling THP is currently discouraged until selective use of THP by Ceph is implemented.")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP),

    Option("key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Authentication key")
    .set_long_description("A CephX authentication key, base64 encoded.  It normally looks something like 'AQAtut9ZdMbNJBAAHz6yBAWyJyz2yYRyeMWDag=='.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"keyfile", "keyring"}),

    Option("keyfile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to a file containing a key")
    .set_long_description("The file should contain a CephX authentication key and optionally a trailing newline, but nothing else.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"key"}),

    Option("keyring", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to a keyring file.")
    .set_long_description("A keyring file is an INI-style formatted file where the section names are client or daemon names (e.g., 'osd.0') and each section contains a 'key' property with CephX authentication key as the value.")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"key", "keyfile"}),

    Option("heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Frequency of internal heartbeat checks (seconds)")
    .set_default(5)
    .set_flag(Option::FLAG_STARTUP),

    Option("heartbeat_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("File to touch on successful internal heartbeat")
    .set_long_description("If set, this file will be touched every time an internal heartbeat check succeeds.")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"heartbeat_interval"}),

    Option("heartbeat_inject_failure", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("perf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable internal performance metrics")
    .set_long_description("If enabled, collect and expose internal health metrics")
    .set_default(true),

    Option("ms_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Messenger implementation to use for network communication")
    .set_default("async+posix")
    .set_flag(Option::FLAG_STARTUP),

    Option("ms_public_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Messenger implementation to use for the public network")
    .set_long_description("If not specified, use ms_type")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_type"}),

    Option("ms_cluster_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Messenger implementation to use for the internal cluster network")
    .set_long_description("If not specified, use ms_type")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_type"}),

    Option("ms_mon_cluster_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Connection modes (crc, secure) for intra-mon connections in order of preference")
    .set_default("secure crc")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_mon_service_mode", "ms_mon_client_mode", "ms_service_mode", "ms_cluster_mode", "ms_client_mode"}),

    Option("ms_mon_service_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Allowed connection modes (crc, secure) for connections to mons")
    .set_default("secure crc")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_service_mode", "ms_mon_cluster_mode", "ms_mon_client_mode", "ms_cluster_mode", "ms_client_mode"}),

    Option("ms_mon_client_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Connection modes (crc, secure) for connections from clients to monitors in order of preference")
    .set_default("secure crc")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_mon_service_mode", "ms_mon_cluster_mode", "ms_service_mode", "ms_cluster_mode", "ms_client_mode"}),

    Option("ms_cluster_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Connection modes (crc, secure) for intra-cluster connections in order of preference")
    .set_default("crc secure")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_service_mode", "ms_client_mode"}),

    Option("ms_service_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Allowed connection modes (crc, secure) for connections to daemons")
    .set_default("crc secure")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_cluster_mode", "ms_client_mode"}),

    Option("ms_client_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Connection modes (crc, secure) for connections from clients in order of preference")
    .set_default("crc secure")
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"ms_cluster_mode", "ms_service_mode"}),

    Option("ms_osd_compress_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Compression policy to use in Messenger for communicating with OSD")
    .set_default("none")
    .set_enum_allowed({"none", "force"})
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("osd")
    .add_see_also({"ms_compress_secure"}),

    Option("ms_osd_compress_min_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Minimal message size eligable for on-wire compression")
    .set_default(1_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("osd")
    .add_see_also({"ms_osd_compress_mode"}),

    Option("ms_osd_compression_algorithm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Compression algorithm to use in Messenger when communicating with OSD")
    .set_long_description("Compression algorithm for connections with OSD in order of preference Although the default value is set to snappy, a list (like snappy zlib zstd etc.) is acceptable as well.")
    .set_default("snappy")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("osd")
    .add_see_also({"ms_osd_compress_mode"}),

    Option("ms_compress_secure", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Allowing compression when on-wire encryption is enabled")
    .set_long_description("Combining encryption with compression reduces the level of security of messages between peers. In case both encryption and compression are enabled, compression setting will be ignored and message will not be compressed. This behaviour can be override using this setting.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"ms_osd_compress_mode"}),

    Option("ms_learn_addr_from_peer", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Learn address from what IP our first peer thinks we connect from")
    .set_long_description("Use the IP address our first peer (usually a monitor) sees that we are connecting from.  This is useful if a client is behind some sort of NAT and we want to see it identified by its local (not NATed) address.")
    .set_default(true),

    Option("ms_tcp_nodelay", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Disable Nagle's algorithm and send queued network traffic immediately")
    .set_default(true),

    Option("ms_tcp_rcvbuf", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Size of TCP socket receive buffer")
    .set_default(0),

    Option("ms_tcp_prefetch_max_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum amount of data to prefetch out of the socket receive buffer")
    .set_default(4_K),

    Option("ms_initial_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Initial backoff after a network error is detected (seconds)")
    .set_default(0.2),

    Option("ms_max_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Maximum backoff after a network error before retrying (seconds)")
    .set_default(15.0)
    .add_see_also({"ms_initial_backoff"}),

    Option("ms_crc_data", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Set and/or verify crc32c checksum on data payload sent over network")
    .set_default(true),

    Option("ms_crc_header", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Set and/or verify crc32c checksum on header payload sent over network")
    .set_default(true),

    Option("ms_die_on_bad_msg", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Induce a daemon crash/exit when a bad network message is received")
    .set_default(false),

    Option("ms_die_on_unhandled_msg", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Induce a daemon crash/exit when an unrecognized message is received")
    .set_default(false),

    Option("ms_die_on_old_message", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Induce a daemon crash/exit when a old, undecodable message is received")
    .set_default(false),

    Option("ms_die_on_skipped_message", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Induce a daemon crash/exit if sender skips a message sequence number")
    .set_default(false),

    Option("ms_die_on_bug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Induce a crash/exit on various bugs (for testing purposes)")
    .set_default(false),

    Option("ms_dispatch_throttle_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Limit messages that are read off the network but still being processed")
    .set_default(100_M),

    Option("ms_bind_ipv4", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Bind servers to IPv4 address(es)")
    .set_default(true)
    .add_see_also({"ms_bind_ipv6"}),

    Option("ms_bind_ipv6", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Bind servers to IPv6 address(es)")
    .set_default(false)
    .add_see_also({"ms_bind_ipv4"}),

    Option("ms_bind_prefer_ipv4", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Prefer IPV4 over IPV6 address(es)")
    .set_default(false),

    Option("ms_bind_msgr1", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Bind servers to msgr1 (legacy) protocol address(es)")
    .set_default(true)
    .add_see_also({"ms_bind_msgr2"}),

    Option("ms_bind_msgr2", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Bind servers to msgr2 (nautilus+) protocol address(es)")
    .set_default(true)
    .add_see_also({"ms_bind_msgr1"}),

    Option("ms_bind_port_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Lowest port number to bind daemon(s) to")
    .set_default(6800),

    Option("ms_bind_port_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Highest port number to bind daemon(s) to")
    .set_default(7568),

    Option("ms_bind_retry_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of attempts to make while bind(2)ing to a port"),

    Option("ms_bind_retry_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Delay between bind(2) attempts (seconds)"),

    Option("ms_bind_before_connect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Call bind(2) on client sockets")
    .set_default(false),

    Option("ms_tcp_listen_backlog", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Size of queue of incoming connections for accept(2)")
    .set_default(512),

    Option("ms_connection_ready_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Time before we declare a not yet ready connection as dead (seconds)")
    .set_default(10),

    Option("ms_connection_idle_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Time before an idle connection is closed (seconds)")
    .set_default(900),

    Option("ms_pq_max_tokens_per_priority", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(16_M),

    Option("ms_pq_min_cost", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K),

    Option("ms_inject_socket_failures", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Inject a socket failure every Nth socket operation")
    .set_default(0),

    Option("ms_inject_delay_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Entity type to inject delays for")
    .set_flag(Option::FLAG_RUNTIME),

    Option("ms_inject_delay_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Max delay to inject")
    .set_default(1.0),

    Option("ms_inject_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("ms_inject_internal_delays", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Inject various internal delays to induce races (seconds)")
    .set_default(0.0),

    Option("ms_inject_network_congestion", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Inject a network congestions that stuck with N times operations")
    .set_default(0),

    Option("ms_blackhole_osd", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("ms_blackhole_mon", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("ms_blackhole_mds", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("ms_blackhole_mgr", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("ms_blackhole_client", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("ms_dump_on_send", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Hexdump message to debug log on message send")
    .set_default(false),

    Option("ms_dump_corrupt_message_level", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Log level at which to hexdump corrupt messages we receive")
    .set_default(1),

    Option("ms_async_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Threadpool size for AsyncMessenger (ms_type=async)")
    .set_default(3)
    .set_min_max(1, 24),

    Option("ms_async_reap_threshold", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("number of deleted connections before we reap")
    .set_default(5)
    .set_min(1),

    Option("ms_async_rdma_device_name", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_async_rdma_enable_hugepage", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("ms_async_rdma_buffer_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(128_K),

    Option("ms_async_rdma_send_buffers", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_K),

    Option("ms_async_rdma_receive_buffers", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32_K),

    Option("ms_async_rdma_receive_queue_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4_K),

    Option("ms_async_rdma_support_srq", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("ms_async_rdma_port_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1),

    Option("ms_async_rdma_polling_us", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1000),

    Option("ms_async_rdma_gid_idx", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("use gid_idx to select GID for choosing RoCEv1 or RoCEv2")
    .set_default(0),

    Option("ms_async_rdma_local_gid", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_async_rdma_roce_ver", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1),

    Option("ms_async_rdma_sl", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3),

    Option("ms_async_rdma_dscp", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(96),

    Option("ms_max_accept_failures", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of consecutive failed accept() calls before considering the daemon is misconfigured and abort it.")
    .set_default(4),

    Option("ms_async_rdma_cm", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("ms_async_rdma_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("ib"),

    Option("ms_dpdk_port_id", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0),

    Option("ms_dpdk_coremask", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("0xF")
    .add_see_also({"ms_async_op_threads"}),

    Option("ms_dpdk_memory_channel", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("4"),

    Option("ms_dpdk_hugepages", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_dpdk_pmd", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_dpdk_devs_allowlist", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("NIC's PCIe address are allowed to use")
    .set_long_description("for a single NIC use ms_dpdk_devs_allowlist=-a 0000:7d:010 or --allow=0000:7d:010; for a bond nics use ms_dpdk_devs_allowlist=--allow=0000:7d:01.0 --allow=0000:7d:02.6 --vdev=net_bonding0,mode=2,slave=0000:7d:01.0,slave=0000:7d:02.6."),

    Option("ms_dpdk_host_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_dpdk_gateway_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_dpdk_netmask_ipv4_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("ms_dpdk_lro", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("ms_dpdk_enable_tso", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("ms_dpdk_hw_flow_control", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("ms_dpdk_hw_queue_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0),

    Option("ms_dpdk_debug_allow_loopback", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("ms_dpdk_rx_buffer_count_per_core", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8192),

    Option("inject_early_sigterm", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("send ourselves a SIGTERM early during startup")
    .set_default(false),

    Option("mon_initial_members", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service("mon"),

    Option("mon_max_pg_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max number of PGs per OSD the cluster will allow")
    .set_long_description("If the number of PGs per OSD exceeds this, a health warning will be visible in `ceph status`.  This is also used in automated PG management, as the threshold at which some pools' pg_num may be shrunk in order to enable increasing the pg_num of others.")
    .set_default(250)
    .set_min(1)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service({"mgr", "mon"}),

    Option("mon_osd_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("full ratio of OSDs to be set during initial creation of the cluster")
    .set_default(0.95)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE),

    Option("mon_osd_backfillfull_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.9)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE),

    Option("mon_osd_nearfull_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("nearfull ratio for OSDs to be set during initial creation of cluster")
    .set_default(0.85)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE),

    Option("mon_osd_initial_require_min_compat_client", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("luminous")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE),

    Option("mon_allow_pool_delete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("allow pool deletions")
    .set_default(false)
    .add_service("mon"),

    Option("mon_fake_pool_delete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("fake pool deletions by renaming the rados pool")
    .set_default(false)
    .add_service("mon"),

    Option("mon_globalid_prealloc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of globalid values to preallocate")
    .set_long_description("This setting caps how many new clients can authenticate with the cluster before the monitors have to perform a write to preallocate more.  Large values burn through the 64-bit ID space more quickly.")
    .set_default(10000)
    .add_service("mon"),

    Option("mon_osd_report_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("time before OSDs who do not report to the mons are marked down (seconds)")
    .set_default(15_min)
    .add_service("mon"),

    Option("mon_warn_on_insecure_global_id_reclaim", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue AUTH_INSECURE_GLOBAL_ID_RECLAIM health warning if any connected clients are insecurely reclaiming global_id")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_warn_on_insecure_global_id_reclaim_allowed", "auth_allow_insecure_global_id_reclaim", "auth_expose_insecure_global_id_reclaim"}),

    Option("mon_warn_on_insecure_global_id_reclaim_allowed", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED health warning if insecure global_id reclaim is allowed")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_warn_on_insecure_global_id_reclaim", "auth_allow_insecure_global_id_reclaim", "auth_expose_insecure_global_id_reclaim"}),

    Option("mon_warn_on_msgr2_not_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue MON_MSGR2_NOT_ENABLED health warning if monitors are all running Nautilus but not all binding to a msgr2 port")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"ms_bind_msgr2"}),

    Option("mon_warn_on_slow_ping_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Override mon_warn_on_slow_ping_ratio with specified threshold in milliseconds")
    .set_default(0.0)
    .add_service({"mgr", "osd"})
    .add_see_also({"mon_warn_on_slow_ping_ratio"}),

    Option("mon_warn_on_slow_ping_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Issue a health warning if heartbeat ping longer than percentage of osd_heartbeat_grace")
    .set_default(0.05)
    .add_service({"mgr", "osd"})
    .add_see_also({"osd_heartbeat_grace", "mon_warn_on_slow_ping_time"}),

    Option("mon_max_snap_prune_per_epoch", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("max number of pruned snaps we will process in a single OSDMap epoch")
    .set_default(100)
    .add_service("mon"),

    Option("mon_min_osdmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("min number of OSDMaps to store")
    .set_default(500)
    .add_service("mon"),

    Option("mon_max_log_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max number of past cluster log epochs to store")
    .set_default(500)
    .add_service("mon"),

    Option("mon_max_mdsmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max number of FSMaps/MDSMaps to store")
    .set_default(500)
    .add_service("mon"),

    Option("mon_max_mgrmap_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max number of MgrMaps to store")
    .set_default(500)
    .add_service("mon"),

    Option("mon_max_osd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max number of OSDs in a cluster")
    .set_default(10000)
    .add_service("mon"),

    Option("mon_probe_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("timeout for querying other mons during bootstrap pre-election phase (seconds)")
    .set_default(2.0)
    .add_service("mon"),

    Option("mon_client_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("max bytes of outstanding client messages mon will read off the network")
    .set_default(100_M)
    .add_service("mon"),

    Option("mon_warn_pg_not_scrubbed_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Percentage of the scrub max interval past the scrub max interval to warn")
    .set_default(0.5)
    .set_min(0.0)
    .add_see_also({"osd_scrub_max_interval"}),

    Option("mon_warn_pg_not_deep_scrubbed_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Percentage of the deep scrub interval past the deep scrub interval to warn")
    .set_default(0.75)
    .set_min(0.0)
    .add_see_also({"osd_deep_scrub_interval"}),

    Option("mon_scrub_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("frequency for scrubbing mon database")
    .set_default(1_day)
    .add_service("mon"),

    Option("mon_scrub_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("timeout to restart scrub of mon quorum participant does not respond for the latest chunk")
    .set_default(5_min)
    .add_service("mon"),

    Option("mon_scrub_max_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max keys per on scrub chunk/step")
    .set_default(100)
    .add_service("mon"),

    Option("mon_scrub_inject_crc_mismatch", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("probability for injecting crc mismatches into mon scrub")
    .set_default(0.0)
    .add_service("mon"),

    Option("mon_scrub_inject_missing_keys", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("probability for injecting missing keys into mon scrub")
    .set_default(0.0)
    .add_service("mon"),

    Option("mon_config_key_max_entry_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Defines the number of bytes allowed to be held in a single config-key entry")
    .set_default(64_K)
    .add_service("mon"),

    Option("mon_sync_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("timeout before canceling sync if syncing mon does not respond")
    .set_default(1_min)
    .add_service("mon"),

    Option("mon_sync_max_payload_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("target max message payload for mon sync")
    .set_default(1_M)
    .add_service("mon"),

    Option("mon_sync_max_payload_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("target max keys in message payload for mon sync")
    .set_default(2000)
    .add_service("mon"),

    Option("mon_sync_debug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable extra debugging during mon sync")
    .set_default(false)
    .add_service("mon"),

    Option("mon_inject_sync_get_chunk_delay", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("inject delay during sync (seconds)")
    .set_default(0.0)
    .add_service("mon"),

    Option("mon_osd_min_down_reporters", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of OSDs from different subtrees who need to report a down OSD for it to count")
    .set_default(2)
    .add_service("mon")
    .add_see_also({"mon_osd_reporter_subtree_level"}),

    Option("mon_osd_reporter_subtree_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("in which level of parent bucket the reporters are counted")
    .set_default("host")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_osd_snap_trim_queue_warn_on", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Warn when snap trim queue is that large (or larger).")
    .set_long_description("Warn when snap trim queue length for at least one PG crosses this value, as this is indicator of snap trimmer not keeping up, wasting disk space")
    .set_default(32768)
    .add_service("mon"),

    Option("mon_osd_force_trim_to", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("force mons to trim osdmaps through this epoch")
    .set_default(0)
    .add_service("mon"),

    Option("mon_debug_extra_checks", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Enable some additional monitor checks")
    .set_long_description("Enable some additional monitor checks that would be too expensive to run on production systems, or would only be relevant while testing or debugging.")
    .set_default(false)
    .add_service("mon"),

    Option("mon_debug_block_osdmap_trim", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Block OSDMap trimming while the option is enabled.")
    .set_long_description("Blocking OSDMap trimming may be quite helpful to easily reproduce states in which the monitor keeps (hundreds of) thousands of osdmaps.")
    .set_default(false)
    .add_service("mon"),

    Option("mon_debug_deprecated_as_obsolete", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("treat deprecated mon commands as obsolete")
    .set_default(false)
    .add_service("mon"),

    Option("mon_debug_dump_transactions", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("dump paxos transactions to log")
    .set_default(false)
    .add_service("mon")
    .add_see_also({"mon_debug_dump_location"}),

    Option("mon_debug_dump_json", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("dump paxos transasctions to log as json")
    .set_default(false)
    .add_service("mon")
    .add_see_also({"mon_debug_dump_transactions"}),

    Option("mon_debug_dump_location", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("file to dump paxos transactions to")
    .set_default("/var/log/ceph/$cluster-$name.tdump")
    .add_service("mon")
    .add_see_also({"mon_debug_dump_transactions"}),

    Option("mon_debug_no_require_quincy", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("do not set quincy feature for new mon clusters")
    .set_default(false)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service("mon"),

    Option("mon_debug_no_require_reef", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("do not set reef feature for new mon clusters")
    .set_default(false)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service("mon"),

    Option("mon_debug_no_require_bluestore_for_ec_overwrites", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("do not require bluestore OSDs to enable EC overwrites on a rados pool")
    .set_default(false)
    .add_service("mon"),

    Option("mon_debug_no_initial_persistent_features", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("do not set any monmap features for new mon clusters")
    .set_default(false)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service("mon"),

    Option("mon_inject_transaction_delay_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("max duration of injected delay in paxos")
    .set_default(10.0)
    .add_service("mon"),

    Option("mon_inject_transaction_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("probability of injecting a delay in paxos")
    .set_default(0.0)
    .add_service("mon"),

    Option("mon_inject_pg_merge_bounce_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("probability of failing and reverting a pg_num decrement")
    .set_default(0.0)
    .add_service("mon"),

    Option("mon_sync_provider_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("kill mon sync requester at specific point")
    .set_default(0)
    .add_service("mon"),

    Option("mon_sync_requester_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("kill mon sync requestor at specific point")
    .set_default(0)
    .add_service("mon"),

    Option("mon_force_quorum_join", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("force mon to rejoin quorum even though it was just removed")
    .set_default(false)
    .add_service("mon"),

    Option("mon_keyvaluedb", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("database backend to use for the mon database")
    .set_default("rocksdb")
    .set_enum_allowed({"rocksdb"})
    .set_flag(Option::FLAG_CREATE)
    .add_service("mon"),

    Option("mon_debug_unsafe_allow_tier_with_nonempty_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mon"),

    Option("auth_cluster_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("authentication methods required by the cluster")
    .set_default("cephx"),

    Option("auth_service_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("authentication methods required by service daemons")
    .set_default("cephx"),

    Option("auth_client_required", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("authentication methods allowed by clients")
    .set_default("cephx, none"),

    Option("auth_supported", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("authentication methods required (deprecated)"),

    Option("max_rotating_auth_attempts", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("number of attempts to initialize rotating keys before giving up")
    .set_default(10),

    Option("rotating_keys_bootstrap_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("timeout for obtaining rotating keys during bootstrap phase (seconds)")
    .set_default(30),

    Option("rotating_keys_renewal_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("timeout for updating rotating keys (seconds)")
    .set_default(10),

    Option("cephx_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("cephx_require_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Cephx version required (1 = pre-mimic, 2 = mimic+)")
    .set_default(2),

    Option("cephx_cluster_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("cephx_cluster_require_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Cephx version required by the cluster from clients (1 = pre-mimic, 2 = mimic+)")
    .set_default(2),

    Option("cephx_service_require_signatures", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("cephx_service_require_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Cephx version required from ceph services (1 = pre-mimic, 2 = mimic+)")
    .set_default(2),

    Option("cephx_sign_messages", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("auth_mon_ticket_ttl", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(72_hr),

    Option("auth_service_ticket_ttl", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1_hr),

    Option("auth_allow_insecure_global_id_reclaim", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Allow reclaiming global_id without presenting a valid ticket proving previous possession of that global_id")
    .set_long_description("Allowing unauthorized global_id (re)use poses a security risk. Unfortunately, older clients may omit their ticket on reconnects and therefore rely on this being allowed for preserving their global_id for the lifetime of the client instance. Setting this value to false would immediately prevent new connections from those clients (assuming auth_expose_insecure_global_id_reclaim set to true) and eventually break existing sessions as well (regardless of auth_expose_insecure_global_id_reclaim setting).")
    .set_default(true)
    .add_see_also({"mon_warn_on_insecure_global_id_reclaim", "mon_warn_on_insecure_global_id_reclaim_allowed", "auth_expose_insecure_global_id_reclaim"}),

    Option("auth_expose_insecure_global_id_reclaim", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Force older clients that may omit their ticket on reconnects to reconnect as part of establishing a session")
    .set_long_description("In permissive mode (auth_allow_insecure_global_id_reclaim set to true), this helps with identifying clients that are not patched. In enforcing mode (auth_allow_insecure_global_id_reclaim set to false), this is a fail-fast mechanism: don't establish a session that will almost inevitably be broken later.")
    .set_default(true)
    .add_see_also({"mon_warn_on_insecure_global_id_reclaim", "mon_warn_on_insecure_global_id_reclaim_allowed", "auth_allow_insecure_global_id_reclaim"}),

    Option("auth_debug", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("mon_client_hunt_parallel", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(3),

    Option("mon_client_target_rank", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(-1)
    .set_min(-1)
    .set_flag(Option::FLAG_RUNTIME),

    Option("mon_client_hunt_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(3.0),

    Option("mon_client_log_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("How frequently we send queued cluster log messages to mon")
    .set_default(1.0),

    Option("mon_client_ping_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0),

    Option("mon_client_ping_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0),

    Option("mon_client_hunt_interval_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.5),

    Option("mon_client_hunt_interval_min_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0),

    Option("mon_client_hunt_interval_max_multiple", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0),

    Option("mon_client_max_log_entries_per_message", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000),

    Option("mon_client_directed_command_retry", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Number of times to try sending a command directed at a specific monitor")
    .set_default(2),

    Option("crush_location", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("crush_location_hook", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("crush_location_hook_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10),

    Option("objecter_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(5.0),

    Option("objecter_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Seconds before in-flight op is considered 'laggy' and we query mon for the latest OSDMap")
    .set_default(10.0),

    Option("objecter_inflight_op_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Max in-flight data in bytes (both directions)")
    .set_default(100_M),

    Option("objecter_inflight_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max in-flight operations")
    .set_default(1_K),

    Option("objecter_completion_locks_per_session", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(32),

    Option("objecter_inject_no_watch_ping", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("objecter_retry_writes_after_first_reply", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("objecter_debug_inject_relock_delay", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filer_max_purge_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max in-flight operations for purging a striped range (e.g., MDS journal)")
    .set_default(10),

    Option("filer_max_truncate_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max in-flight operations for truncating/deleting a striped sequence (e.g., MDS journal)")
    .set_default(128),

    Option("journaler_write_head_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Interval in seconds between journal header updates (to help bound replay time)")
    .set_default(15),

    Option("journaler_prefetch_periods", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of striping periods to prefetch while reading MDS journal")
    .set_default(10)
    .set_min(2),

    Option("journaler_prezero_periods", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of striping periods to zero head of MDS journal write position")
    .set_default(5)
    .set_min(2),

    Option("osd_calc_pg_upmaps_aggressively", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("try to calculate PG upmaps more aggressively, e.g., by doing a fairly exhaustive search of existing PGs that can be unmapped or upmapped")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_calc_pg_upmaps_aggressively_fast", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Prevent very long (>10 minutes) calculations in some extreme cases (applicable only to aggressive mode)")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_calc_pg_upmaps_local_fallback_retries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of PGs we can attempt to unmap or upmap for a specific overfull or underfull osd per iteration ")
    .set_default(100)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_crush_chooseleaf_type", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("default chooseleaf type for osdmaptool --create")
    .set_default(1)
    .set_flag(Option::FLAG_CLUSTER_CREATE),

    Option("osd_pool_use_gmt_hitset", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("use UTC for hitset timestamps")
    .set_long_description("This setting only exists for compatibility with hammer (and older) clusters.")
    .set_default(true),

    Option("osd_pool_default_ec_fast_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set ec_fast_read for new erasure-coded pools")
    .set_default(false)
    .add_service("mon"),

    Option("osd_pool_default_crush_rule", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("CRUSH rule for newly created pools")
    .set_default(-1)
    .add_service("mon"),

    Option("osd_pool_default_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the number of copies of an object for new replicated pools")
    .set_default(3)
    .set_min_max(0, 10)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("osd_pool_default_min_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the minimal number of copies allowed to write to a degraded pool for new replicated pools")
    .set_long_description("0 means no specific default; ceph will use size-size/2")
    .set_default(0)
    .set_min_max(0, 255)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"osd_pool_default_size"}),

    Option("osd_pool_default_pg_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of PGs for new pools")
    .set_long_description("With default value of `osd_pool_default_pg_autoscale_mode` being `on` the number of PGs for new pools will start out with 1 pg, unless the user specifies the pg_num.")
    .set_default(32)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"osd_pool_default_pg_autoscale_mode"}),

    Option("osd_pool_default_pgp_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of PGs for placement purposes (0 to match pg_num)")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"osd_pool_default_pg_num", "osd_pool_default_pg_autoscale_mode"}),

    Option("osd_pool_default_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default type of pool to create")
    .set_default("replicated")
    .set_enum_allowed({"replicated", "erasure"})
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("osd_pool_default_erasure_code_profile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default erasure code profile for new erasure-coded pools")
    .set_default("plugin=jerasure technique=reed_sol_van k=2 m=2")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("osd_erasure_code_plugins", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("erasure code plugins to load")
    .set_flag(Option::FLAG_STARTUP)
    .add_service({"mon", "osd"}),

    Option("osd_pool_default_flags", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("(integer) flags to set on new pools")
    .set_default(0)
    .add_service("mon"),

    Option("osd_pool_default_flag_hashpspool", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set hashpspool (better hashing scheme) flag on new pools")
    .set_default(true)
    .add_service("mon"),

    Option("osd_pool_default_flag_nodelete", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set nodelete flag on new pools")
    .set_default(false)
    .add_service("mon"),

    Option("osd_pool_default_flag_nopgchange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set nopgchange flag on new pools")
    .set_default(false)
    .add_service("mon"),

    Option("osd_pool_default_flag_nosizechange", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set nosizechange flag on new pools")
    .set_default(false)
    .add_service("mon"),

    Option("osd_pool_default_flag_bulk", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set bulk flag on new pools")
    .set_default(false)
    .add_service("mon"),

    Option("osd_pool_default_hit_set_bloom_fpp", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .add_service("mon")
    .add_see_also({"osd_tier_default_cache_hit_set_type"}),

    Option("osd_pool_default_cache_target_dirty_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.4),

    Option("osd_pool_default_cache_target_dirty_high_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.6),

    Option("osd_pool_default_cache_target_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.8),

    Option("osd_pool_default_cache_min_flush_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0),

    Option("osd_pool_default_cache_min_evict_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0),

    Option("osd_pool_default_cache_max_evict_check_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10),

    Option("osd_pool_default_pg_autoscale_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default PG autoscaling behavior for new pools")
    .set_long_description("With default value `on`, the autoscaler starts a new pool with 1 pg, unless the user specifies the pg_num.")
    .set_default("on")
    .set_enum_allowed({"off", "warn", "on"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_pool_default_read_lease_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Default read_lease_ratio for a pool, as a multiple of osd_heartbeat_grace")
    .set_long_description("This should be <= 1.0 so that the read lease will have expired by the time we decide to mark a peer OSD down.")
    .set_default(0.8)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_heartbeat_grace"}),

    Option("osd_hit_set_min_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1000),

    Option("osd_hit_set_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(100000),

    Option("osd_hit_set_namespace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default(".ceph-internal"),

    Option("osd_tier_promote_max_objects_sec", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(25),

    Option("osd_tier_promote_max_bytes_sec", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(5_M),

    Option("osd_tier_default_cache_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("writeback")
    .set_enum_allowed({"none", "writeback", "forward", "readonly", "readforward", "readproxy", "proxy"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_tier_default_cache_hit_set_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4),

    Option("osd_tier_default_cache_hit_set_period", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1200),

    Option("osd_tier_default_cache_hit_set_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("bloom")
    .set_enum_allowed({"bloom", "explicit_hash", "explicit_object"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_tier_default_cache_min_read_recency_for_promote", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of recent HitSets the object must appear in to be promoted (on read)")
    .set_default(1),

    Option("osd_tier_default_cache_min_write_recency_for_promote", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of recent HitSets the object must appear in to be promoted (on write)")
    .set_default(1),

    Option("osd_tier_default_cache_hit_set_grade_decay_rate", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20),

    Option("osd_tier_default_cache_hit_set_search_last_n", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1),

    Option("osd_objecter_finishers", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_map_dedup", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_map_message_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of OSDMaps to include in a single message")
    .set_default(40)
    .add_service({"osd", "mon"}),

    Option("osd_map_message_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum number of bytes worth of OSDMaps to include in a single message")
    .set_default(10_M)
    .add_service({"osd", "mon"}),

    Option("osd_ignore_stale_divergent_priors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_heartbeat_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Interval (in seconds) between peer pings")
    .set_default(6)
    .set_min_max(1ULL, 1_min),

    Option("osd_heartbeat_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(20),

    Option("osd_heartbeat_stale", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Interval (in seconds) we mark an unresponsive heartbeat peer as stale.")
    .set_long_description("Automatically mark unresponsive heartbeat sessions as stale and tear them down. The primary benefit is that OSD doesn't need to keep a flood of blocked heartbeat messages around in memory.")
    .set_default(10_min),

    Option("osd_heartbeat_use_min_delay_socket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_heartbeat_min_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Minimum heartbeat packet size in bytes. Will add dummy payload if heartbeat packet is smaller than this.")
    .set_default(2000),

    Option("osd_pg_max_concurrent_snap_trims", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_min(1),

    Option("osd_max_trimming_pgs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2),

    Option("osd_heartbeat_min_healthy_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.33),

    Option("osd_mon_heartbeat_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30),

    Option("osd_mon_heartbeat_stat_stale", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Stop reporting on heartbeat ping times not updated for this many seconds.")
    .set_long_description("Stop reporting on old heartbeat information unless this is set to zero")
    .set_default(1_hr),

    Option("osd_mon_report_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Frequency of OSD reports to mon for peer failures, fullness status changes")
    .set_default(5),

    Option("osd_mon_report_max_in_flight", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2),

    Option("osd_beacon_report_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5_min),

    Option("osd_pg_stat_report_interval_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500),

    Option("osd_max_snap_prune_intervals_per_epoch", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Max number of snap intervals to report to mgr in pg_stat_t")
    .set_default(512),

    Option("osd_default_data_pool_replay_window", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(45),

    Option("osd_auto_mark_unfound_lost", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_check_for_log_corruption", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_use_stale_snap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_rollback_to_cluster_snap", Option::TYPE_STR, Option::LEVEL_ADVANCED),

    Option("osd_default_notify_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("default number of seconds after which notify propagation times out. used if a client has not specified other value")
    .set_default(30),

    Option("osd_kill_backfill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("osd_pg_epoch_persisted_max_stale", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(40),

    Option("osd_target_pg_log_entries_per_osd", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("target number of PG entries total on an OSD - limited per pg by the min and max options below")
    .set_default(300000)
    .add_see_also({"osd_max_pg_log_entries", "osd_min_pg_log_entries"}),

    Option("osd_min_pg_log_entries", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("minimum number of entries to maintain in the PG log")
    .set_default(250)
    .add_service("osd")
    .add_see_also({"osd_max_pg_log_entries", "osd_pg_log_dups_tracked", "osd_target_pg_log_entries_per_osd"}),

    Option("osd_max_pg_log_entries", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("maximum number of entries to maintain in the PG log")
    .set_default(10000)
    .add_service("osd")
    .add_see_also({"osd_min_pg_log_entries", "osd_pg_log_dups_tracked", "osd_target_pg_log_entries_per_osd"}),

    Option("osd_pg_log_dups_tracked", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("how many versions back to track in order to detect duplicate ops; this is combined with both the regular pg log entries and additional minimal dup detection entries")
    .set_default(3000)
    .add_service("osd")
    .add_see_also({"osd_min_pg_log_entries", "osd_max_pg_log_entries"}),

    Option("osd_object_clean_region_max_num_intervals", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("number of intervals in clean_offsets")
    .set_long_description("partial recovery uses multiple intervals to record the clean part of the objectwhen the number of intervals is greater than osd_object_clean_region_max_num_intervals, minimum interval will be trimmed(0 will recovery the entire object data interval)")
    .set_default(10)
    .add_service("osd"),

    Option("osd_force_recovery_pg_log_entries_factor", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.3),

    Option("osd_pg_log_trim_min", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Minimum number of log entries to trim at once. This lets us trim in larger batches rather than with each write.")
    .set_default(100)
    .add_see_also({"osd_max_pg_log_entries", "osd_min_pg_log_entries"}),

    Option("osd_force_auth_primary_missing_objects", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Approximate missing objects above which to force auth_log_shard to be primary temporarily")
    .set_default(100),

    Option("osd_async_recovery_min_cost", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("A mixture measure of number of current log entries difference and historical missing objects,  above which we switch to use asynchronous recovery when appropriate")
    .set_default(100)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_max_pg_per_osd_hard_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of PG per OSD, a factor of 'mon_max_pg_per_osd'")
    .set_long_description("OSD will refuse to instantiate PG if the number of PG it serves exceeds this number.")
    .set_default(3.0)
    .set_min(1.0)
    .add_see_also({"mon_max_pg_per_osd"}),

    Option("osd_pg_log_trim_max", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of entries to remove at once from the PG log")
    .set_default(10000)
    .add_service("osd")
    .add_see_also({"osd_min_pg_log_entries", "osd_max_pg_log_entries"}),

    Option("osd_op_complaint_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(30.0),

    Option("osd_command_max_records", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(256),

    Option("osd_max_pg_blocked_by", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16),

    Option("osd_op_log_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5),

    Option("osd_backoff_on_unfound", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_backoff_on_degraded", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_backoff_on_peering", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_debug_shutdown", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Turn up debug levels during shutdown")
    .set_default(false),

    Option("osd_debug_crash_on_ignored_backoff", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_inject_dispatch_delay_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("osd_debug_inject_dispatch_delay_duration", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.1),

    Option("osd_debug_drop_ping_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("N/A")
    .set_default(0.0),

    Option("osd_debug_drop_ping_duration", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("N/A")
    .set_default(0),

    Option("osd_debug_op_order", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_verify_missing_on_start", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_verify_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_verify_stray_on_activate", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_skip_full_check_in_backfill_reservation", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_reject_backfill_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("osd_debug_inject_copyfrom_error", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_misdirected_ops", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_skip_full_check_in_recovery", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_random_push_read_error", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("osd_debug_verify_cached_snaps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_deep_scrub_sleep", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Inject an expensive sleep during deep scrub IO to make it easier to induce preemption")
    .set_default(0.0),

    Option("osd_debug_no_acting_change", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_no_purge_strays", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_debug_pretend_recovery_active", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_num_op_tracker_shard", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(32),

    Option("osd_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20),

    Option("osd_op_history_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(600),

    Option("osd_op_history_slow_op_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(20),

    Option("osd_op_history_slow_op_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(10.0),

    Option("osd_target_transaction_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(30),

    Option("osd_failsafe_full_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.97),

    Option("osd_fast_shutdown", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Fast, immediate shutdown")
    .set_long_description("Setting this to false makes the OSD do a slower teardown of all state when it receives a SIGINT or SIGTERM or when shutting down for any other reason.  That slow shutdown is primarilyy useful for doing memory leak checking with valgrind.")
    .set_default(true),

    Option("osd_fast_shutdown_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("timeout in seconds for osd fast-shutdown (0 is unlimited)")
    .set_default(15)
    .set_min(0),

    Option("osd_fast_shutdown_notify_mon", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Tell mon about OSD shutdown on immediate shutdown")
    .set_long_description("Tell the monitor the OSD is shutting down on immediate shutdown. This helps with cluster log messages from other OSDs reporting it immediately failed.")
    .set_default(true)
    .add_see_also({"osd_fast_shutdown", "osd_mon_shutdown_timeout"}),

    Option("osd_fast_fail_on_connection_refused", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_pg_object_context_cache_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64),

    Option("osd_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_function_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_fast_info", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_debug_pg_log_writeout", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_loop_before_reset_tphandle", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64),

    Option("threadpool_default_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1_min),

    Option("threadpool_empty_queue_max_wait", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2),

    Option("rocksdb_log_to_ceph_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("rocksdb_cache_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_M)
    .set_flag(Option::FLAG_RUNTIME),

    Option("rocksdb_cache_row_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0),

    Option("rocksdb_cache_shard_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4),

    Option("rocksdb_cache_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("binned_lru"),

    Option("rocksdb_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K),

    Option("rocksdb_perf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("rocksdb_collect_compaction_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("rocksdb_collect_extended_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("rocksdb_collect_memory_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("rocksdb_delete_range_threshold", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The number of keys required to invoke DeleteRange when deleting muliple keys.")
    .set_default(1_M),

    Option("rocksdb_bloom_bits_per_key", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of bits per key to use for RocksDB's bloom filters.")
    .set_long_description("RocksDB bloom filters can be used to quickly answer the question of whether or not a key may exist or definitely does not exist in a given RocksDB SST file without having to read all keys into memory.  Using a higher bit value decreases the likelihood of false positives at the expense of additional disk space and memory consumption when the filter is loaded into RAM.  The current default value of 20 was found to provide significant performance gains when getattr calls are made (such as during new object creation in bluestore) without significant memory overhead or cache pollution when combined with rocksdb partitioned index filters.  See: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters for more information.")
    .set_default(20),

    Option("rocksdb_cache_index_and_filter_blocks", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Whether to cache indices and filters in block cache")
    .set_long_description("By default RocksDB will load an SST file's index and bloom filters into memory when it is opened and remove them from memory when an SST file is closed.  Thus, memory consumption by indices and bloom filters is directly tied to the number of concurrent SST files allowed to be kept open.  This option instead stores cached indicies and filters in the block cache where they directly compete with other cached data.  By default we set this option to true to better account for and bound rocksdb memory usage and keep filters in memory even when an SST file is closed.")
    .set_default(true),

    Option("rocksdb_cache_index_and_filter_blocks_with_high_priority", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Whether to cache indices and filters in the block cache with high priority")
    .set_long_description("A downside of setting rocksdb_cache_index_and_filter_blocks to true is that regular data can push indices and filters out of memory.  Setting this option to true means they are cached with higher priority than other data and should typically stay in the block cache.")
    .set_default(false),

    Option("rocksdb_pin_l0_filter_and_index_blocks_in_cache", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Whether to pin Level 0 indices and bloom filters in the block cache")
    .set_long_description("A downside of setting rocksdb_cache_index_and_filter_blocks to true is that regular data can push indices and filters out of memory.  Setting this option to true means that level 0 SST files will always have their indices and filters pinned in the block cache.")
    .set_default(false),

    Option("rocksdb_index_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Type of index for SST files: binary_search, hash_search, two_level")
    .set_long_description("This option controls the table index type.  binary_search is a space efficient index block that is optimized for block-search-based index. hash_search may improve prefix lookup performance at the expense of higher disk and memory usage and potentially slower compactions.  two_level is an experimental index type that uses two binary search indexes and works in conjunction with partition filters.  See: http://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html")
    .set_default("binary_search"),

    Option("rocksdb_partition_filters", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("(experimental) partition SST index/filters into smaller blocks")
    .set_long_description("This is an experimental option for rocksdb that works in conjunction with two_level indices to avoid having to keep the entire filter/index in cache when cache_index_and_filter_blocks is true.  The idea is to keep a much smaller top-level index in heap/cache and then opportunistically cache the lower level indices.  See: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters")
    .set_default(false),

    Option("rocksdb_metadata_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("The block size for index partitions. (0 = rocksdb default)")
    .set_default(4_K),

    Option("rocksdb_cf_compact_on_deletion", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Compact the column family when a certain number of tombstones are observed within a given window.")
    .set_long_description("This setting instructs RocksDB to compact a column family when a certain number of tombstones are observed during iteration within a certain sliding window. For instance if rocksdb_cf_compact_on_deletion_sliding_window is 8192 and rocksdb_cf_compact_on_deletion_trigger is 4096,  then once 4096 tombstones are observed after iteration over 8192 entries, the column family will be compacted.")
    .set_default(true)
    .add_see_also({"rocksdb_cf_compact_on_deletion_sliding_window", "rocksdb_cf_compact_on_deletion_trigger"}),

    Option("rocksdb_cf_compact_on_deletion_sliding_window", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("The sliding window to use when rocksdb_cf_compact_on_deletion is enabled.")
    .set_default(32768)
    .add_see_also({"rocksdb_cf_compact_on_deletion"}),

    Option("rocksdb_cf_compact_on_deletion_trigger", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("The trigger to use when rocksdb_cf_compact_on_deletion is enabled.")
    .set_default(16384)
    .add_see_also({"rocksdb_cf_compact_on_deletion"}),

    Option("osd_client_op_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(63),

    Option("osd_recovery_op_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Priority to use for recovery operations if not specified for the pool")
    .set_default(3),

    Option("osd_peering_op_priority", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(255),

    Option("osd_snap_trim_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5),

    Option("osd_snap_trim_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M),

    Option("osd_pg_delete_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(5),

    Option("osd_pg_delete_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M),

    Option("osd_scrub_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Priority for scrub operations in work queue")
    .set_default(5),

    Option("osd_scrub_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Cost for scrub operations in work queue")
    .set_default(50_M),

    Option("osd_scrub_event_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Cost for each scrub operation, used when osd_op_queue=mclock_scheduler")
    .set_default(4_K),

    Option("osd_requested_scrub_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(120),

    Option("osd_recovery_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Priority of recovery in the work queue")
    .set_long_description("Not related to a pool's recovery_priority")
    .set_default(5),

    Option("osd_recovery_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(20_M),

    Option("osd_recovery_op_warn_multiple", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16),

    Option("osd_mon_shutdown_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0),

    Option("osd_shutdown_pgref_assert", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_max_object_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(128_M),

    Option("osd_max_object_name_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(2_K),

    Option("osd_max_object_namespace_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(256),

    Option("osd_max_attr_name_len", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100),

    Option("osd_max_attr_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(0),

    Option("osd_max_omap_entries_per_request", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_K),

    Option("osd_max_omap_bytes_per_request", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G),

    Option("osd_max_write_op_reply_len", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Max size of the per-op payload for requests with the RETURNVEC flag set")
    .set_long_description("This value caps the amount of data (per op; a request may have many ops) that will be sent back to the client and recorded in the PG log.")
    .set_default(64),

    Option("osd_objectstore", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("backend type for an OSD (like filestore or bluestore)")
    .set_default("bluestore")
    .set_enum_allowed({"bluestore", "filestore", "memstore", "kstore", "seastore", "cyanstore"})
    .set_flag(Option::FLAG_CREATE),

    Option("osd_objectstore_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_objectstore_fuse", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_bench_small_size_max_iops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(100),

    Option("osd_bench_large_size_max_throughput", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M),

    Option("osd_bench_max_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M),

    Option("osd_bench_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(30),

    Option("osd_blkin_trace_all", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osdc_blkin_trace_all", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_discard_disconnected_ops", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_memory_target", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("When tcmalloc and cache autotuning is enabled, try to keep this many bytes mapped in memory.")
    .set_long_description("The minimum value must be at least equal to osd_memory_base + osd_memory_cache_min.")
    .set_default(4_G)
    .set_min(896_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_cache_autotune", "osd_memory_cache_min", "osd_memory_base", "osd_memory_target_autotune"}),

    Option("osd_memory_target_autotune", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("If enabled, allow orchestrator to automatically tune osd_memory_target")
    .set_default(false)
    .add_see_also({"osd_memory_target"}),

    Option("osd_memory_target_cgroup_limit_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Set the default value for osd_memory_target to the cgroup memory limit (if set) times this value")
    .set_long_description("A value of 0 disables this feature.")
    .set_default(0.8)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_memory_target"}),

    Option("osd_memory_base", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the minimum amount of memory in bytes the OSD will need.")
    .set_default(768_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_cache_autotune"}),

    Option("osd_memory_expected_fragmentation", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the percent of memory fragmentation.")
    .set_default(0.15)
    .set_min_max(0.0, 1.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_cache_autotune"}),

    Option("osd_memory_cache_min", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, set the minimum amount of memory used for caches.")
    .set_default(128_M)
    .set_min(128_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_cache_autotune"}),

    Option("osd_memory_cache_resize_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, wait this many seconds between resizing caches.")
    .set_default(1.0)
    .add_see_also({"bluestore_cache_autotune"}),

    Option("memstore_device_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_G),

    Option("memstore_page_set", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("memstore_page_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K),

    Option("memstore_debug_omit_block_device_write", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("write metadata only")
    .set_default(false)
    .add_see_also({"bluestore_debug_omit_block_device_write"}),

    Option("objectstore_blackhole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bdev_debug_inflight_ios", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bdev_inject_crash", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("bdev_inject_crash_flush_delay", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2),

    Option("bdev_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("bdev_aio_poll_ms", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250),

    Option("bdev_aio_max_queue_depth", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1024),

    Option("bdev_aio_reap_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(16),

    Option("bdev_block_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K),

    Option("bdev_read_buffer_alignment", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_K),

    Option("bdev_read_preallocated_huge_buffers", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("description of pools arrangement for huge page-based read buffers")
    .set_long_description("Arrangement of preallocated, huge pages-based pools for reading from a KernelDevice. Applied to minimize size of scatter-gather lists sent to NICs. Targets really  big buffers (>= 2 or 4 MBs). Keep in mind the system must be configured accordingly (see /proc/sys/vm/nr_hugepages). Otherwise the OSD wil fail early. Beware BlueStore, by default, stores large chunks across many smaller blobs. Increasing bluestore_max_blob_size changes that, and thus allows the data to be read back into small number of huge page-backed buffers.")
    .add_see_also({"bluestore_max_blob_size"}),

    Option("bdev_debug_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bdev_debug_aio_suicide_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1_min),

    Option("bdev_debug_aio_log_age", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(5.0),

    Option("bdev_nvme_unbind_from_kernel", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bdev_enable_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bdev_async_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bdev_flock_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("interval to retry the flock")
    .set_default(0.1),

    Option("bdev_flock_retry", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("times to retry the flock")
    .set_long_description("The number of times to retry on getting the block device lock. Programs such as systemd-udevd may compete with Ceph for this lock. 0 means 'unlimited'.")
    .set_default(3),

    Option("bluefs_alloc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Allocation unit size for DB and WAL devices")
    .set_default(1_M),

    Option("bluefs_shared_alloc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Allocation unit size for primary/shared device")
    .set_default(64_K),

    Option("bluefs_failed_shared_alloc_cooldown", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("duration(in seconds) untill the next attempt to use 'bluefs_shared_alloc_size' after facing ENOSPC failure.")
    .set_long_description("Cooldown period(in seconds) when BlueFS uses shared/slow device allocation size instead of \"bluefs_shared_alloc_size' one after facing recoverable (via fallback to smaller chunk size) ENOSPC failure. Intended primarily to avoid repetitive unsuccessful allocations which might be expensive.")
    .set_default(600.0),

    Option("bluefs_max_prefetch", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M),

    Option("bluefs_min_log_runway", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1_M),

    Option("bluefs_max_log_runway", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(4_M),

    Option("bluefs_log_compact_min_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(5.0),

    Option("bluefs_log_compact_min_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(16_M),

    Option("bluefs_min_flush_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_K),

    Option("bluefs_compact_log_sync", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bluefs_buffered_io", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enabled buffered IO for bluefs reads.")
    .set_long_description("When this option is enabled, bluefs will in some cases perform buffered reads.  This allows the kernel page cache to act as a secondary cache for things like RocksDB block reads.  For example, if the rocksdb block cache isn't large enough to hold all blocks during OMAP iteration, it may be possible to read them from page cache instead of from the disk.  This can dramatically improve performance when the osd_memory_target is too small to hold all entries in block cache but it does come with downsides.  It has been reported to occasionally cause excessive kernel swapping (and associated stalls) under certain workloads. Currently the best and most consistent performing combination appears to be enabling bluefs_buffered_io and disabling system level swap.  It is possible that this recommendation may change in the future however.")
    .set_default(true),

    Option("bluefs_sync_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bluefs_allocator", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("hybrid")
    .set_enum_allowed({"bitmap", "stupid", "avl", "hybrid"}),

    Option("bluefs_log_replay_check_allocations", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables checks for allocations consistency during log replay")
    .set_default(true),

    Option("bluefs_replay_recovery", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Attempt to read bluefs log so large that it became unreadable.")
    .set_long_description("If BlueFS log grows to extreme sizes (200GB+) it is likely that it becames unreadable. This options enables heuristics that scans devices for missing data. DO NOT ENABLE BY DEFAULT")
    .set_default(false),

    Option("bluefs_replay_recovery_disable_compact", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bluefs_check_for_zeros", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Check data read for suspicious pages")
    .set_long_description("Looks into data read to check if there is a 4K block entirely filled with zeros. If this happens, we re-read data. If there is difference, we print error to log.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_retry_disk_reads"}),

    Option("bluefs_check_volume_selector_on_umount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Check validity of volume selector on umount")
    .set_long_description("Checks if volume selector did not diverge from the state it should be in. Reference is constructed from bluefs inode table. Asserts on inconsistency.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluefs_check_volume_selector_often", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Periodically check validity of volume selector")
    .set_long_description("Periodically checks if current volume selector does not diverge from the valid state. Reference is constructed from bluefs inode table. Asserts on inconsistency. This is debug feature.")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"bluefs_check_volume_selector_on_umount"}),

    Option("bluestore_bluefs", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Use BlueFS to back rocksdb")
    .set_long_description("BlueFS allows rocksdb to share the same physical device(s) as the rest of BlueStore.  It should be used in all cases unless testing/developing an alternative metadata database for BlueStore.")
    .set_default(true)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_bluefs_env_mirror", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Mirror bluefs data to file system for testing/validation")
    .set_default(false)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_bluefs_max_free", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum free space allocated to BlueFS")
    .set_default(10_G),

    Option("bluestore_bluefs_alloc_failure_dump_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("How frequently (in seconds) to dump allocator on BlueFS space allocation failure")
    .set_default(0.0),

    Option("bluestore_spdk_mem", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Amount of dpdk memory size in MB")
    .set_long_description("If running multiple SPDK instances per node, you must specify the amount of dpdk memory size in MB each instance will use, to make sure each instance uses its own dpdk memory")
    .set_default(512),

    Option("bluestore_spdk_coremask", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("A hexadecimal bit mask of the cores to run on. Note the core numbering can change between platforms and should be determined beforehand")
    .set_default("0x1"),

    Option("bluestore_spdk_max_io_completion", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Maximal I/Os to be batched completed while checking queue pair completions, 0 means let spdk library determine it")
    .set_default(0),

    Option("bluestore_spdk_io_sleep", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Time period to wait if there is no completed I/O from polling")
    .set_default(5),

    Option("bluestore_block_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Path to block device/file")
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Size of file to create for backing bluestore")
    .set_default(100_G)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_block_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Create bluestore_block_path if it doesn't exist")
    .set_default(true)
    .set_flag(Option::FLAG_CREATE)
    .add_see_also({"bluestore_block_path", "bluestore_block_size"}),

    Option("bluestore_block_db_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Path for db block device")
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_block_db_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Size of file to create for bluestore_block_db_path")
    .set_default(0)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_block_db_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Create bluestore_block_db_path if it doesn't exist")
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .add_see_also({"bluestore_block_db_path", "bluestore_block_db_size"}),

    Option("bluestore_block_wal_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Path to block device/file backing bluefs wal")
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_block_wal_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Size of file to create for bluestore_block_wal_path")
    .set_default(96_M)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_block_wal_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Create bluestore_block_wal_path if it doesn't exist")
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .add_see_also({"bluestore_block_wal_path", "bluestore_block_wal_size"}),

    Option("bluestore_block_preallocate_file", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Preallocate file created via bluestore_block*_create")
    .set_default(false)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_ignore_data_csum", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Ignore checksum errors on read and do not generate an EIO error")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_csum_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default checksum algorithm to use")
    .set_long_description("crc32c, xxhash32, and xxhash64 are available.  The _16 and _8 variants use only a subset of the bits for more compact (but less reliable) checksumming.")
    .set_default("crc32c")
    .set_enum_allowed({"none", "crc32c", "crc32c_16", "crc32c_8", "xxhash32", "xxhash64"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_retry_disk_reads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of read retries on checksum validation error")
    .set_long_description("Retries to read data from the disk this many times when checksum validation fails to handle spurious read errors gracefully.")
    .set_default(3)
    .set_min_max(0, 255)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_min_alloc_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Minimum allocation size to allocate for an object")
    .set_long_description("A smaller allocation size generally means less data is read and then rewritten when a copy-on-write operation is triggered (e.g., when writing to something that was recently snapshotted).  Similarly, less data is journaled before performing an overwrite (writes smaller than min_alloc_size must first pass through the BlueStore journal).  Larger values of min_alloc_size reduce the amount of metadata required to describe the on-disk layout and reduce overall fragmentation.")
    .set_default(0)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_min_alloc_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default min_alloc_size value for rotational media")
    .set_default(4_K)
    .set_flag(Option::FLAG_CREATE)
    .add_see_also({"bluestore_min_alloc_size"}),

    Option("bluestore_min_alloc_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default min_alloc_size value for non-rotational (solid state)  media")
    .set_default(4_K)
    .set_flag(Option::FLAG_CREATE)
    .add_see_also({"bluestore_min_alloc_size"}),

    Option("bluestore_use_optimal_io_size_for_min_alloc_size", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Discover media optimal IO Size and use for min_alloc_size")
    .set_default(false)
    .set_flag(Option::FLAG_CREATE)
    .add_see_also({"bluestore_min_alloc_size"}),

    Option("bluestore_max_alloc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum size of a single allocation (0 for no max)")
    .set_default(0)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_prefer_deferred_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Writes smaller than this size will be written to the journal and then asynchronously written to the device.  This can be beneficial when using rotational media where seeks are expensive, and is helpful both with and without solid state journal/wal devices.")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_prefer_deferred_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default bluestore_prefer_deferred_size for rotational media")
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_prefer_deferred_size"}),

    Option("bluestore_prefer_deferred_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default bluestore_prefer_deferred_size for non-rotational (solid state) media")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_prefer_deferred_size"}),

    Option("bluestore_compression_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default policy for using compression when pool does not specify")
    .set_long_description("'none' means never use compression.  'passive' means use compression when clients hint that data is compressible.  'aggressive' means use compression unless clients hint that data is not compressible.  This option is used when the per-pool property for the compression mode is not present.")
    .set_default("none")
    .set_enum_allowed({"none", "passive", "aggressive", "force"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_compression_algorithm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default compression algorithm to use when writing object data")
    .set_long_description("This controls the default compressor to use (if any) if the per-pool property is not set.  Note that zstd is *not* recommended for bluestore due to high CPU overhead when compressing small amounts of data.")
    .set_default("snappy")
    .set_enum_allowed({"", "snappy", "zlib", "zstd", "lz4"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_compression_min_blob_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum chunk size to apply compression to when random access is expected for an object.")
    .set_long_description("Chunks larger than this are broken into smaller chunks before being compressed")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_compression_min_blob_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default value of bluestore_compression_min_blob_size for rotational media")
    .set_default(8_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_compression_min_blob_size"}),

    Option("bluestore_compression_min_blob_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default value of bluestore_compression_min_blob_size for non-rotational (solid state) media")
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_compression_min_blob_size"}),

    Option("bluestore_compression_max_blob_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum chunk size to apply compression to when non-random access is expected for an object.")
    .set_long_description("Chunks larger than this are broken into smaller chunks before being compressed")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_compression_max_blob_size_hdd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default value of bluestore_compression_max_blob_size for rotational media")
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_compression_max_blob_size"}),

    Option("bluestore_compression_max_blob_size_ssd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Default value of bluestore_compression_max_blob_size for non-rotational (solid state) media")
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_compression_max_blob_size"}),

    Option("bluestore_gc_enable_blob_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_gc_enable_total_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_max_blob_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_long_description("Bluestore blobs are collections of extents (ie on-disk data) originating from one or more objects.  Blobs can be compressed, typically have checksum data, may be overwritten, may be shared (with an extent ref map), or split.  This setting controls the maximum size a blob is allowed to be.")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_max_blob_size_hdd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_max_blob_size"}),

    Option("bluestore_max_blob_size_ssd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_max_blob_size"}),

    Option("bluestore_compression_required_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Compression ratio required to store compressed data")
    .set_long_description("If we compress data and get less than this we discard the result and store the original uncompressed data.")
    .set_default(0.875)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_extent_map_shard_max_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Max size (bytes) for a single extent map shard before splitting")
    .set_default(1200),

    Option("bluestore_extent_map_shard_target_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Target size (bytes) for a single extent map shard")
    .set_default(500),

    Option("bluestore_extent_map_shard_min_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Min size (bytes) for a single extent map shard before merging")
    .set_default(150),

    Option("bluestore_extent_map_shard_target_size_slop", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Ratio above/below target for a shard when trying to align to an existing extent or blob boundary")
    .set_default(0.2),

    Option("bluestore_extent_map_inline_shard_prealloc_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Preallocated buffer for inline shards")
    .set_default(256),

    Option("bluestore_cache_trim_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("How frequently we trim the bluestore cache")
    .set_default(0.05),

    Option("bluestore_cache_trim_max_skip_pinned", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Max pinned cache entries we consider before giving up")
    .set_default(1000),

    Option("bluestore_cache_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Cache replacement algorithm")
    .set_default("2q")
    .set_enum_allowed({"2q", "lru"}),

    Option("bluestore_2q_cache_kin_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("2Q paper suggests .5")
    .set_default(0.5),

    Option("bluestore_2q_cache_kout_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("2Q paper suggests .5")
    .set_default(0.5),

    Option("bluestore_cache_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Cache size (in bytes) for BlueStore")
    .set_long_description("This includes data and metadata cached by BlueStore as well as memory devoted to rocksdb's cache(s).")
    .set_default(0),

    Option("bluestore_cache_size_hdd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Default bluestore_cache_size for rotational media")
    .set_default(1_G)
    .add_see_also({"bluestore_cache_size"}),

    Option("bluestore_cache_size_ssd", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Default bluestore_cache_size for non-rotational (solid state) media")
    .set_default(3_G)
    .add_see_also({"bluestore_cache_size"}),

    Option("bluestore_cache_meta_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Ratio of bluestore cache to devote to metadata")
    .set_default(0.45)
    .add_see_also({"bluestore_cache_size"}),

    Option("bluestore_cache_kv_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Ratio of bluestore cache to devote to key/value database (RocksDB)")
    .set_default(0.45)
    .add_see_also({"bluestore_cache_size"}),

    Option("bluestore_cache_kv_onode_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Ratio of bluestore cache to devote to kv onode column family (rocksdb)")
    .set_default(0.04)
    .add_see_also({"bluestore_cache_size"}),

    Option("bluestore_cache_autotune", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Automatically tune the ratio of caches while respecting min values.")
    .set_default(true)
    .add_see_also({"bluestore_cache_size", "bluestore_cache_meta_ratio"}),

    Option("bluestore_cache_autotune_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("The number of seconds to wait between rebalances when cache autotune is enabled.")
    .set_default(5.0)
    .add_see_also({"bluestore_cache_autotune"}),

    Option("bluestore_cache_age_bin_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("The duration (in seconds) represented by a single cache age bin.")
    .set_default(1.0)
    .add_see_also({"bluestore_cache_age_bins_kv", "bluestore_cache_age_bins_kv_onode", "bluestore_cache_age_bins_meta", "bluestore_cache_age_bins_data"}),

    Option("bluestore_cache_age_bins_kv", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("A 10 element, space separated list of age bins for kv cache")
    .set_default("1 2 6 24 120 720 0 0 0 0")
    .add_see_also({"bluestore_cache_age_bin_interval"}),

    Option("bluestore_cache_age_bins_kv_onode", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("A 10 element, space separated list of age bins for kv onode cache")
    .set_default("0 0 0 0 0 0 0 0 0 720")
    .add_see_also({"bluestore_cache_age_bin_interval"}),

    Option("bluestore_cache_age_bins_meta", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("A 10 element, space separated list of age bins for onode cache")
    .set_default("1 2 6 24 120 720 0 0 0 0")
    .add_see_also({"bluestore_cache_age_bin_interval"}),

    Option("bluestore_cache_age_bins_data", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("A 10 element, space separated list of age bins for data cache")
    .set_default("1 2 6 24 120 720 0 0 0 0")
    .add_see_also({"bluestore_cache_age_bin_interval"}),

    Option("bluestore_alloc_stats_dump_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("The period (in second) for logging allocation statistics.")
    .set_default(1_day),

    Option("bluestore_kvbackend", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Key value database to use for bluestore")
    .set_default("rocksdb")
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_elastic_shared_blobs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Let bluestore to reuse existing shared blobs if possible")
    .set_long_description("Overwrites on snapped objects cause shared blob count to grow. It has a very negative performance effect. When enabled shared blob count is significantly reduced.")
    .set_default(true)
    .set_flag(Option::FLAG_CREATE),

    Option("bluestore_allocator", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Allocator policy")
    .set_long_description("Allocator to use for bluestore.  Stupid should only be used for testing.")
    .set_default("hybrid")
    .set_enum_allowed({"bitmap", "stupid", "avl", "hybrid", "zoned"}),

    Option("bluestore_freelist_blocks_per_key", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Block (and bits) per database key")
    .set_default(128),

    Option("bluestore_bitmapallocator_blocks_per_zone", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_K),

    Option("bluestore_bitmapallocator_span_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_K),

    Option("bluestore_max_deferred_txc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max transactions with deferred writes that can accumulate before we force flush deferred writes")
    .set_default(32),

    Option("bluestore_max_defer_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("max duration to force deferred submit")
    .set_default(3.0),

    Option("bluestore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Full set of rocksdb settings to override")
    .set_default("compression=kLZ4Compression,max_write_buffer_number=64,min_write_buffer_number_to_merge=6,compaction_style=kCompactionStyleLevel,write_buffer_size=16777216,max_background_jobs=4,level0_file_num_compaction_trigger=8,max_bytes_for_level_base=1073741824,max_bytes_for_level_multiplier=8,compaction_readahead_size=2MB,max_total_wal_size=1073741824,writable_file_max_buffer_size=0"),

    Option("bluestore_rocksdb_options_annex", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("An addition to bluestore_rocksdb_options. Allows setting rocksdb options without repeating the existing defaults."),

    Option("bluestore_rocksdb_cf", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable use of rocksdb column families for bluestore metadata")
    .set_default(true)
    #ifdef WITH_SEASTAR
    // This is necessary as the Seastar's allocator imposes restrictions
    // on the number of threads that entered malloc/free/*. Unfortunately,
    // RocksDB sharding in BlueStore dramatically lifted the number of
    // threads spawn during RocksDB's init.
    .set_validator([](std::string *value, std::string *error_message) {
      if (const bool parsed_value = strict_strtob(value->c_str(), error_message);
        error_message->empty() && parsed_value) {
        *error_message = "invalid BlueStore sharding configuration."
                         " Be aware any change takes effect only on mkfs!";
        return -EINVAL;
      } else {
        return 0;
      }
    })
    #endif
    ,

    Option("bluestore_rocksdb_cfs", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Definition of column families and their sharding")
    .set_long_description("Space separated list of elements: column_def [ '=' rocksdb_options ]. column_def := column_name [ '(' shard_count [ ',' hash_begin '-' [ hash_end ] ] ')' ]. Example: 'I=write_buffer_size=1048576 O(6) m(7,10-)'. Interval [hash_begin..hash_end) defines characters to use for hash calculation. Recommended hash ranges: O(0-13) P(0-8) m(0-16). Sharding of S,T,C,M,B prefixes is inadvised")
    .set_default("m(3) p(3,0-12) O(3,0-13)=block_cache={type=binned_lru} L=min_write_buffer_number_to_merge=32 P=min_write_buffer_number_to_merge=32"),

    Option("bluestore_qfsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run quick-fsck at mount comparing allocation-file to RocksDB allocation state")
    .set_default(true),

    Option("bluestore_fsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run fsck at mount")
    .set_default(false),

    Option("bluestore_fsck_on_mount_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run deep fsck at mount when bluestore_fsck_on_mount is set to true")
    .set_default(false),

    Option("bluestore_fsck_quick_fix_on_mount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Do quick-fix for the store at mount")
    .set_default(false),

    Option("bluestore_fsck_on_umount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run fsck at umount")
    .set_default(false),

    Option("bluestore_allocation_from_file", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Remove allocation info from RocksDB and store the info in a new allocation file")
    .set_default(true),

    Option("bluestore_debug_inject_allocation_from_file_failure", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Enables random error injections when restoring allocation map from file.")
    .set_long_description("Specifies error injection probability for restoring allocation map from file hence causing full recovery. Intended primarily for testing.")
    .set_default(0.0),

    Option("bluestore_fsck_on_umount_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run deep fsck at umount when bluestore_fsck_on_umount is set to true")
    .set_default(false),

    Option("bluestore_fsck_on_mkfs", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run fsck after mkfs")
    .set_default(true),

    Option("bluestore_fsck_on_mkfs_deep", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Run deep fsck after mkfs")
    .set_default(false),

    Option("bluestore_sync_submit_transaction", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Try to submit metadata transaction to rocksdb in queuing thread context")
    .set_default(false),

    Option("bluestore_fsck_read_bytes_cap", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum bytes read at once by deep fsck")
    .set_default(64_M)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_fsck_quick_fix_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of additional threads to perform quick-fix (shallow fsck) command")
    .set_default(2),

    Option("bluestore_fsck_shared_blob_tracker_size", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Size(a fraction of osd_memory_target, defaults to 128MB) of a hash table to track shared blobs ref counts. Higher the size, more precise is the tracker -> less overhead during the repair.")
    .set_default(0.03125)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_memory_target"}),

    Option("bluestore_throttle_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum bytes in flight before we throttle IO submission")
    .set_default(64_M)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_throttle_deferred_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum bytes for deferred writes before we throttle IO submission")
    .set_default(128_M)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_throttle_cost_per_io", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Overhead added to transaction cost (in bytes) for each IO")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_throttle_cost_per_io_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Default bluestore_throttle_cost_per_io for rotational media")
    .set_default(670000)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_throttle_cost_per_io"}),

    Option("bluestore_throttle_cost_per_io_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Default bluestore_throttle_cost_per_io for non-rotation (solid state) media")
    .set_default(4000)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_throttle_cost_per_io"}),

    Option("bluestore_deferred_batch_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max number of deferred writes before we flush the deferred write queue")
    .set_default(0)
    .set_min_max(0, 65535)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_deferred_batch_ops_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Default bluestore_deferred_batch_ops for rotational media")
    .set_default(64)
    .set_min_max(0, 65535)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_deferred_batch_ops"}),

    Option("bluestore_deferred_batch_ops_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Default bluestore_deferred_batch_ops for non-rotational (solid state) media")
    .set_default(16)
    .set_min_max(0, 65535)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"bluestore_deferred_batch_ops"}),

    Option("bluestore_nid_prealloc", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Number of unique object ids to preallocate at a time")
    .set_default(1024),

    Option("bluestore_blobid_prealloc", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Number of unique blob ids to preallocate at a time")
    .set_default(10_K),

    Option("bluestore_clone_cow", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use copy-on-write when cloning objects (versus reading and rewriting them at clone time)")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_default_buffered_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Cache read results by default (unless hinted NOCACHE or WONTNEED)")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_default_buffered_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Cache writes by default (unless hinted NOCACHE or WONTNEED)")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_debug_no_reuse_blocks", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_small_allocations", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("bluestore_debug_too_many_blobs_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(24576),

    Option("bluestore_debug_freelist", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_prefill", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("simulate fragmentation")
    .set_default(0.0),

    Option("bluestore_debug_prefragment_max", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_M),

    Option("bluestore_debug_inject_read_err", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_randomize_serial_transaction", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("bluestore_debug_omit_block_device_write", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_fsck_abort", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_omit_kv_commit", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_permit_any_bdev_label", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("bluestore_debug_random_read_err", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("bluestore_debug_inject_csum_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("inject crc verification errors into bluestore device reads")
    .set_default(0.0),

    Option("bluestore_debug_legacy_omap", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Allows mkfs to create OSD in legacy OMAP naming mode (neither per-pool nor per-pg). This is intended primarily for developers' purposes. The resulting OSD might/would be transformed to the currrently default 'per-pg' format when BlueStore's quick-fix or repair are applied.")
    .set_default(false),

    Option("bluestore_fsck_error_on_no_per_pool_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Make fsck error (instead of warn) when bluestore lacks per-pool stats, e.g., after an upgrade")
    .set_default(false),

    Option("bluestore_warn_on_bluefs_spillover", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable health indication on bluefs slow device usage")
    .set_default(true),

    Option("bluestore_warn_on_legacy_statfs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable health indication on lack of per-pool statfs reporting from bluestore")
    .set_default(true),

    Option("bluestore_warn_on_spurious_read_errors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable health indication when spurious read errors are observed by OSD")
    .set_default(true),

    Option("bluestore_fsck_error_on_no_per_pool_omap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Make fsck error (instead of warn) when objects without per-pool omap are found")
    .set_default(false),

    Option("bluestore_fsck_error_on_no_per_pg_omap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Make fsck error (instead of warn) when objects without per-pg omap are found")
    .set_default(false),

    Option("bluestore_warn_on_no_per_pool_omap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable health indication on lack of per-pool omap")
    .set_default(true),

    Option("bluestore_warn_on_no_per_pg_omap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable health indication on lack of per-pg omap")
    .set_default(false),

    Option("bluestore_log_op_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("log operation if it's slower than this age (seconds)")
    .set_default(5.0),

    Option("bluestore_log_omap_iterator_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("log omap iteration operation if it's slower than this age (seconds)")
    .set_default(5.0),

    Option("bluestore_log_collection_list_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("log collection list operation if it's slower than this age (seconds)")
    .set_default(1_min),

    Option("bluestore_debug_enforce_settings", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Enforces specific hw profile settings")
    .set_long_description("'hdd' enforces settings intended for BlueStore above a rotational drive. 'ssd' enforces settings intended for BlueStore above a solid drive. 'default' - using settings for the actual hardware.")
    .set_default("default")
    .set_enum_allowed({"default", "hdd", "ssd"}),

    Option("bluestore_avl_alloc_ff_max_search_count", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Search for this many ranges in first-fit mode before switching over to to best-fit mode. 0 to iterate through all ranges for required chunk.")
    .set_default(100),

    Option("bluestore_avl_alloc_ff_max_search_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Maximum distance to search in first-fit mode before switching over to to best-fit mode. 0 to iterate through all ranges for required chunk.")
    .set_default(16_M),

    Option("bluestore_avl_alloc_bf_threshold", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Sets threshold at which shrinking max free chunk size triggers enabling best-fit mode.")
    .set_long_description("AVL allocator works in two modes: near-fit and best-fit. By default, it uses very fast near-fit mode, in which it tries to fit a new block near the last allocated block of similar size. The second mode is much slower best-fit mode, in which it tries to find an exact match for the requested allocation. This mode is used when either the device gets fragmented or when it is low on free space. When the largest free block is smaller than 'bluestore_avl_alloc_bf_threshold', best-fit mode is used.")
    .set_default(128_K)
    .add_see_also({"bluestore_avl_alloc_bf_free_pct"}),

    Option("bluestore_avl_alloc_bf_free_pct", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Sets threshold at which shrinking free space (in %, integer) triggers enabling best-fit mode.")
    .set_long_description("AVL allocator works in two modes: near-fit and best-fit. By default, it uses very fast near-fit mode, in which it tries to fit a new block near the last allocated block of similar size. The second mode is much slower best-fit mode, in which it tries to find an exact match for the requested allocation. This mode is used when either the device gets fragmented or when it is low on free space. When free space is smaller than 'bluestore_avl_alloc_bf_free_pct', best-fit mode is used.")
    .set_default(4)
    .add_see_also({"bluestore_avl_alloc_bf_threshold"}),

    Option("bluestore_hybrid_alloc_mem_cap", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Maximum RAM hybrid allocator should use before enabling bitmap supplement")
    .set_default(64_M),

    Option("bluestore_volume_selection_policy", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Determines bluefs volume selection policy")
    .set_long_description("Determines bluefs volume selection policy. 'use_some_extra*' policy allows to override RocksDB level granularity and put high level's data to faster device even when the level doesn't completely fit there. 'fit_to_fast' policy enables using 100% of faster disk capacity and allows the user to turn on 'level_compaction_dynamic_level_bytes' option in RocksDB options.")
    .set_default("use_some_extra")
    .set_enum_allowed({"rocksdb_original", "use_some_extra", "use_some_extra_enforced", "fit_to_fast"}),

    Option("bluestore_volume_selection_reserved_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("DB level size multiplier. Determines amount of space at DB device to bar from the usage when 'use some extra' policy is in action. Reserved size is determined as sum(L_max_size[0], L_max_size[L-1]) + L_max_size[L] * this_factor")
    .set_default(2.0)
    .set_flag(Option::FLAG_STARTUP),

    Option("bluestore_volume_selection_reserved", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Space reserved at DB device and not allowed for 'use some extra' policy usage. Overrides 'bluestore_volume_selection_reserved_factor' setting and introduces straightforward limit.")
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP),

    Option("bdev_ioring", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables Linux io_uring API instead of libaio")
    .set_default(false),

    Option("bdev_ioring_hipri", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables Linux io_uring API Use polled IO completions")
    .set_default(false),

    Option("bdev_ioring_sqthread_poll", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables Linux io_uring API Offload submission/completion to kernel thread")
    .set_default(false),

    Option("bluestore_kv_sync_util_logging_s", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("KV sync thread utilization logging period")
    .set_long_description("How often (in seconds) to print KV sync thread utilization, not logged when set to 0 or when utilization is 0%")
    .set_default(10.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_fail_eio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("fail/crash on EIO")
    .set_long_description("whether bluestore osd fails on eio")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("bluestore_zero_block_detection", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("punch holes instead of writing zeros")
    .set_long_description("Intended for large-scale synthetic testing. Currently this is implemented with punch hole semantics, affecting the logical extent map of the object. This does not interact well with some RBD and CephFS features.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("kstore_max_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512),

    Option("kstore_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_M),

    Option("kstore_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("rocksdb"),

    Option("kstore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Options to pass through when RocksDB is used as the KeyValueDB for kstore.")
    .set_default("compression=kNoCompression"),

    Option("kstore_fsck_on_mount", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Whether or not to run fsck on mount for kstore.")
    .set_default(false),

    Option("kstore_fsck_on_mount_deep", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Whether or not to run deep fsck on mount for kstore")
    .set_default(true),

    Option("kstore_nid_prealloc", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_K),

    Option("kstore_sync_transaction", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("kstore_sync_submit_transaction", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("kstore_onode_map_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1_K),

    Option("kstore_default_stripe_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K),

    Option("filestore_rocksdb_options", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Options to pass through when RocksDB is used as the KeyValueDB for filestore.")
    .set_default("max_background_jobs=10,compaction_readahead_size=2097152,compression=kNoCompression"),

    Option("filestore_omap_backend", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("The KeyValueDB to use for filestore metadata (ie omap).")
    .set_default("rocksdb")
    .set_enum_allowed({"leveldb", "rocksdb"}),

    Option("filestore_omap_backend_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("The path where the filestore KeyValueDB should store it's database(s)."),

    Option("filestore_wbthrottle_enable", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enabling throttling of operations to backing file system")
    .set_default(true),

    Option("filestore_wbthrottle_btrfs_bytes_start_flusher", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Start flushing (fsyncing) when this many bytes are written(btrfs)")
    .set_default(40_M),

    Option("filestore_wbthrottle_btrfs_bytes_hard_limit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Block writes when this many bytes haven't been flushed (fsynced) (btrfs)")
    .set_default(400_M),

    Option("filestore_wbthrottle_btrfs_ios_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Start flushing (fsyncing) when this many IOs are written (brtrfs)")
    .set_default(500),

    Option("filestore_wbthrottle_btrfs_ios_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Block writes when this many IOs haven't been flushed (fsynced) (btrfs)")
    .set_default(5000),

    Option("filestore_wbthrottle_btrfs_inodes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Start flushing (fsyncing) when this many distinct inodes have been modified (btrfs)")
    .set_default(500),

    Option("filestore_wbthrottle_xfs_bytes_start_flusher", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Start flushing (fsyncing) when this many bytes are written(xfs)")
    .set_default(40_M),

    Option("filestore_wbthrottle_xfs_bytes_hard_limit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Block writes when this many bytes haven't been flushed (fsynced) (xfs)")
    .set_default(400_M),

    Option("filestore_wbthrottle_xfs_ios_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Start flushing (fsyncing) when this many IOs are written (xfs)")
    .set_default(500),

    Option("filestore_wbthrottle_xfs_ios_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Block writes when this many IOs haven't been flushed (fsynced) (xfs)")
    .set_default(5000),

    Option("filestore_wbthrottle_xfs_inodes_start_flusher", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Start flushing (fsyncing) when this many distinct inodes have been modified (xfs)")
    .set_default(500),

    Option("filestore_wbthrottle_btrfs_inodes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Block writing when this many inodes have outstanding writes (btrfs)")
    .set_default(5000),

    Option("filestore_wbthrottle_xfs_inodes_hard_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Block writing when this many inodes have outstanding writes (xfs)")
    .set_default(5000),

    Option("filestore_odsync_write", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Write with O_DSYNC")
    .set_default(false),

    Option("filestore_index_retry_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_debug_inject_read_err", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_debug_random_read_err", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_debug_omap_check", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_omap_header_cache_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_K),

    Option("filestore_max_inline_xattr_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(0),

    Option("filestore_max_inline_xattr_size_xfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K),

    Option("filestore_max_inline_xattr_size_btrfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(2_K),

    Option("filestore_max_inline_xattr_size_other", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(512),

    Option("filestore_max_inline_xattrs", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0),

    Option("filestore_max_inline_xattrs_xfs", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10),

    Option("filestore_max_inline_xattrs_btrfs", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(10),

    Option("filestore_max_inline_xattrs_other", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(2),

    Option("filestore_max_xattr_value_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(0),

    Option("filestore_max_xattr_value_size_xfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K),

    Option("filestore_max_xattr_value_size_btrfs", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K),

    Option("filestore_max_xattr_value_size_other", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_K),

    Option("filestore_sloppy_crc", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_sloppy_crc_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K),

    Option("filestore_max_alloc_hint_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(1_M),

    Option("filestore_max_sync_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Period between calls to syncfs(2) and journal trims (seconds)")
    .set_default(5.0),

    Option("filestore_min_sync_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Minimum period between calls to syncfs(2)")
    .set_default(0.01),

    Option("filestore_btrfs_snap", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true),

    Option("filestore_btrfs_clone_range", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use btrfs clone_range ioctl to efficiently duplicate objects")
    .set_default(true),

    Option("filestore_zfs_snap", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_fsync_flushes_journal_data", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_fiemap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use fiemap ioctl(2) to determine which parts of objects are sparse")
    .set_default(false),

    Option("filestore_punch_hole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use fallocate(2) FALLOC_FL_PUNCH_HOLE to efficiently zero ranges of objects")
    .set_default(false),

    Option("filestore_seek_data_hole", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use lseek(2) SEEK_HOLE and SEEK_DATA to determine which parts of objects are sparse")
    .set_default(false),

    Option("filestore_splice", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use splice(2) to more efficiently copy data between files")
    .set_default(false),

    Option("filestore_fadvise", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use posix_fadvise(2) to pass hints to file system")
    .set_default(true),

    Option("filestore_collect_device_partition_information", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Collect metadata about the backing file system on OSD startup")
    .set_default(true),

    Option("filestore_xfs_extsize", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use XFS extsize ioctl(2) to hint allocator about expected write sizes")
    .set_default(false),

    Option("filestore_journal_parallel", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_journal_writeahead", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_journal_trailing", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_queue_max_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max IO operations in flight")
    .set_default(50),

    Option("filestore_queue_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Max (written) bytes in flight")
    .set_default(100_M),

    Option("filestore_caller_concurrency", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(10),

    Option("filestore_expected_throughput_bytes", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Expected throughput of backend device (aids throttling calculations)")
    .set_default(209715200.0),

    Option("filestore_expected_throughput_ops", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Expected through of backend device in IOPS (aids throttling calculations)")
    .set_default(200.0),

    Option("filestore_queue_max_delay_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_queue_high_delay_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_queue_max_delay_multiple_bytes", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_queue_high_delay_multiple_bytes", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_queue_max_delay_multiple_ops", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_queue_high_delay_multiple_ops", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("filestore_queue_low_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.3),

    Option("filestore_queue_high_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.9),

    Option("filestore_op_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Threads used to apply changes to backing file system")
    .set_default(2),

    Option("filestore_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Seconds before a worker thread is considered stalled")
    .set_default(1_min),

    Option("filestore_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Seconds before a worker thread is considered dead")
    .set_default(3_min),

    Option("filestore_commit_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Seconds before backing file system is considered hung")
    .set_default(10_min),

    Option("filestore_fiemap_threshold", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K),

    Option("filestore_merge_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-10),

    Option("filestore_split_multiple", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2),

    Option("filestore_split_rand_factor", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(20),

    Option("filestore_update_to", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1000),

    Option("filestore_blackhole", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("filestore_fd_cache_size", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(128),

    Option("filestore_fd_cache_shards", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(16),

    Option("filestore_ondisk_finisher_threads", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1),

    Option("filestore_apply_finisher_threads", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1),

    Option("filestore_dump_file", Option::TYPE_STR, Option::LEVEL_DEV),

    Option("filestore_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("filestore_inject_stall", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("filestore_fail_eio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true),

    Option("filestore_debug_verify_split", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("journal_dio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true),

    Option("journal_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true),

    Option("journal_force_aio", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("journal_block_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K),

    Option("journal_block_align", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true),

    Option("journal_write_header_frequency", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0),

    Option("journal_max_write_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Max bytes in flight to journal")
    .set_default(10_M),

    Option("journal_max_write_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max IOs in flight to journal")
    .set_default(100),

    Option("journal_throttle_low_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.6),

    Option("journal_throttle_high_threshhold", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.9),

    Option("journal_throttle_high_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("journal_throttle_max_multiple", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("journal_align_min_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(64_K),

    Option("journal_replay_from", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0),

    Option("journal_zero_on_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("journal_ignore_corruption", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("journal_discard", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("fio_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("/tmp/fio"),

    Option("rados_mon_op_timeout", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("timeout for operations handled by monitors such as statfs (0 is unlimited)")
    .set_default(0)
    .set_min(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("rados_osd_op_timeout", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("timeout for operations handled by osds such as write (0 is unlimited)")
    .set_default(0)
    .set_min(0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("rados_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("mgr_connect_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .add_service("common"),

    Option("mgr_client_service_daemon_unregister_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Time to wait during shutdown to deregister service with mgr")
    .set_default(1.0),

    Option("throttler_perf_counter", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("event_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("bluestore_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable bluestore event tracing.")
    .set_default(false),

    Option("bluestore_throttle_trace_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Rate at which to sample bluestore transactions (per second)")
    .set_default(0.0),

    Option("debug_deliberately_leak_memory", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("debug_asserts_on_shutdown", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Enable certain asserts to check for refcounting bugs on shutdown; see http://tracker.ceph.com/issues/21738")
    .set_default(false),

    Option("debug_asok_assert_abort", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("allow commands 'assert' and 'abort' via asok for testing crash dumps etc")
    .set_default(false),

    Option("target_max_misplaced_ratio", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_description("Max ratio of misplaced objects to target when throttling data rebalancing activity")
    .set_default(0.05),

    Option("device_failure_prediction_mode", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Method used to predict device failures")
    .set_long_description("To disable prediction, use 'none',  'local' uses a prediction model that runs inside the mgr daemon.  'cloud' will share metrics with a cloud service and query the service for devicelife expectancy.")
    .set_default("none")
    .set_enum_allowed({"none", "local", "cloud"})
    .set_flag(Option::FLAG_RUNTIME),

    Option("gss_ktab_client_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("GSS/KRB5 Keytab file for client authentication")
    .set_long_description("This sets the full path for the GSS/Kerberos client keytab file location.")
    .set_default("/var/lib/ceph/$name/gss_client_$name.ktab")
    .add_service({"mon", "osd"}),

    Option("gss_target_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_long_description("This sets the gss target service name.")
    .set_default("ceph")
    .add_service({"mon", "osd"}),

    Option("debug_disable_randomized_ping", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Disable heartbeat ping randomization for testing purposes")
    .set_default(false),

    Option("debug_heartbeat_testing_span", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Override 60 second periods for testing only")
    .set_default(0),

    Option("librados_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Size of thread pool for Objecter")
    .set_default(2)
    .set_min(1)
    .add_tag("client"),

    Option("osd_asio_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Size of thread pool for ASIO completions")
    .set_default(2)
    .set_min(1)
    .add_tag("osd"),

    Option("cephsqlite_lock_renewal_interval", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED)
    .set_description("number of milliseconds before lock is renewed")
    .set_default(2000)
    .set_min(100)
    .add_tag("client")
    .add_see_also({"cephsqlite_lock_renewal_timeout"}),

    Option("cephsqlite_lock_renewal_timeout", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED)
    .set_description("number of milliseconds before transaction lock times out")
    .set_long_description("The amount of time before a running libcephsqlite VFS connection has to renew a lock on the database before the lock is automatically lost. If the lock is lost, the VFS will abort the process to prevent database corruption.")
    .set_default(30000)
    .set_min(100)
    .add_tag("client")
    .add_see_also({"cephsqlite_lock_renewal_interval"}),

    Option("cephsqlite_blocklist_dead_locker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("blocklist the last dead owner of the database lock")
    .set_long_description("Require that the Ceph SQLite VFS blocklist the last dead owner of the database when cleanup was incomplete. DO NOT CHANGE THIS UNLESS YOU UNDERSTAND THE RAMIFICATIONS. CORRUPTION MAY RESULT.")
    .set_default(true)
    .add_tag("client"),

    Option("bdev_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Explicitly set the device type to select the driver if it's needed")
    .set_enum_allowed({"aio", "spdk", "pmem", "hm_smr"}),

    Option("bluestore_cleaner_sleep_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("How long cleaner should sleep before re-checking utilization")
    .set_default(5.0),

    Option("jaeger_tracing_enable", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Ceph should use jaeger tracing system")
    .set_default(false)
    .add_service({"rgw", "osd"}),

    Option("jaeger_agent_port", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("port number of the jaeger agent")
    .set_default(6799)
    .add_service({"rgw", "osd"}),

    Option("mgr_ttl_cache_expire_seconds", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Set the time to live in seconds - set to 0 to disable the cache.")
    .set_default(0)
    .add_service("mgr"),


  });
}
