#include <gtest/gtest.h>

#include "config.h"

#if USE_MYSQL

#include <sstream>

#include <mysqlxx/PoolFactory.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

namespace
{

std::string entryName(const std::string & xml, const std::string & config_name = "source")
{
    std::stringstream stream(xml); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration(stream));
    return mysqlxx::getPoolEntryName(*config, config_name, /* default_max_connections= */ 16);
}

}

/// A pool that is not shared is never cached, so it needs no key.
TEST(MySQLPoolEntryName, NotSharedHasNoName)
{
    EXPECT_EQ(entryName("<c><source><host>h</host><port>3306</port><user>u</user><db>d</db></source></c>"), "");
}

/// Two sources that differ only by the Unix socket talk to different MySQL instances, so they must
/// not alias to the same cached pool.
TEST(MySQLPoolEntryName, SocketIsPartOfTheEndpoint)
{
    const auto with_socket = [](const std::string & socket)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><user>u</user><db>d</db>"
            "<socket>" + socket + "</socket></source></c>");
    };

    EXPECT_NE(with_socket("/run/mysqld/first.sock"), with_socket("/run/mysqld/second.sock"));
    EXPECT_EQ(with_socket("/run/mysqld/first.sock"), with_socket("/run/mysqld/first.sock"));
}

/// The socket of a replica is resolved the same way `Pool::Pool` resolves it: the replica-level value
/// first, the parent configuration as the fallback.
TEST(MySQLPoolEntryName, ReplicaSocketOverridesTheParentOne)
{
    const auto with_sockets = [](const std::string & parent, const std::string & replica)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><user>u</user><db>d</db>"
            "<socket>" + parent + "</socket>"
            "<replica><priority>1</priority><socket>" + replica + "</socket></replica>"
            "</source></c>");
    };

    EXPECT_NE(with_sockets("/run/parent.sock", "/run/first.sock"), with_sockets("/run/parent.sock", "/run/second.sock"));
    /// The parent value is only a fallback: it does not change the key once the replica overrides it.
    EXPECT_EQ(with_sockets("/run/a.sock", "/run/first.sock"), with_sockets("/run/b.sock", "/run/first.sock"));
}

/// The database of a replica is resolved with the same lookup order, and it selects the data.
TEST(MySQLPoolEntryName, ReplicaDatabaseOverridesTheParentOne)
{
    const auto with_db = [](const std::string & db)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>parent</db>"
            "<replica><priority>1</priority><db>" + db + "</db></replica>"
            "</source></c>");
    };

    EXPECT_NE(with_db("first"), with_db("second"));
}

/// The TLS credentials decide as whom the pool authenticates, in both forms (a path taken from the
/// configuration file and the contents of the same file).
TEST(MySQLPoolEntryName, TLSCredentialsArePartOfTheKey)
{
    const auto with_tls = [](const std::string & keys)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            + keys + "</source></c>");
    };

    EXPECT_NE(with_tls("<ssl_cert>/a.pem</ssl_cert>"), with_tls("<ssl_cert>/b.pem</ssl_cert>"));
    EXPECT_NE(with_tls("<ssl_cert_pem>a</ssl_cert_pem>"), with_tls("<ssl_cert_pem>b</ssl_cert_pem>"));
    EXPECT_NE(with_tls("<ssl_ca_pem>a</ssl_ca_pem>"), with_tls(""));
    /// A credential of a replica is folded into the segment of that replica.
    EXPECT_NE(
        with_tls("<replica><priority>1</priority><ssl_key_pem>a</ssl_key_pem></replica>"),
        with_tls("<replica><priority>1</priority><ssl_key_pem>b</ssl_key_pem></replica>"));
}

/// The hash of the credentials frames every field with its length, so distinct tuples that
/// concatenate to the same byte stream must not collapse to the same key.
TEST(MySQLPoolEntryName, TLSCredentialFieldsAreFramed)
{
    const auto with_tls = [](const std::string & keys)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            + keys + "</source></c>");
    };

    /// The same bytes split differently between two adjacent fields.
    EXPECT_NE(
        with_tls("<ssl_ca_pem>ab</ssl_ca_pem><ssl_cert_pem>c</ssl_cert_pem>"),
        with_tls("<ssl_ca_pem>a</ssl_ca_pem><ssl_cert_pem>bc</ssl_cert_pem>"));
    /// The same value shifted into another field.
    EXPECT_NE(with_tls("<ssl_ca_pem>x</ssl_ca_pem>"), with_tls("<ssl_cert_pem>x</ssl_cert_pem>"));
    /// A path and the contents are different credentials even when they read the same.
    EXPECT_NE(with_tls("<ssl_ca>x</ssl_ca>"), with_tls("<ssl_ca_pem>x</ssl_ca_pem>"));
}

/// The password decides as whom the pool authenticates, exactly like the TLS credentials.
TEST(MySQLPoolEntryName, PasswordIsPartOfTheKey)
{
    const auto with_password = [](const std::string & password)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            "<password>" + password + "</password></source></c>");
    };

    EXPECT_NE(with_password("first"), with_password("second"));
    /// The password itself never appears in the key: it is folded into the hash of the credentials.
    EXPECT_EQ(with_password("secret").find("secret"), std::string::npos);
}

/// The per-connection settings that `Pool::Pool` reads change how the pooled connections behave, so
/// a source that asks for different values must not inherit the pool of whichever source came first.
TEST(MySQLPoolEntryName, ConnectionSettingsArePartOfTheKey)
{
    const auto with_settings = [](const std::string & settings)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            + settings + "</source></c>");
    };

    EXPECT_NE(with_settings("<connect_timeout>1</connect_timeout>"), with_settings("<connect_timeout>2</connect_timeout>"));
    EXPECT_NE(with_settings("<rw_timeout>1</rw_timeout>"), with_settings("<rw_timeout>2</rw_timeout>"));
    EXPECT_NE(with_settings("<enable_local_infile>1</enable_local_infile>"), with_settings("<enable_local_infile>0</enable_local_infile>"));
    EXPECT_NE(with_settings("<opt_reconnect>1</opt_reconnect>"), with_settings("<opt_reconnect>0</opt_reconnect>"));
    EXPECT_NE(with_settings("<background_reconnect>1</background_reconnect>"), with_settings(""));

    /// The same settings resolved for a replica: the timeouts are read at the replica level only, the
    /// rest falls back to the parent configuration.
    const auto with_replica_settings = [](const std::string & settings)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            "<replica><priority>1</priority>" + settings + "</replica></source></c>");
    };

    EXPECT_NE(with_replica_settings("<rw_timeout>1</rw_timeout>"), with_replica_settings("<rw_timeout>2</rw_timeout>"));
    EXPECT_NE(with_replica_settings("<opt_reconnect>1</opt_reconnect>"), with_replica_settings("<opt_reconnect>0</opt_reconnect>"));
}

/// The priority orders the replicas inside the pool.
TEST(MySQLPoolEntryName, ReplicaPriorityIsPartOfTheKey)
{
    const auto with_priorities = [](const std::string & first, const std::string & second)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            "<replica><priority>" + first + "</priority><host>a</host></replica>"
            "<replica><priority>" + second + "</priority><host>b</host></replica>"
            "</source></c>");
    };

    EXPECT_NE(with_priorities("1", "2"), with_priorities("2", "1"));
}

/// A single physical pool cannot have two different sizes or wait semantics.
TEST(MySQLPoolEntryName, PoolSettingsArePartOfTheKey)
{
    const auto with_pool_size = [](const std::string & size)
    {
        return entryName(
            "<c><source><share_connection>1</share_connection><host>h</host><port>3306</port><user>u</user><db>d</db>"
            "<connection_pool_size>" + size + "</connection_pool_size></source></c>");
    };

    EXPECT_NE(with_pool_size("2"), with_pool_size("4"));
}

#endif
