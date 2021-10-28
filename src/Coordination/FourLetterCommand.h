#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperInfos.h>
#include <IO/WriteBufferFromString.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace DB
{
struct IFourLetterCommand;
using FourLetterCommandPtr = std::shared_ptr<DB::IFourLetterCommand>;

/// Just like zookeeper Four Letter Words commands, CH Keeper responds to a small set of commands.
/// Each command is composed of four letters, these commands are useful to monitor and issue system problems.
/// The feature is based on Zookeeper 3.5.9, details is in https://zookeeper.apache.org/doc/r3.5.9/zookeeperAdmin.html#sc_zkCommands.
struct IFourLetterCommand
{
public:
    using StringBuffer = DB::WriteBufferFromOwnString;
    explicit IFourLetterCommand(const KeeperDispatcher & keeper_dispatcher_);

    virtual String name() = 0;
    virtual String run() = 0;

    virtual ~IFourLetterCommand();
    Int32 code();

    static inline String toName(Int32 code);
    static inline Int32 toCode(const String & name);

    static void printSet(StringBuffer & buffer, std::unordered_set<String> & set, String && prefix);

protected:
    const KeeperDispatcher & keeper_dispatcher;
};

struct FourLetterCommandFactory : private boost::noncopyable
{
public:
    using Commands = std::unordered_map<Int32, FourLetterCommandPtr>;
    using WhiteList = std::vector<Int32>;

    static constexpr Int32 WHITE_LIST_ALL = 0;

    bool isKnown(Int32 code);
    bool isEnabled(Int32 code);

    FourLetterCommandPtr get(Int32 code);

    /// There is no need to make it thread safe, because registration is no initialization and get is after startup.
    void registerCommand(FourLetterCommandPtr & command);
    void initializeWhiteList(const KeeperDispatcher & keeper_dispatcher);

    void checkInitialization() const;
    bool isInitialized() const { return initialized; }
    void setInitialize(bool flag) { initialized = flag; }

    static FourLetterCommandFactory & instance();
    static void registerCommands(const KeeperDispatcher & keeper_dispatcher);

private:
    volatile bool initialized = false;
    Commands commands;
    WhiteList white_list;
};

/**Tests if server is running in a non-error state. The server will respond with imok if it is running.
 * Otherwise it will not respond at all.
 *
 * A response of "imok" does not necessarily indicate that the server has joined the quorum,
 * just that the server process is active and bound to the specified client port.
 * Use "stat" for details on state wrt quorum and client connection information.
 */
struct RuokCommand : public IFourLetterCommand
{
    explicit RuokCommand(const KeeperDispatcher & keeper_dispatcher_) : IFourLetterCommand(keeper_dispatcher_) { }

    String name() override;
    String run() override;
    ~RuokCommand() override;
};

/**Outputs a list of variables that could be used for monitoring the health of the cluster.
 *
 * echo mntr | nc localhost 2181
 * zk_version  3.5.9
 * zk_avg_latency  0
 * zk_max_latency  0
 * zk_min_latency  0
 * zk_packets_received 70
 * zk_packets_sent 69
 * zk_outstanding_requests 0
 * zk_server_state leader
 * zk_znode_count   4
 * zk_watch_count  0
 * zk_ephemerals_count 0
 * zk_approximate_data_size    27
 * zk_open_file_descriptor_count 23    - only available on Unix platforms
 * zk_max_file_descriptor_count 1024   - only available on Unix platforms
 * zk_followers 2                      - only exposed by the Leader
 * zk_synced_followers  2              - only exposed by the Leader
 * zk_pending_syncs 0                  - only exposed by the Leader
 */
struct MonitorCommand : public IFourLetterCommand
{
    explicit MonitorCommand(const KeeperDispatcher & keeper_dispatcher_) : IFourLetterCommand(keeper_dispatcher_) { }

    String name() override;
    String run() override;
    ~MonitorCommand() override;
private:
    static void print(StringBuffer & buf, const String & key, const String & value);
    static void print(StringBuffer & buf, const String & key, UInt64 value);
};

struct StatResetCommand : public IFourLetterCommand
{
    explicit StatResetCommand(const KeeperDispatcher & keeper_dispatcher_) : IFourLetterCommand(keeper_dispatcher_) { }

    String name() override;
    String run() override;
    ~StatResetCommand() override;
};

/** A command that does not do anything except reply to client with predefined message.
 * It is used to inform clients who execute none white listed four letter word commands.
 */
struct NopCommand : public IFourLetterCommand
{
    explicit NopCommand(const KeeperDispatcher & keeper_dispatcher_) : IFourLetterCommand(keeper_dispatcher_) { }

    String name() override;
    String run() override;
    ~NopCommand() override;
};

struct ConfCommand : public IFourLetterCommand
{
    explicit ConfCommand(const KeeperDispatcher & keeper_dispatcher_) : IFourLetterCommand(keeper_dispatcher_) { }

    String name() override;
    String run() override;
    ~ConfCommand() override;
};

}
