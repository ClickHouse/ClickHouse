#pragma once

#include <Common/ZooKeeper/KeeperClientCLI/Commands.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/Names.h>
#include <filesystem>
#include <future>
#include <unordered_map>


namespace fs = std::filesystem;

namespace DB
{

static const NameSet four_letter_word_commands
    {
        "ruok", "mntr", "srvr", "stat", "srst", "conf",
        "cons", "crst", "envi", "dirs", "isro", "wchs",
        "wchc", "wchp", "dump", "csnp", "lgif", "rqld",
        "rclc", "clrs", "ftfl", "ydld", "pfev", "lgrq",
        "rcfg", "rcvr", "apiv", "jmst", "jmfp", "jmep",
        "jmdp",
    };

/// Format a ZooKeeper node name for display and round-tripping through the parser.
/// Returns the name bare when it contains no special characters, or wrapped in
/// single quotes with \' and \\ escaping otherwise. The result is always parseable
/// by parseKeeperArg (either as a bare token or as an inline quoted segment).
/// Used by both `ls` output and tab completion.
String formatKeeperNodeName(const String & name);

class KeeperClientBase
{
public:
    explicit KeeperClientBase(std::ostream & cout_, std::ostream & cerr_);

    fs::path getAbsolutePath(const String & relative) const;

    void askConfirmation(const String & prompt, std::function<void()> && callback);

    virtual String executeFourLetterCommand(const String & command);

    zkutil::ZooKeeperPtr zookeeper;
    std::filesystem::path cwd = "/";
    std::function<void()> confirmation_callback;
    bool ask_confirmation = true;

    inline static std::map<String, Command> commands;

    std::unordered_map<String, std::future<Coordination::WatchResponse>> watches;

    std::ostream & cout;
    std::ostream & cerr;

    void processQueryText(const String & text);

    virtual ~KeeperClientBase() = default;

protected:

    void loadCommands(std::vector<Command> && new_commands);

    bool waiting_confirmation = false;

    std::vector<String> registered_commands_and_four_letter_words;
};

}
