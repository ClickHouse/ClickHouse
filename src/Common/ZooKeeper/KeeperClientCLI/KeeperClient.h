#pragma once

#include "Parser.h"
#include "Commands.h"
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/Names.h>
#include <Client/LineReader.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Util/Application.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

static const NameSet four_letter_word_commands
    {
        "ruok", "mntr", "srvr", "stat", "srst", "conf",
        "cons", "crst", "envi", "dirs", "isro", "wchs",
        "wchc", "wchp", "dump", "csnp", "lgif", "rqld",
    };

class KeeperClientBase
{
public:
    KeeperClientBase(std::ostream & cout_ = std::cout, std::ostream & cerr_ = std::cerr);

    fs::path getAbsolutePath(const String & relative) const;

    void askConfirmation(const String & prompt, std::function<void()> && callback);

    String executeFourLetterCommand(const String & command);

    zkutil::ZooKeeperPtr zookeeper;
    std::filesystem::path cwd = "/";
    std::function<void()> confirmation_callback;

    inline static std::map<String, Command> commands;

    std::ostream & cout;
    std::ostream & cerr;

    bool processQueryText(const String & text);

protected:

    void loadCommands(std::vector<Command> && new_commands);

    std::vector<String> getCompletions(const String & prefix) const;

    String history_file;
    UInt32 history_max_entries; /// Maximum number of entries in the history file.

    LineReader::Suggest suggest;

    zkutil::ZooKeeperArgs zk_args;

    bool ask_confirmation = true;
    bool waiting_confirmation = false;

    std::vector<String> registered_commands_and_four_letter_words;
};

}
