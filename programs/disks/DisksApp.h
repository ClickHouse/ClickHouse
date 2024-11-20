#pragma once

#include <unordered_map>
#include <vector>
#include <Client/LineReader.h>
#include <Loggers/Loggers.h>
#include "DisksClient.h"
#include "ICommand_fwd.h"

#include <Interpreters/Context.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <Poco/Util/Application.h>

namespace DB
{

using ProgramOptionsDescription = boost::program_options::options_description;
using CommandLineOptions = boost::program_options::variables_map;

class DisksApp : public Poco::Util::Application
{
public:
    void addOptions();

    void processOptions();

    bool processQueryText(const String & text);

    void init(const std::vector<String> & common_arguments);

    int main(const std::vector<String> & /*args*/) override;

    CommandPtr getCommandByName(const String & command) const;

    void initializeHistoryFile();

    static void parseAndCheckOptions(
        const std::vector<String> & arguments, const ProgramOptionsDescription & options_description, CommandLineOptions & options);

    void printEntryHelpMessage() const;
    void printAvailableCommandsHelpMessage() const;
    void printCommandHelpMessage(String command_name) const;
    void printCommandHelpMessage(CommandPtr command) const;
    String getCommandLineWithAliases(CommandPtr command) const;


    std::vector<String> getCompletions(const String & prefix) const;

    std::vector<String> getEmptyCompletion(String command_name) const;

    ~DisksApp() override;

private:
    void runInteractive();
    void runInteractiveReplxx();
    void runInteractiveTestMode();

    String getDefaultConfigFileName();

    std::vector<String> getCommandsToComplete(const String & command_prefix) const;

    // Fields responsible for the REPL work
    String history_file;
    LineReader::Suggest suggest;
    static LineReader::Patterns query_extenders;
    static LineReader::Patterns query_delimiters;
    static String word_break_characters;

    // General command line arguments parsing fields

    SharedContextHolder shared_context;
    ContextMutablePtr global_context;
    ProgramOptionsDescription options_description;
    CommandLineOptions options;
    std::unordered_map<String, CommandPtr> command_descriptions;

    std::optional<String> query;

    const std::unordered_map<String, String> aliases
        = {{"cp", "copy"},
           {"mv", "move"},
           {"ls", "list"},
           {"list_disks", "list-disks"},
           {"ln", "link"},
           {"rm", "remove"},
           {"cat", "read"},
           {"r", "read"},
           {"w", "write"},
           {"create", "touch"},
           {"delete", "remove"},
           {"ls-disks", "list-disks"},
           {"ls_disks", "list-disks"},
           {"packed_io", "packed-io"},
           {"change-dir", "cd"},
           {"change_dir", "cd"},
           {"switch_disk", "switch-disk"},
           {"current", "current_disk_with_path"},
           {"current_disk", "current_disk_with_path"},
           {"current_path", "current_disk_with_path"},
           {"cur", "current_disk_with_path"}};

    std::set<String> multidisk_commands = {"copy", "packed-io", "switch-disk", "cd"};

    std::unique_ptr<DisksClient> client{};
};
}
