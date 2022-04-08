#pragma once

#include "CommandCopy.cpp"
#include "CommandLink.cpp"
#include "CommandList.cpp"
#include "CommandListDisks.cpp"
#include "CommandMove.cpp"
#include "CommandRead.cpp"
#include "CommandRemove.cpp"
#include "CommandWrite.cpp"

#include <Common/Config/ConfigProcessor.h>
#include <Loggers/Loggers.h>
#include <Client/ClientBase.h>
#include <Client/LocalConnection.h>

#include <Common/ProgressIndication.h>
#include <Common/StatusFile.h>
#include <Common/InterruptListener.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

using CommandPtr = std::unique_ptr<ICommand>;

class DisksApp : public Poco::Util::Application, public Loggers
{
public:
    DisksApp() = default;

    void init(std::vector<String> & common_arguments);

    int main(const std::vector<String> & args) override;

protected:
    void loadConfiguration();

    static String getDefaultConfigFileName();

    void addOptions(
        std::optional<ProgramOptionsDescription> & options_description,
        boost::program_options::positional_options_description & positional_options_description);
    void processOptions();

    void printHelpMessage(std::optional<ProgramOptionsDescription> & command_option_description);

    size_t findCommandPos(std::vector<String> & common_arguments);

private:
    void parseAndCheckOptions(
        std::optional<ProgramOptionsDescription> & options_description,
        boost::program_options::positional_options_description & positional_options_description,
        std::vector<String> & arguments);

protected:
    String config_path;

    ContextMutablePtr global_context;
    SharedContextHolder shared_context;

    String command_name;
    std::vector<String> command_flags;

    std::unordered_set<String> supported_commands;

    std::unordered_map<String, CommandPtr> command_descriptions;

    po::variables_map options;
};
}
