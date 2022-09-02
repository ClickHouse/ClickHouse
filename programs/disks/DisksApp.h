#pragma once

#include "CommandCopy.cpp"
#include "CommandLink.cpp"
#include "CommandList.cpp"
#include "CommandListDisks.cpp"
#include "CommandMove.cpp"
#include "CommandRead.cpp"
#include "CommandRemove.cpp"
#include "CommandWrite.cpp"

#include <Loggers/Loggers.h>

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
    static String getDefaultConfigFileName();

    void addOptions(
        ProgramOptionsDescription & options_description,
        boost::program_options::positional_options_description & positional_options_description);
    void processOptions();

    void printHelpMessage(ProgramOptionsDescription & command_option_description);

    size_t findCommandPos(std::vector<String> & common_arguments);

private:
    void parseAndCheckOptions(
        ProgramOptionsDescription & options_description,
        boost::program_options::positional_options_description & positional_options_description,
        std::vector<String> & arguments);

protected:
    ContextMutablePtr global_context;
    SharedContextHolder shared_context;

    String command_name;
    std::vector<String> command_arguments;

    std::unordered_set<String> supported_commands;

    std::unordered_map<String, CommandPtr> command_descriptions;

    po::variables_map options;
};
}
