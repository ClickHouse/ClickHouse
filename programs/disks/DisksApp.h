#pragma once

#include <Loggers/Loggers.h>

#include <Interpreters/Context.h>
#include <Poco/Util/Application.h>

#include <boost/program_options.hpp>

namespace DB
{

class ICommand;
using CommandPtr = std::unique_ptr<ICommand>;

namespace po = boost::program_options;
using ProgramOptionsDescription = boost::program_options::options_description;
using CommandLineOptions = boost::program_options::variables_map;

class DisksApp : public Poco::Util::Application, public Loggers
{
public:
    DisksApp() = default;
    ~DisksApp() override;

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
