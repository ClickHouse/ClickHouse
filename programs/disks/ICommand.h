#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskSelector.h>

#include <boost/program_options.hpp>

#include <Common/Config/ConfigProcessor.h>
#include <Poco/Util/Application.h>

#include <memory>

namespace DB
{

namespace po = boost::program_options;
using ProgramOptionsDescription = boost::program_options::options_description;
using CommandLineOptions = boost::program_options::variables_map;

class ICommand
{
public:
    ICommand() = default;

    virtual ~ICommand() = default;

    virtual void execute(
        const std::vector<String> & command_arguments,
        std::shared_ptr<DiskSelector> & disk_selector,
        Poco::Util::LayeredConfiguration & config) = 0;

    const std::optional<ProgramOptionsDescription> & getCommandOptions() const { return command_option_description; }

    void addOptions(ProgramOptionsDescription & options_description);

    virtual void processOptions(Poco::Util::LayeredConfiguration & config, po::variables_map & options) const = 0;

protected:
    void printHelpMessage() const;

    static String validatePathAndGetAsRelative(const String & path);

public:
    String command_name;
    String description;

protected:
    std::optional<ProgramOptionsDescription> command_option_description;
    String usage;
    po::positional_options_description positional_options_description;
};

using CommandPtr = std::unique_ptr<ICommand>;

}

DB::CommandPtr makeCommandCopy();
DB::CommandPtr makeCommandLink();
DB::CommandPtr makeCommandList();
DB::CommandPtr makeCommandListDisks();
DB::CommandPtr makeCommandMove();
DB::CommandPtr makeCommandRead();
DB::CommandPtr makeCommandRemove();
DB::CommandPtr makeCommandWrite();
DB::CommandPtr makeCommandMkDir();
DB::CommandPtr makeCommandPackedIO();
