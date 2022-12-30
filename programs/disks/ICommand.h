#pragma once

#include <Disks/IDisk.h>

#include <Poco/Util/Application.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/copyData.h>

#include <boost/program_options.hpp>

#include <Common/TerminalSize.h>
#include <Common/Config/ConfigProcessor.h>

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
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration & config) = 0;

    const std::optional<ProgramOptionsDescription> & getCommandOptions() const { return command_option_description; }

    void addOptions(ProgramOptionsDescription & options_description);

    virtual void processOptions(Poco::Util::LayeredConfiguration & config, po::variables_map & options) const = 0;

protected:
    void printHelpMessage() const;

    static String fullPathWithValidate(const DiskPtr & disk, const String & path);

public:
    String command_name;
    String description;

protected:
    std::optional<ProgramOptionsDescription> command_option_description;
    String usage;
    po::positional_options_description positional_options_description;
};

}

std::unique_ptr <DB::ICommand> makeCommandCopy();
std::unique_ptr <DB::ICommand> makeCommandLink();
std::unique_ptr <DB::ICommand> makeCommandList();
std::unique_ptr <DB::ICommand> makeCommandListDisks();
std::unique_ptr <DB::ICommand> makeCommandMove();
std::unique_ptr <DB::ICommand> makeCommandRead();
std::unique_ptr <DB::ICommand> makeCommandRemove();
std::unique_ptr <DB::ICommand> makeCommandWrite();
