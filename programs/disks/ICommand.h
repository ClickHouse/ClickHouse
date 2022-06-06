#pragma once

#include <Disks/IDisk.h>
#include <Poco/Util/Application.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <boost/program_options.hpp>
#include <Common/TerminalSize.h>
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
    void execute(
        const std::vector<String> & command_arguments,
        const DB::ContextMutablePtr & global_context,
        Poco::Util::AbstractConfiguration & config,
        po::variables_map & options);

protected:
    virtual void processOptions(
        Poco::Util::AbstractConfiguration & config,
        po::variables_map & options) const = 0;

    virtual void executeImpl(
        const DB::ContextMutablePtr & global_context,
        const Poco::Util::AbstractConfiguration & config) const = 0;

    void printHelpMessage() const;

    static String fullPathWithValidate(const DiskPtr & disk, const String & path);

public:
    String command_name;
    String description;

protected:
    std::optional<ProgramOptionsDescription> command_option_description;
    String usage;
    po::positional_options_description positional_options_description;
    std::vector<String> pos_arguments;
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
