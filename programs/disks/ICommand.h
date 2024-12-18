#pragma once

#include <optional>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>

#include <boost/any/bad_any_cast.hpp>
#include <boost/program_options.hpp>

#include <Poco/Util/Application.h>
#include "Common/Exception.h"
#include <Common/Config/ConfigProcessor.h>

#include <boost/program_options/positional_options.hpp>

#include "DisksApp.h"

#include "DisksClient.h"

#include "ICommand_fwd.h"

namespace DB
{

namespace po = boost::program_options;
using ProgramOptionsDescription = po::options_description;
using PositionalProgramOptionsDescription = po::positional_options_description;
using CommandLineOptions = po::variables_map;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ICommand
{
public:
    explicit ICommand() = default;

    virtual ~ICommand() = default;

    void execute(const Strings & commands, DisksClient & client);

    virtual void executeImpl(const CommandLineOptions & options, DisksClient & client) = 0;

    CommandLineOptions processCommandLineArguments(const Strings & commands);

protected:
    template <typename T>
    static T getValueFromCommandLineOptions(const CommandLineOptions & options, const String & name)
    {
        try
        {
            return options[name].as<T>();
        }
        catch (boost::bad_any_cast &)
        {
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' has wrong type and can't be parsed", name);
        }
    }

    template <typename T>
    static T getValueFromCommandLineOptionsThrow(const CommandLineOptions & options, const String & name)
    {
        if (options.count(name))
            return getValueFromCommandLineOptions<T>(options, name);

        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Mandatory argument '{}' is missing", name);
    }

    template <typename T>
    static T getValueFromCommandLineOptionsWithDefault(const CommandLineOptions & options, const String & name, const T & default_value)
    {
        if (options.count(name))
            return getValueFromCommandLineOptions<T>(options, name);

        return default_value;
    }

    template <typename T>
    static std::optional<T> getValueFromCommandLineOptionsWithOptional(const CommandLineOptions & options, const String & name)
    {
        if (options.count(name))
            return std::optional{getValueFromCommandLineOptions<T>(options, name)};

        return std::nullopt;
    }

    DiskWithPath & getDiskWithPath(DisksClient & client, const CommandLineOptions & options, const String & name);

    String getTargetLocation(const String & path_from, DiskWithPath & disk_to, const String & path_to)
    {
        if (!disk_to.getDisk()->existsDirectory(path_to))
        {
            return path_to;
        }
        String copied_path_from = path_from;
        if (copied_path_from.ends_with('/'))
        {
            copied_path_from.pop_back();
        }
        String plain_filename = fs::path(copied_path_from).filename();

        return fs::path{path_to} / plain_filename;
    }


public:
    String command_name;
    String description;
    ProgramOptionsDescription options_description;

protected:
    PositionalProgramOptionsDescription positional_options_description;
};

DB::CommandPtr makeCommandCopy();
DB::CommandPtr makeCommandListDisks();
DB::CommandPtr makeCommandList();
DB::CommandPtr makeCommandChangeDirectory();
DB::CommandPtr makeCommandLink();
DB::CommandPtr makeCommandMove();
DB::CommandPtr makeCommandRead();
DB::CommandPtr makeCommandRemove();
DB::CommandPtr makeCommandWrite();
DB::CommandPtr makeCommandMkDir();
DB::CommandPtr makeCommandSwitchDisk();
DB::CommandPtr makeCommandGetCurrentDiskAndPath();
DB::CommandPtr makeCommandHelp(const DisksApp & disks_app);
DB::CommandPtr makeCommandTouch();
#ifdef CLICKHOUSE_CLOUD
DB::CommandPtr makeCommandPackedIO();
#endif
}
