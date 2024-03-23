#pragma once

#include "Common/NamePrompter.h"
#include <Parsers/ASTCreateQuery.h>
#include <Common/ProgressIndication.h>
#include <Common/InterruptListener.h>
#include <Common/ShellCommand.h>
#include <Common/Stopwatch.h>
#include <Common/DNSResolver.h>
#include <Core/ExternalTable.h>
#include <Poco/Util/Application.h>
#include <Interpreters/Context.h>
#include <Client/ClientCore.h>
#include <Client/Suggest.h>
#include <Client/QueryFuzzer.h>
#include <boost/program_options.hpp>
#include <Storages/StorageFile.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace po = boost::program_options;


namespace DB
{


void interruptSignalHandler(int signum);

class InternalTextLogs;
class WriteBufferFromFileDescriptor;

class ClientBaseApplication : public ClientCore, public Poco::Util::Application, public IHints<2>
{

public:
    using Arguments = std::vector<String>;

    static ClientBaseApplication & getInstance()
    {
        return dynamic_cast<ClientBaseApplication&>(Poco::Util::Application::instance());
    }

    ClientBaseApplication();
    ~ClientBaseApplication() override;

    void init(int argc, char ** argv);

    std::vector<String> getAllRegisteredNames() const override { return cmd_options; }

protected:
    static void setupSignalHandler();
    using ProgramOptionsDescription = boost::program_options::options_description;
    using CommandLineOptions = boost::program_options::variables_map;

    struct OptionsDescription
    {
        std::optional<ProgramOptionsDescription> main_description;
        std::optional<ProgramOptionsDescription> external_description;
        std::optional<ProgramOptionsDescription> hosts_and_ports_description;
    };

    virtual void printHelpMessage(const OptionsDescription & options_description) = 0;
    virtual void addOptions(OptionsDescription & options_description) = 0;
    virtual void processOptions(const OptionsDescription & options_description,
                                const CommandLineOptions & options,
                                const std::vector<Arguments> & external_tables_arguments,
                                const std::vector<Arguments> & hosts_and_ports_arguments) = 0;
    virtual void processConfig() = 0;

    virtual void readArguments(
        int argc,
        char ** argv,
        Arguments & common_arguments,
        std::vector<Arguments> & external_tables_arguments,
        std::vector<Arguments> & hosts_and_ports_arguments) = 0;

    void addMultiquery(std::string_view query, Arguments & common_arguments) const;
    void initUserProvidedQueryIdFormats() override;

private:
    void parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments);
};

}
