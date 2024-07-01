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
#include <Client/ClientBase.h>
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

class ClientBaseApplication : public ClientBase, public Poco::Util::Application, public IHints<2>
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

    Poco::Util::LayeredConfiguration & getClientConfiguration() override;

    static void setupSignalHandler();

    virtual void readArguments(
        int argc,
        char ** argv,
        Arguments & common_arguments,
        std::vector<Arguments> & external_tables_arguments,
        std::vector<Arguments> & hosts_and_ports_arguments) = 0;

    void addMultiquery(std::string_view query, Arguments & common_arguments) const;

private:
    void parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments);
};

}
