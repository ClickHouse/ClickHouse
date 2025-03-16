#pragma once


#include <Poco/Util/Application.h>
#include <Client/ClientBase.h>
#include <Client/Suggest.h>
#include <Common/NamePrompter.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/SimpleFileChannel.h>
#include <Poco/SplitterChannel.h>

#include <boost/program_options.hpp>

#include <vector>

namespace po = boost::program_options;

namespace DB
{

void interruptSignalHandler(int signum);

/**
 * The base class for client appliucations such as
 * clickhouse-client or clickhouse-local.
 * The main purpose and responsibility of it is dealing with
 * application-specific stuff such as command line arguments parsing
 * and setting up signal handlers, so queries will be cancelled after
 * Ctrl+C is pressed.
 */
class ClientApplicationBase : public ClientBase, public Poco::Util::Application, public IHints<2>
{
public:
    using ClientBase::processOptions;
    using Arguments = ClientBase::Arguments;

    static ClientApplicationBase & getInstance();

    ClientApplicationBase();
    ~ClientApplicationBase() override;

    void init(int argc, char ** argv);
    std::vector<String> getAllRegisteredNames() const override { return cmd_options; }

protected:
    Poco::Util::LayeredConfiguration & getClientConfiguration() override;
    void setupSignalHandler() override;
    void addMultiquery(std::string_view query, Arguments & common_arguments) const;

private:
    void parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments);

    std::vector<String> cmd_options;

    LoggerPtr fatal_log;
    Poco::AutoPtr<Poco::SplitterChannel> fatal_channel_ptr;
    Poco::AutoPtr<Poco::Channel> fatal_console_channel_ptr;
    Poco::AutoPtr<Poco::Channel> fatal_file_channel_ptr;
    Poco::Thread signal_listener_thread;
    std::unique_ptr<Poco::Runnable> signal_listener;
};


}
