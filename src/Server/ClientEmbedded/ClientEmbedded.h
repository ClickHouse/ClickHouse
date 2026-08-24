#pragma once

#if defined(OS_LINUX)

#include <Client/ClientBase.h>
#include <Client/LocalConnection.h>

#include <Common/Config/ConfigProcessor.h>


#include <Poco/Util/LayeredConfiguration.h>
#include <memory>


namespace DB
{

// Client class which can be run embedded into server
class ClientEmbedded final : public ClientBase
{
public:
    explicit ClientEmbedded(
        std::unique_ptr<Session> && session_,
        int in_fd_,
        int out_fd_,
        int err_fd_,
        std::istream & input_stream_,
        std::ostream & output_stream_,
        std::ostream & error_stream_);

    [[ nodiscard ]] int run(const NameToNameMap & envVars, const String & first_query);

    /// NOP. The embedded client runs inside the server process which has its own signal handlers.
    /// Thus we cannot override it in any way.
    void setupSignalHandler() override {}

    ~ClientEmbedded() override { cleanup(); }

protected:
    void connect() override;

    Poco::Util::LayeredConfiguration & getClientConfiguration() override;

    void processError(std::string_view query) const override;

    String getName() const override { return "embedded"; }

    void printHelpMessage(const OptionsDescription &) override;
    void addExtraOptions(OptionsDescription &) override {}
    void processOptions(const OptionsDescription &,
                        const CommandLineOptions &,
                        const std::vector<Arguments> &,
                        const std::vector<Arguments> &) override {}
    void processConfig() override {}
    bool isEmbeeddedClient() const override;

private:
    void cleanup();

    std::unique_ptr<Session> session;

    ConfigurationPtr configuration;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> layered_configuration;
};

}

#endif
