#pragma once

#include <Client/ClientBase.h>
#include <Client/LocalConnection.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Loggers/Loggers.h>
#include <Common/InterruptListener.h>
#include <Common/StatusFile.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/Config/ConfigProcessor.h>


#include <Poco/Util/LayeredConfiguration.h>
#include <filesystem>
#include <memory>
#include <optional>


namespace DB
{

// Client class which can be run embedded into server
class ClientEmbedded : public ClientBase
{
public:
    explicit ClientEmbedded(
        std::unique_ptr<Session> && session_,
        int in_fd_,
        int out_fd_,
        int err_fd_,
        std::istream & input_stream_,
        std::ostream & output_stream_,
        std::ostream & error_stream_)
        : ClientBase(in_fd_, out_fd_, err_fd_, input_stream_, output_stream_, error_stream_), session(std::move(session_))
    {
        global_context = session->makeSessionContext();
        configuration = ConfigHelper::createEmpty();
        layered_configuration = new Poco::Util::LayeredConfiguration();
        layered_configuration->add(configuration);
    }

    int run(const NameToNameMap & envVars, const String & first_query);

    ~ClientEmbedded() override { cleanup(); }

protected:
    void connect() override;

    Poco::Util::LayeredConfiguration & getClientConfiguration() override;

    void processError(const String & query) const override;

    String getName() const override { return "embedded"; }

private:
    void cleanup();

    std::unique_ptr<Session> session;

    ConfigurationPtr configuration;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> layered_configuration;
};

}
