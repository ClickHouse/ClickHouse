#pragma once

#include <Client/ClientApplicationBase.h>


namespace BuzzHouse
{
    class FuzzConfig;
    class ExternalIntegrations;
};

namespace DB
{

class Client : public ClientApplicationBase
{
public:
    using Arguments = ClientApplicationBase::Arguments;

    Client();
    ~Client() override;

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & /*args*/) override;

protected:
    Poco::Util::LayeredConfiguration & getClientConfiguration() override;

    bool processWithFuzzing(const String & full_query) override;
    bool buzzHouse() override;
    std::optional<bool> processFuzzingStep(const String & query_to_execute, const ASTPtr & parsed_query, bool permissive);

    void connect() override;

    void processError(const String & query) const override;

    String getName() const override { return "client"; }

    void printHelpMessage(const OptionsDescription & options_description) override;

    void addExtraOptions(OptionsDescription & options_description) override;

    void processOptions(
        const OptionsDescription & options_description,
        const CommandLineOptions & options,
        const std::vector<Arguments> & external_tables_arguments,
        const std::vector<Arguments> & hosts_and_ports_arguments) override;

    void processConfig() override;

    void readArguments(
        int argc,
        char ** argv,
        Arguments & common_arguments,
        std::vector<Arguments> & external_tables_arguments,
        std::vector<Arguments> & hosts_and_ports_arguments) override;

private:
    void printChangedSettings() const;
    void showWarnings();
#if USE_BUZZHOUSE
    std::unique_ptr<BuzzHouse::FuzzConfig> fuzz_config;
    std::unique_ptr<BuzzHouse::ExternalIntegrations> external_integrations;

    bool logAndProcessQuery(std::ofstream & outf, const String & full_query);
    bool processBuzzHouseQuery(const String & full_query);
#endif
    void parseConnectionsCredentials(Poco::Util::AbstractConfiguration & config, const std::string & connection_name);
    std::vector<String> loadWarningMessages();
};
}
