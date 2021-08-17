#pragma once

#include <Client/ClientBase.h>


namespace DB
{

class Client : public ClientBase
{
public:
    Client() = default;

    void initialize(Poco::Util::Application & self) override;

protected:
    void processSingleQuery(const String & full_query) override;

    bool processMultiQuery(const String & all_queries_text) override;

    bool processWithFuzzing(const String & full_query) override;


    void reportQueryError(const String & query) const override;

    void executeSingleQuery(const String & query_to_execute, ASTPtr parsed_query) override;

    void loadSuggestionData(Suggest & suggest) override;


    int mainImpl() override;

    void readArguments(int argc, char ** argv,
                       Arguments & common_arguments,
                       std::vector<Arguments> & external_tables_arguments) override;

    void printHelpMessage(const OptionsDescription & options_description) override;

    void addAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments) override;

    void processOptions(const OptionsDescription & options_description,
                        const CommandLineOptions & options,
                        const std::vector<Arguments> & external_tables_arguments) override;
void processConfig() override;

private:
    size_t format_max_block_size = 0; /// Max block size for console output.
    String insert_format; /// Format of INSERT data that is read from stdin in batch mode.
    size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.
    size_t max_client_network_bandwidth = 0; /// The maximum speed of data exchange over the network for the client in bytes per second.

    UInt64 server_revision = 0;
    String server_version;

    String current_profile;

    void connect() override;
    void printChangedSettings() const;

    void processInsertQuery(const String & query_to_execute, ASTPtr parsed_query);

    void sendData(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query);
    void sendDataFrom(ReadBuffer & buf, Block & sample,
                      const ColumnsDescription & columns_description, ASTPtr parsed_query);

    void receiveLogs(ASTPtr parsed_query);
    bool receiveEndOfQuery();
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query);

    void writeFinalProgress();

    std::vector<String> loadWarningMessages();
    void reconnectIfNeeded()
    {
        if (!connection->checkConnected())
            connect();
    }

};
}
