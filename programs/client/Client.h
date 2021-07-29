#pragma once

#include <Client/ClientBase.h>
#include <Core/ExternalTable.h>


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
    std::unique_ptr<Connection> connection; /// Connection to DB.
    ConnectionParameters connection_parameters;

    /// The last exception that was received from the server. Is used for the
    /// return code in batch mode.
    std::unique_ptr<Exception> server_exception;
    /// Likewise, the last exception that occurred on the client.
    std::unique_ptr<Exception> client_exception;

    String format; /// Query results output format.
    bool is_default_format = true; /// false, if format is set in the config or command line.
    size_t format_max_block_size = 0; /// Max block size for console output.
    String insert_format; /// Format of INSERT data that is read from stdin in batch mode.
    size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.
    size_t max_client_network_bandwidth = 0; /// The maximum speed of data exchange over the network for the client in bytes per second.

    UInt64 server_revision = 0;
    String server_version;

    /// External tables info.
    std::list<ExternalTable> external_tables;

    /// Dictionary with query parameters for prepared statements.
    NameToNameMap query_parameters;
    QueryProcessingStage::Enum query_processing_stage;
    String current_profile;

    void connect();
    void printChangedSettings() const;
    void sendExternalTables(ASTPtr parsed_query);

    void processInsertQuery(const String & query_to_execute, ASTPtr parsed_query);
    void processOrdinaryQuery(const String & query_to_execute, ASTPtr parsed_query);

    void sendData(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query);
    void sendDataFrom(ReadBuffer & buf, Block & sample,
                      const ColumnsDescription & columns_description, ASTPtr parsed_query);

    void receiveResult(ASTPtr parsed_query);
    void receiveLogs(ASTPtr parsed_query);
    bool receiveEndOfQuery();
    bool receiveAndProcessPacket(ASTPtr parsed_query, bool cancelled);
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query);

    void initBlockOutputStream(const Block & block, ASTPtr parsed_query);
    void initLogsOutputStream();

    void onData(Block & block, ASTPtr parsed_query);
    void onLogData(Block & block);
    void onTotals(Block & block, ASTPtr parsed_query);
    void onExtremes(Block & block, ASTPtr parsed_query);
    void onProgress(const Progress & value);

    void writeFinalProgress();
    void onReceiveExceptionFromServer(std::unique_ptr<Exception> && e);
    void onProfileInfo(const BlockStreamProfileInfo & profile_info);
    void onEndOfStream();

    std::vector<String> loadWarningMessages();
    void reconnectIfNeeded()
    {
        if (!connection->checkConnected())
            connect();
    }

};
}
