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
    int mainImpl() override;

    bool supportPasswordOption() const override { return true; }

    void reconnectIfNeeded() override
    {
        if (!connection->checkConnected())
            connect();
    }

    bool processMultiQueryFromFile(const String & file) override
    {
        connection->setDefaultDatabase(connection_parameters.default_database);
        String text;
        ReadBufferFromFile in(file);
        readStringUntilEOF(text, in);
        return processMultiQuery(text);
    }

    std::vector<String> loadWarningMessages();

    void loadSuggestionDataIfPossible() override;

    bool checkErrorMatchesHints(const TestHint & test_hint, bool had_error) override;
    void reportQueryError() const override;

    bool processWithFuzzing(const String & text) override;

    void executeParsedQueryPrefix() override;
    void executeParsedQueryImpl() override;
    void executeParsedQuerySuffix() override;

    void readArguments(int argc, char ** argv,
                       Arguments & common_arguments,
                       std::vector<Arguments> & external_tables_arguments) override;
    void printHelpMessage(const OptionsDescription & options_description) override;
    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description,
                        const CommandLineOptions & options,
                        const std::vector<Arguments> & external_tables_arguments) override;

    void processConfig() override;

private:
    std::unique_ptr<Connection> connection; /// Connection to DB.

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

    ConnectionParameters connection_parameters;

    void connect();
    void printChangedSettings() const;
    void sendExternalTables();

    void executeInsertQuery();
    void executeOrdinaryQuery();

    void sendData(Block & sample, const ColumnsDescription & columns_description);
    void sendDataFrom(ReadBuffer & buf, Block & sample,
                      const ColumnsDescription & columns_description);

    void receiveResult();
    void receiveLogs();
    bool receiveEndOfQuery();
    bool receiveAndProcessPacket(bool cancelled);
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description);

    void initBlockOutputStream(const Block & block);
    void initLogsOutputStream();

    void onData(Block & block);
    void onLogData(Block & block);
    void onTotals(Block & block);
    void onExtremes(Block & block);
    void onProgress(const Progress & value);

    void writeFinalProgress();
    void onReceiveExceptionFromServer(std::unique_ptr<Exception> && e);
    void onProfileInfo(const BlockStreamProfileInfo & profile_info);
    void onEndOfStream();
};
}
