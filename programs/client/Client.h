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
    void processSingleQuery(const String & query_to_execute, ASTPtr parsed_query) override;
    bool processMultiQuery(const String & all_queries_text) override;
    bool processWithFuzzing(const String & full_query) override;

    void processError(const String & query) const override;
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
    void connect() override;

    void printChangedSettings() const;

    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query);

    std::vector<String> loadWarningMessages();
};
}
