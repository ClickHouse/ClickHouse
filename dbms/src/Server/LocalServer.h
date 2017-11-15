#pragma once

#include <Poco/Util/Application.h>
#include <memory>

namespace DB
{

class Context;

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public Poco::Util::Application
{
public:

    LocalServer();

    void initialize(Poco::Util::Application & self) override;

    void defineOptions(Poco::Util::OptionSet& _options) override;

    int main(const std::vector<std::string> & args) override;

    ~LocalServer();

private:

    /** Composes CREATE subquery based on passed arguments (--structure --file --table and --input-format)
      * This query will be executed first, before queries passed through --query argument
      * Returns empty string if it cannot compose that query.
      */
    std::string getInitialCreateTableQuery();

    void tryInitPath();
    void applyOptions();
    void attachSystemTables();
    void processQueries();
    void setupUsers();
    void displayHelp();
    void handleHelp(const std::string & name, const std::string & value);

protected:

    std::unique_ptr<Context> context;
    std::string path;
};

}
