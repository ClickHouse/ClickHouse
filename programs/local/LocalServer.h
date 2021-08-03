#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <loggers/Loggers.h>
#include <Poco/Util/Application.h>
#include <Common/ProgressBar.h>

namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public Poco::Util::Application, public Loggers
{
public:
    LocalServer();

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<std::string> & args) override;

    void init(int argc, char ** argv);

    ~LocalServer() override;

private:
    /** Composes CREATE subquery based on passed arguments (--structure --file --table and --input-format)
      * This query will be executed first, before queries passed through --query argument
      * Returns empty string if it cannot compose that query.
      */
    std::string getInitialCreateTableQuery();

    void tryInitPath();
    void applyCmdOptions(ContextPtr context);
    void applyCmdSettings(ContextPtr context);
    void processQueries();
    void setupUsers();
    void cleanup();


protected:
    SharedContextHolder shared_context;
    ContextPtr global_context;

    /// Settings specified via command line args
    Settings cmd_settings;
    ProgressBar progress_bar;
    Progress progress;
    Stopwatch watch;

    std::optional<std::filesystem::path> temporary_directory_to_delete;
};

}
