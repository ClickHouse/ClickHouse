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

	LocalServer() = default;

	void initialize(Poco::Util::Application & self) override;

	void defineOptions(Poco::Util::OptionSet& _options) override;

	int main(const std::vector<std::string> & args) override;

	~LocalServer() = default;

private:

	std::string getInitialCreateTableQuery();

	void applyOptions();

	void attachSystemTables();

	void processQueries();

	void setupUsers();

	void displayHelp();

	void handleHelp(const std::string & name, const std::string & value);

	static const char * default_user_xml;

protected:

	std::unique_ptr<Context> context;
};

}
