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

protected:

	void processQueries();

	void setupUsers();

	std::unique_ptr<Context> context;
};

}
