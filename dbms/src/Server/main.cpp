#include "Server.h"
#include "LocalServer.h"
#include <DB/Common/StringUtils.h>

/// Universal executable for various clickhouse applications
int main_clickhouse_server(int argc, char ** argv);
int main_clickhouse_client(int argc, char ** argv);
int main_clickhouse_local(int argc, char ** argv);


static bool is_clickhouse_app(const std::string & app_suffix, std::vector<char *> & argv)
{
	std::string arg_mode_app = "--" + app_suffix;

	/// Use local mode if --local arg is passed (the arg should be quietly removed)
	auto arg_it = std::find_if(argv.begin(), argv.end(), [&](char * arg) { return !arg_mode_app.compare(arg); } );
	if (arg_it != argv.end())
	{
		argv.erase(arg_it);
		return true;
	}

	std::string app_name = "clickhouse-" + app_suffix;

	/// Use local mode if app is run through symbolic link with name clickhouse-local
	if (!app_name.compare(argv[0]) || endsWith(argv[0], "/" + app_name))
		return true;

	return false;
}


int main(int argc_, char ** argv_)
{
	std::vector<char *> argv(argv_, argv_ + argc_);

	auto main_func = main_clickhouse_server;

	if (is_clickhouse_app("local", argv))
		main_func = main_clickhouse_local;
	else if (is_clickhouse_app("client", argv))
		main_func = main_clickhouse_client;
	else if (is_clickhouse_app("server", argv)) /// --server arg should be cut
		main_func = main_clickhouse_server;

	return main_func(static_cast<int>(argv.size()), argv.data());
}
