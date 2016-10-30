#include "Server.h"
#include "LocalServer.h"
#include <common/ApplicationServerExt.h>
#include <DB/Common/StringUtils.h>

/// Universal executable for various clickhouse application

YANDEX_APP_SERVER_MAIN_FUNC(DB::Server, main_clickhouse_server);
YANDEX_APP_MAIN_FUNC(DB::LocalServer, main_clickhouse_local);

int main (int argc_, char * argv_[])
{
	std::vector<char *> argv(argv_, argv_ + argc_);
	auto main_func = main_clickhouse_server;

	/// Use local mode if app is run through symbolic link with name clickhouse-local
	if (!strcmp(argv[0], "clickhouse-local") || endsWith(argv[0], "/clickhouse-local"))
		main_func = main_clickhouse_local;

	/// Use local mode if --local-mode arg is passed (the arg should be quietly removed)
	auto it_mode_local = std::find_if(argv.begin(), argv.end(), [](char * arg) { return !strcmp(arg, "--mode-local"); } );
	if (it_mode_local != argv.end())
	{
		argv.erase(it_mode_local);
		main_func = main_clickhouse_local;
	}

	return main_func(static_cast<int>(argv.size()), argv.data());
}
