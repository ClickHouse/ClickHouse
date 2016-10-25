#include "Server.h"
#include "LocalServer.h"
#include <common/ApplicationServerExt.h>

/// Universal executable for various clickhouse application

YANDEX_APP_SERVER_MAIN_FUNC(DB::Server, main_clickhouse_server);
YANDEX_APP_MAIN_FUNC(DB::LocalServer, main_clickhouse_local);

int main (int argc_, char * argv_[])
{
	if (argc_ > 1 && !strcmp(argv_[1], "--local-mode"))
	{
		/// Cut first argument
		int argc = argc_ - 1;
		std::vector<char *> argv(argc);
		argv[0] = argv_[0];
		for (int i_arg = 2; i_arg < argc_; ++i_arg)
			argv[i_arg - 1] = argv_[i_arg];

		main_clickhouse_local(argc, argv.data());
	}
	else
	{
		main_clickhouse_server(argc_, argv_);
	}
}

