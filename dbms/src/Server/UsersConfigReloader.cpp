#include "UsersConfigReloader.h"

#include <Poco/Util/Application.h>
#include <Poco/File.h>

#include <common/logger_useful.h>

#include <DB/Interpreters/Context.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/ConfigProcessor.h>


namespace DB
{

namespace ErrorCodes { extern const int FILE_DOESNT_EXIST; }


UsersConfigReloader::UsersConfigReloader(const std::string & path_, Context * context_)
	: path(path_), context(context_), file_modification_time(0), quit(false), log(&Logger::get("UsersConfigReloader"))
{
	/// Если путь к конфигу не абсолютный, угадаем, относительно чего он задан.
	/// Сначала поищем его рядом с основным конфигом, потом - в текущей директории.
	if (path.empty() || path[0] != '/')
	{
		std::string main_config_path = Poco::Util::Application::instance().config().getString("config-file", "config.xml");
		std::string config_dir = Poco::Path(main_config_path).parent().toString();
		if (Poco::File(config_dir + path).exists())
			path = config_dir + path;
	}

	reloadIfNewer(true);
	thread = std::thread(&UsersConfigReloader::run, this);
}


UsersConfigReloader::~UsersConfigReloader()
{
	try
	{
		quit = true;
		thread.join();
	}
	catch (...)
	{
		tryLogCurrentException("~UsersConfigReloader");
	}
}


void UsersConfigReloader::run()
{
	setThreadName("UserConfReload");

	while (!quit)
	{
		std::this_thread::sleep_for(std::chrono::seconds(2));
		reloadIfNewer(false);
	}
}


void UsersConfigReloader::reloadIfNewer(bool force)
{
	Poco::File f(path);
	if (!f.exists())
	{
		if (force)
			throw Exception("Users config not found at: " + path, ErrorCodes::FILE_DOESNT_EXIST);
		if (file_modification_time)
		{
			LOG_ERROR(log, "Users config not found at: " << path);
			file_modification_time = 0;
		}
		return;
	}
	time_t new_modification_time = f.getLastModified().epochTime();
	if (!force && new_modification_time == file_modification_time)
		return;
	file_modification_time = new_modification_time;

	LOG_DEBUG(log, "Loading users config");

	ConfigurationPtr config;

	try
	{
		config = ConfigProcessor(!force).loadConfig(path);
	}
	catch (Poco::Exception & e)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error loading users config: " << e.what() << ": " << e.displayText());
		return;
	}
	catch (...)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error loading users config.");
		return;
	}

	try
	{
		context->setUsersConfig(config);
	}
	catch (Exception & e)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error updating users config: " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString());
	}
	catch (Poco::Exception & e)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error updating users config: " << e.what() << ": " << e.displayText());
	}
	catch (...)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error updating users config.");
	}
}


}
