#include "ConfigReloader.h"

#include <Poco/Util/Application.h>
#include <Poco/File.h>

#include <common/logger_useful.h>

#include <DB/Interpreters/Context.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/ConfigProcessor.h>


namespace DB
{

namespace ErrorCodes { extern const int FILE_DOESNT_EXIST; }


ConfigReloader::ConfigReloader(const std::string & main_config_path_, const std::string & users_config_path_, const std::string & include_from_path_, Context * context_)
	: main_config_path(main_config_path_), users_config_path(users_config_path_), include_from_path(include_from_path_), context(context_)
{
	/// Assume that paths derived from --config-file, <users_config> and <include_from> are not changed

	/// Если путь к конфигу не абсолютный, угадаем, относительно чего он задан.
    /// Сначала поищем его рядом с основным конфигом, потом - в текущей директории.
	if (users_config_path.empty() || users_config_path[0] != '/')
	{
		std::string config_dir = Poco::Path(main_config_path).parent().toString();
        if (Poco::File(config_dir + users_config_path).exists())
			users_config_path = config_dir + users_config_path;
	}

	/// Setup users on server init
	reloadIfNewer(false, true);

	thread = std::thread(&ConfigReloader::run, this);
}

ConfigReloader::~ConfigReloader()
{
	try
	{
		LOG_DEBUG(log, "ConfigReloader::~ConfigReloader()");
		quit = true;
		thread.join();
	}
	catch (...)
	{
		tryLogCurrentException("~ConfigReloader");
	}
}


void ConfigReloader::run()
{
	setThreadName("UserConfReload");

	while (!quit)
	{
		std::this_thread::sleep_for(std::chrono::seconds(2));
		reloadIfNewer(false, false);
	}
}


ConfigReloader::FilesChangesTracker ConfigReloader::getFileListFor(const std::string & root_config_path)
{
	FilesChangesTracker file_list;

	file_list.addIfExists(root_config_path);
	file_list.addIfExists(this->include_from_path);

	for (auto & path : ConfigProcessor::getConfigMergeFiles(root_config_path))
		file_list.addIfExists(path);

 	return file_list;
}


ConfigurationPtr ConfigReloader::loadConfigFor(const std::string & root_config_path, bool throw_on_error)
{
	ConfigurationPtr config;

	LOG_DEBUG(log, "Loading config '" << root_config_path << "'");

	try
	{
		config = ConfigProcessor().loadConfig(root_config_path);
	}
	catch (Poco::Exception & e)
	{
		if (throw_on_error)
			throw;

		LOG_ERROR(log, "Error loading config from '" << root_config_path << "' : " << e.what() << ": " << e.displayText());
		return nullptr;
	}
	catch (...)
	{
		if (throw_on_error)
			throw;

		LOG_ERROR(log, "Error loading config from '" << root_config_path << "'.");
		return nullptr;
	}

	return config;
}


bool ConfigReloader::applyConfigFor(bool for_main, ConfigurationPtr config, bool throw_on_error)
{
	auto & root_config_path = (for_main) ? main_config_path : users_config_path;

	try
	{
		if (for_main)
		{
			context->setClustersConfig(config);
		}
		else
		{
			context->setUsersConfig(config);
		}
	}
	catch (Exception & e)
	{
		if (throw_on_error)
			throw;

		LOG_ERROR(log, "Error updating config from '" << root_config_path << "': " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString());
		return false;
	}
	catch (Poco::Exception & e)
	{
		if (throw_on_error)
			throw;

		LOG_ERROR(log, "Error updating config from '" << root_config_path << "': " <<  e.what() << ": " << e.displayText());
		return false;
	}
	catch (...)
	{
		if (throw_on_error)
			throw;

		LOG_ERROR(log, "Error updating config from '" << root_config_path << "'.");
		return false;
	}

	return true;
}


void ConfigReloader::reloadIfNewer(bool force_main, bool force_users)
{
	FilesChangesTracker main_config_files = getFileListFor(main_config_path);
	if (force_main || main_config_files.isDifferOrNewerThan(last_main_config_files))
	{
		last_main_config_files = std::move(main_config_files);

		ConfigurationPtr config = loadConfigFor(main_config_path, force_main);
		if (config)
			applyConfigFor(true, config, force_main);
	}

	FilesChangesTracker users_config_files = getFileListFor(users_config_path);
	if (force_users || users_config_files.isDifferOrNewerThan(last_users_config_files))
	{
		last_users_config_files = std::move(users_config_files);

		ConfigurationPtr config = loadConfigFor(users_config_path, force_users);
		if (config)
			applyConfigFor(false, config, force_users);
	}
}


}
