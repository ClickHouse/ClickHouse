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


constexpr decltype(ConfigReloader::reload_interval) ConfigReloader::reload_interval;


ConfigReloader::ConfigReloader(const std::string & main_config_path_, const std::string & users_config_path_,
							   const std::string & include_from_path_, Context * context_)
	: main_config_path(main_config_path_), users_config_path(users_config_path_),
	include_from_path(include_from_path_), context(context_)
{
	/// If path to users' config isn't absolute, try guess its root (current) dir.
	/// At first, try to find it in dir of main config, after will use current dir.
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
		{
			std::lock_guard<std::mutex> lock{mutex};
			quit = true;
		}

		cond.notify_one();
		thread.join();
	}
	catch (...)
	{
		DB::tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


void ConfigReloader::run()
{
	setThreadName("ConfigReloader");

	std::unique_lock<std::mutex> lock{mutex};

	while (true)
	{
		if (cond.wait_for(lock, reload_interval, [this] { return quit; }))
			break;

		reloadIfNewer(false, false);
	}
}


ConfigReloader::FilesChangesTracker ConfigReloader::getFileListFor(const std::string & root_config_path)
{
	FilesChangesTracker file_list;

	file_list.addIfExists(root_config_path);
	file_list.addIfExists(include_from_path);

	for (const auto & path : ConfigProcessor::getConfigMergeFiles(root_config_path))
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
	catch (...)
	{
		if (throw_on_error)
			throw;

		tryLogCurrentException(log, "Error loading config from '" + root_config_path + "' ");
		return nullptr;
	}

	return config;
}


void ConfigReloader::reloadIfNewer(bool force_main, bool force_users)
{
	FilesChangesTracker main_config_files = getFileListFor(main_config_path);
	if (force_main || main_config_files.isDifferOrNewerThan(last_main_config_files))
	{
		ConfigurationPtr config = loadConfigFor(main_config_path, force_main);
		if (config)
		{
			/** We should remember last modification time if and only if config was sucessfully loaded
			  * Otherwise a race condition could occur during config files update:
			  *  File is contain raw (and non-valid) data, therefore config is not applied.
			  *  When file has been written (and contain valid data), we don't load new data since modification time remains the same.
			  */
			last_main_config_files = std::move(main_config_files);

			try
			{
				context->setClustersConfig(config);
			}
			catch (...)
			{
				if (force_main)
					throw;
				tryLogCurrentException(log, "Error updating remote_servers config from '" + main_config_path + "' ");
			}
		}
	}

	FilesChangesTracker users_config_files = getFileListFor(users_config_path);
	if (force_users || users_config_files.isDifferOrNewerThan(last_users_config_files))
	{
		ConfigurationPtr config = loadConfigFor(users_config_path, force_users);
		if (config)
		{
			last_users_config_files = std::move(users_config_files);

			try
			{
				context->setUsersConfig(config);
			}
			catch (...)
			{
				if (force_users)
					throw;
				tryLogCurrentException(log, "Error updating users config from '" + users_config_path + "' ");
			}
		}
	}
}


}
