#pragma once

#include <DB/Common/ConfigProcessor.h>

#include <time.h>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <list>


namespace Poco { class Logger; }

namespace DB
{

class Context;

/** Every two seconds checks configuration files for update.
  * If configuration is changed, then config will be reloaded by ConfigProcessor
  *  and the reloaded config will be applied via setUsersConfig() and setClusters() methods of Context.
  * So, ConfigReloader actually reloads only <users> and <remote_servers> "tags".
  * Also, it doesn't take into account changes of --config-file, <users_config> and <include_from> parameters.
  */
class ConfigReloader
{
public:
	/** main_config_path is usually /path/to/.../clickhouse-server/config.xml (i.e. --config-file value)
	  * users_config_path is usually /path/to/.../clickhouse-server/users.xml (i.e. value of <users_config> tag)
	  * include_from_path is usually /path/to/.../etc/metrika.xml (i.e. value of <include_from> tag)
	  */
	ConfigReloader(const std::string & main_config_path_, const std::string & users_config_path_, const std::string & include_from_path_, Context * context_);

	~ConfigReloader();

private:

	struct FileWithTimestamp
	{
		std::string path;
		time_t modification_time;

		FileWithTimestamp(const std::string & path_, time_t modification_time_)
			: path(path_), modification_time(modification_time_) {}

		bool operator < (const FileWithTimestamp & rhs) const
		{
			return path < rhs.path;
		}

		static bool isTheSame(const FileWithTimestamp & lhs, const FileWithTimestamp & rhs)
		{
			return (lhs.modification_time == rhs.modification_time) && (lhs.path == rhs.path);
		}
	};

	struct FilesChangesTracker
	{
		std::set<FileWithTimestamp> files;

		void addIfExists(const std::string & path)
		{
			if (!path.empty() && Poco::File(path).exists())
			{
				files.emplace(path, Poco::File(path).getLastModified().epochTime());
			}
		}

		bool isDifferOrNewerThan(const FilesChangesTracker & rhs)
		{
			return (files.size() != rhs.files.size()) ||
					!std::equal(files.begin(), files.end(), rhs.files.begin(), FileWithTimestamp::isTheSame);
		}
	};

private:

	/// Make sense to separate this function on two threads
	void reloadIfNewer(bool force_main, bool force_users);
	void run();

	FilesChangesTracker getFileListFor(const std::string & root_config_path);
	ConfigurationPtr loadConfigFor(const std::string & root_config_path, bool throw_error);

private:

	static constexpr auto reload_interval = std::chrono::seconds(2);

	std::string main_config_path;
	std::string users_config_path;
	std::string include_from_path;

	Context * context;

	FilesChangesTracker last_main_config_files;
	FilesChangesTracker last_users_config_files;

	bool quit {false};
	std::mutex mutex;
	std::condition_variable cond;
	std::thread thread;

	Poco::Logger * log = &Logger::get("ConfigReloader");
};

}
