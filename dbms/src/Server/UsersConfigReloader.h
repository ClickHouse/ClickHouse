#pragma once

#include <DB/Common/ConfigProcessor.h>

#include <time.h>
#include <string>
#include <thread>
#include <atomic>
#include <list>


namespace Poco { class Logger; }

namespace DB
{

class Context;

/** Каждые две секунды проверяет, не изменился ли конфиг.
  *  Когда изменился, запускает на нем ConfigProcessor и вызывает setUsersConfig у контекста.
  * NOTE: Не перезагружает конфиг, если изменились другие файлы, влияющие на обработку конфига: metrika.xml
  *  и содержимое conf.d и users.d. Это можно исправить, переместив проверку времени изменения файлов в ConfigProcessor.
  */
class UsersConfigReloader
{
public:
	UsersConfigReloader(const std::string & path, Context * context);
	UsersConfigReloader(const std::string & main_config_path_, const std::string & users_config_path_, const std::string & include_from_path_, Context * context_);

	~UsersConfigReloader();

private:

	struct FileWithTimestamp
	{
		std::string path;
		time_t modification_time;

		FileWithTimestamp(const std::string & path_,  time_t modification_time_)
			: path(path_), modification_time(modification_time_) {}

		bool operator < (const FileWithTimestamp & rhs) const
		{
			return path < rhs.path;
		}

		bool operator == (const FileWithTimestamp & rhs) const
		{
			return (modification_time == rhs.modification_time) && (path == rhs.path);
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
			return files != rhs.files;
		}
	};

private:

	/// Make sense to separate this function on two threads
	void reloadIfNewer(bool force_main, bool force_users);
	void run();

	FilesChangesTracker getFileListFor(const std::string & root_config_path);
	ConfigurationPtr loadConfigFor(const std::string & root_config_path, bool throw_error);
	bool applyConfigFor(bool for_main, ConfigurationPtr config, bool throw_on_error);

private:

	std::string main_config_path;
	std::string users_config_path;
	std::string include_from_path;

	Context * context;

	FilesChangesTracker last_main_config_files;
	FilesChangesTracker last_users_config_files;

	std::atomic<bool> quit{false};
	std::thread thread;

	Poco::Logger * log = &Logger::get("UsersConfigReloader");
};

}
