#pragma once

#include <time.h>
#include <string>
#include <thread>
#include <atomic>


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
	~UsersConfigReloader();

private:
	std::string path;
	Context * context;

	time_t file_modification_time;
	std::atomic<bool> quit;
	std::thread thread;

	Poco::Logger * log;

	void reloadIfNewer(bool force);
	void run();
};

}
