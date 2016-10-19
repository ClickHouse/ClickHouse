#include <daemon/GraphiteWriter.h>
#include <daemon/BaseDaemon.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/DNS.h>

#include <mutex>
#include <iomanip>


GraphiteWriter::GraphiteWriter(const std::string & config_name, const std::string & sub_path)
{
	Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
	port = config.getInt(config_name + ".port", 42000);
	host = config.getString(config_name + ".host", "127.0.0.1");
	timeout = config.getDouble(config_name + ".timeout", 0.1);

	root_path = config.getString(config_name + ".root_path", "one_min");
	if (root_path.size())
		root_path += ".";

	/** Что использовать в качестве имени сервера в названии метрики?
	  *
	  * По-умолчанию (для совместимости с существовавшим ранее поведением),
	  *  в качестве имени сервера берётся строка, аналогичная uname -n,
	  *  а затем к нему приписывается то, что указано в hostname_suffix, если есть.
	  * Часто серверы настроены так, что, например, для сервера example01-01-1.yandex.ru, uname -n будет выдавать example01-01-1
	  * Впрочем, uname -n может быть настроен произвольным образом. Он также может совпадать с FQDN.
	  *
	  * Если указано use_fqdn со значением true,
	  *  то в качестве имени сервера берётся FQDN (uname -f),
	  *  а значение hostname_suffix игнорируется.
	  */
	bool use_fqdn = config.getBool(config_name + ".use_fqdn", false);

	std::string hostname_in_path = use_fqdn
		? Poco::Net::DNS::thisHost().name()	/// То же, что hostname -f
		: Poco::Net::DNS::hostName();		/// То же, что uname -n

	/// Заменяем точки на подчёркивания, чтобы Graphite не интерпретировал их, как разделители пути.
	std::replace(std::begin(hostname_in_path), std::end(hostname_in_path), '.', '_');

	root_path += hostname_in_path;

	if (!use_fqdn)
		root_path += config.getString(config_name + ".hostname_suffix", "");

	if (sub_path.size())
		root_path += "." + sub_path;
}


std::string GraphiteWriter::getPerServerPath(const std::string & server_name, const std::string & root_path)
{
	std::string path = root_path + "." + server_name;
	std::replace(path.begin() + root_path.size() + 1, path.end(), '.', '_');
	return path;
}
