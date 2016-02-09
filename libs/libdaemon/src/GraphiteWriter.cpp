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

	/// То же, что uname -n
	std::string hostname_in_path = Poco::Net::DNS::hostName();

	/// Заменяем точки на подчёркивания, чтобы Graphite не интерпретировал их, как разделители пути.
	std::replace(std::begin(hostname_in_path), std::end(hostname_in_path), '.', '_');

	root_path += hostname_in_path + config.getString(config_name + ".hostname_suffix", "");

	if (sub_path.size())
		root_path += "." + sub_path;
}

std::string getPostfix()
{
	/// Угадываем имя среды по имени машинки
	/// машинки имеют имена вида example01dt.yandex.ru
	/// t - test
	/// dev - development
	/// никакого суффикса - production

	std::stringstream path_full;

	std::string hostname = Poco::Net::DNS::hostName();
	hostname = hostname.substr(0, hostname.find('.'));

	const std::string development_suffix = "dev";
	if (hostname.back() == 't')
		path_full << "test.";
	else if (hostname.size() > development_suffix.size() &&
			hostname.substr(hostname.size() - development_suffix.size()) == development_suffix)
		path_full << "development.";
	else
		path_full << "production.";

	const BaseDaemon & daemon = BaseDaemon::instance();

	if (daemon.getLayer())
		path_full << "layer" << std::setfill('0') << std::setw(3) << *daemon.getLayer() << ".";

	/// Когда несколько демонов запускается на одной машине
	/// к имени демона добавляется цифра.
	/// Удалим последнюю цифру
	std::locale locale;
	std::string command_name = daemon.commandName();
	if (std::isdigit(command_name.back(), locale))
		command_name = command_name.substr(0, command_name.size() - 1);
	path_full << command_name;

	return path_full.str();
}

std::string GraphiteWriter::getPerLayerPath(const std::string & prefix)
{
	const std::string static postfix = getPostfix();
	return prefix + "." + postfix;
}

std::string GraphiteWriter::getPerServerPath(const std::string & server_name, const std::string & root_path)
{
	std::string path = root_path + "." + server_name;
	std::replace(path.begin() + root_path.size() + 1, path.end(), '.', '_');
	return path;
}
