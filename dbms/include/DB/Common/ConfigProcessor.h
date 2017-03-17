#pragma once

#include <string>
#include <unordered_set>

#include <Poco/DOM/Document.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/AutoPtr.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <common/logger_useful.h>


namespace zkutil
{
	class ZooKeeperNodeCache;
}

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
using XMLDocumentPtr = Poco::AutoPtr<Poco::XML::Document>;

class ConfigProcessor
{
public:
	using Substitutions = std::vector<std::pair<std::string, std::string> >;

	/// log_to_console нужно использовать, если система логгирования еще не инициализирована.
	ConfigProcessor(bool throw_on_bad_incl = false, bool log_to_console = false, const Substitutions & substitutions = Substitutions());

	~ConfigProcessor();

	/** Выполняет подстановки в конфиге и возвращает XML-документ.
	  *
	  * Пусть в качестве path передана "/path/file.xml"
	  * 1) Объединяем xml-дерево из /path/file.xml со всеми деревьями из файлов /path/{conf,file}.d/ *.{conf,xml}
	  *     Если у элемента есть атрибут replace, заменяем на него подходящий элемент.
	  *     Если у элемента есть атрибут remove, удаляем подходящий элемент.
	  *     Иначе объединяем детей рекурсивно.
	  * 2) Берем из конфига путь к файлу, из которого будем делать подстановки: <include_from>/path2/metrika.xml</include_from>.
	  *     Если путь не указан, используем /etc/metrika.xml
	  * 3) Заменяем элементы вида "<foo incl="bar"/>" на "<foo>содержимое элемента yandex.bar из metrika.xml</foo>"
	  * 4) Заменяет "<layer/>" на "<layer>номер слоя из имени хоста</layer>"
	  */
	XMLDocumentPtr processConfig(
			const std::string & path,
			bool * has_zk_includes = nullptr,
			zkutil::ZooKeeperNodeCache * zk_node_cache = nullptr);


	struct LoadedConfig
	{
		ConfigurationPtr configuration;
		bool has_zk_includes;
		bool loaded_from_preprocessed;
		bool preprocessed_written;
	};

	/** Делает processConfig и создает из результата Poco::Util::XMLConfiguration.
	  * Еще сохраняет результат в файл по пути, полученному из path приписыванием строки "-preprocessed" к имени файла.
	  */

	/// If allow_zk_includes is true, expects that the configuration xml can contain from_zk nodes.
	/// If the xml contains them, set has_zk_includes to true and don't write config-preprocessed.xml,
	/// expecting that config would be reloaded with zookeeper later.

	LoadedConfig loadConfig(const std::string & path, bool allow_zk_includes = false);

	LoadedConfig loadConfigWithZooKeeperIncludes(
			const std::string & path,
			zkutil::ZooKeeperNodeCache & zk_node_cache,
			bool fallback_to_preprocessed = false);

public:

	using Files = std::list<std::string>;

	static Files getConfigMergeFiles(const std::string & config_path);

private:
	bool throw_on_bad_incl;

	Logger * log;
	Poco::AutoPtr<Poco::Channel> channel_ptr;

	Substitutions substitutions;

	Poco::AutoPtr<Poco::XML::NamePool> name_pool;
	Poco::XML::DOMParser dom_parser;

private:
	using NodePtr = Poco::AutoPtr<Poco::XML::Node>;

	void mergeRecursive(XMLDocumentPtr config, Poco::XML::Node * config_node, Poco::XML::Node * with_node);

	void merge(XMLDocumentPtr config, XMLDocumentPtr with);

	std::string layerFromHost();

	void doIncludesRecursive(
			XMLDocumentPtr config,
			XMLDocumentPtr include_from,
			Poco::XML::Node * node,
			zkutil::ZooKeeperNodeCache * zk_node_cache,
			std::unordered_set<std::string> & contributing_zk_paths);

	void savePreprocessedConfig(const XMLDocumentPtr & config, const std::string & preprocessed_path);
};
