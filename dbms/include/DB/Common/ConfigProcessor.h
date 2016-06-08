#pragma once

#include <string>
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

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
using XMLDocumentPtr = Poco::AutoPtr<Poco::XML::Document>;

class ConfigProcessor
{
public:
	using Substitutions = std::vector<std::pair<std::string, std::string> >;

	/// log_to_console нужно использовать, если система логгирования еще не инициализирована.
	ConfigProcessor(bool throw_on_bad_incl = false, bool log_to_console = false, const Substitutions & substitutions = Substitutions());

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
	XMLDocumentPtr processConfig(const std::string & path);

	/** Делает processConfig и создает из результата Poco::Util::XMLConfiguration.
	  * Еще сохраняет результат в файл по пути, полученному из path приписыванием строки "-preprocessed" к имени файла.
	  */
	ConfigurationPtr loadConfig(const std::string & path);

private:
	Logger * log;
	Poco::AutoPtr<Poco::Channel> channel_ptr;
	bool throw_on_bad_incl;
	Substitutions substitutions;

	using DocumentPtr = XMLDocumentPtr;
	using NodePtr = Poco::AutoPtr<Poco::XML::Node>;

	void mergeRecursive(DocumentPtr config, Poco::XML::Node * config_node, Poco::XML::Node * with_node);

	void merge(DocumentPtr config, DocumentPtr with);

	std::string layerFromHost();

	void doIncludesRecursive(DocumentPtr config, DocumentPtr include_from, Poco::XML::Node * node);

	void doIncludes(DocumentPtr config, DocumentPtr include_from);
};
