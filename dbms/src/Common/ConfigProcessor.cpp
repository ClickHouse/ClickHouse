#include <DB/Common/ConfigProcessor.h>
#include <sys/utsname.h>
#include <cerrno>
#include <cstring>
#include <iostream>

#include <Poco/DOM/Text.h>
#include <Poco/DOM/Attr.h>
#include <Poco/DOM/Comment.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace Poco::XML;


static bool endsWith(const std::string & s, const std::string & suffix)
{
	return s.size() >= suffix.size() && s.substr(s.size() - suffix.size()) == suffix;
}

/// Извлекает из строки первое попавшееся число, состоящее из хотя бы двух цифр.
static std::string numberFromHost(const std::string & s)
{
	for (size_t i = 0; i < s.size(); ++i)
	{
		std::string res;
		size_t j = i;
		while (j < s.size() && isdigit(s[j]))
			res += s[j++];
		if (res.size() >= 2)
		{
			while (res[0] == '0')
				res.erase(res.begin());
			return res;
		}
	}
	return "";
}

ConfigProcessor::ConfigProcessor(bool throw_on_bad_incl_, bool log_to_console, const Substitutions & substitutions_)
	: throw_on_bad_incl(throw_on_bad_incl_), substitutions(substitutions_)
{
	if (log_to_console && Logger::has("ConfigProcessor") == nullptr)
	{
		channel_ptr = new Poco::ConsoleChannel;
		log = &Logger::create("ConfigProcessor", channel_ptr.get(), Poco::Message::PRIO_TRACE);
	}
	else
	{
		log = &Logger::get("ConfigProcessor");
	}
}

/// Вектор из имени элемента и отсортированного списка имен и значений атрибутов (кроме атрибутов replace и remove).
/// Взаимно однозначно задает имя элемента и список его атрибутов. Нужен, чтобы сравнивать элементы.
using ElementIdentifier = std::vector<std::string>;

using NamedNodeMapPtr = Poco::AutoPtr<Poco::XML::NamedNodeMap>;
/// NOTE Можно избавиться от использования Node.childNodes() и итерации по полученному списку, потому что
///  доступ к i-му элементу этого списка работает за O(i).
using NodeListPtr = Poco::AutoPtr<Poco::XML::NodeList>;

static ElementIdentifier getElementIdentifier(Node * element)
{
	NamedNodeMapPtr attrs = element->attributes();
	std::vector<std::pair<std::string, std::string> > attrs_kv;
	for (size_t i = 0; i < attrs->length(); ++i)
	{
		Node * node = attrs->item(i);
		std::string name = node->nodeName();
		if (name == "replace" || name == "remove" || name == "incl")
			continue;
		std::string value = node->nodeValue();
		attrs_kv.push_back(std::make_pair(name, value));
	}
	std::sort(attrs_kv.begin(), attrs_kv.end());

	ElementIdentifier res;
	res.push_back(element->nodeName());
	for (const auto & attr : attrs_kv)
	{
		res.push_back(attr.first);
		res.push_back(attr.second);
	}

	return res;
}

static Node * getRootNode(Document * document)
{
	NodeListPtr children = document->childNodes();
	for (size_t i = 0; i < children->length(); ++i)
	{
		Node * child = children->item(i);
		/// Кроме корневого элемента на верхнем уровне могут быть комментарии. Пропустим их.
		if (child->nodeType() == Node::ELEMENT_NODE)
			return child;
	}

	throw Poco::Exception("No root node in document");
}

static bool allWhitespace(const std::string & s)
{
	return s.find_first_not_of(" \t\n\r") == std::string::npos;
}

void ConfigProcessor::mergeRecursive(DocumentPtr config, Node * config_root, Node * with_root)
{
	NodeListPtr with_nodes = with_root->childNodes();
	using ElementsByIdentifier = std::multimap<ElementIdentifier, Node *>;
	ElementsByIdentifier config_element_by_id;
	for (Node * node = config_root->firstChild(); node;)
	{
		Node * next_node = node->nextSibling();
		/// Уберем исходный текст из объединяемой части.
		if (node->nodeType() == Node::TEXT_NODE && !allWhitespace(node->getNodeValue()))
		{
			config_root->removeChild(node);
		}
		else if (node->nodeType() == Node::ELEMENT_NODE)
		{
			config_element_by_id.insert(ElementsByIdentifier::value_type(getElementIdentifier(node), node));
		}
		node = next_node;
	}

	for (size_t i = 0; i < with_nodes->length(); ++i)
	{
		Node * with_node = with_nodes->item(i);

		bool merged = false;
		bool remove = false;
		if (with_node->nodeType() == Node::ELEMENT_NODE)
		{
			Element * with_element = dynamic_cast<Element *>(with_node);
			remove = with_element->hasAttribute("remove");
			bool replace = with_element->hasAttribute("replace");

			if (remove && replace)
				throw Poco::Exception("remove and replace attributes on the same element");

			ElementsByIdentifier::iterator it = config_element_by_id.find(getElementIdentifier(with_node));

			if (it != config_element_by_id.end())
			{
				Node * config_node = it->second;
				config_element_by_id.erase(it);

				if (remove)
				{
					config_root->removeChild(config_node);
				}
				else if (replace)
				{
					with_element->removeAttribute("replace");
					NodePtr new_node = config->importNode(with_node, true);
					config_root->replaceChild(new_node, config_node);
				}
				else
				{
					mergeRecursive(config, config_node, with_node);
				}
				merged = true;
			}
		}
		if (!merged && !remove)
		{
			NodePtr new_node = config->importNode(with_node, true);
			config_root->appendChild(new_node);
		}
	}
}

void ConfigProcessor::merge(DocumentPtr config, DocumentPtr with)
{
	mergeRecursive(config, getRootNode(&*config), getRootNode(&*with));
}

std::string ConfigProcessor::layerFromHost()
{
	utsname buf;
	if (uname(&buf))
		throw Poco::Exception(std::string("uname failed: ") + std::strerror(errno));

	std::string layer = numberFromHost(buf.nodename);
	if (layer.empty())
		throw Poco::Exception(std::string("no layer in host name: ") + buf.nodename);

	return layer;
}

void ConfigProcessor::doIncludesRecursive(DocumentPtr config, DocumentPtr include_from, Node * node)
{
	if (node->nodeType() == Node::TEXT_NODE)
	{
		for (auto & substitution : substitutions)
		{
			std::string value = node->nodeValue();

			bool replace_occured = false;
			size_t pos;
			while ((pos = value.find(substitution.first)) != std::string::npos)
			{
				value.replace(pos, substitution.first.length(), substitution.second);
				replace_occured = true;
			}

			if (replace_occured)
				node->setNodeValue(value);
		}
	}

	if (node->nodeType() != Node::ELEMENT_NODE)
		return;

	/// Будем заменять <layer> на число из имени хоста, только если во входном файле есть тег <layer>, и он пустой, и у него нет атрибутов
	if ( node->nodeName() == "layer" &&
		!node->hasAttributes() &&
		!node->hasChildNodes() &&
		 node->nodeValue().empty())
	{
		NodePtr new_node = config->createTextNode(layerFromHost());
		node->appendChild(new_node);
		return;
	}

	NamedNodeMapPtr attributes = node->attributes();
	Node * incl_attribute = attributes->getNamedItem("incl");

	/// Заменять имеющееся значение, а не добавлять к нему.
	bool replace = attributes->getNamedItem("replace");

	if (incl_attribute)
	{
		std::string name = incl_attribute->getNodeValue();
		Node * included_node = include_from ? include_from->getNodeByPath("yandex/" + name) : nullptr;
		if (!included_node)
		{
			if (attributes->getNamedItem("optional"))
				node->parentNode()->removeChild(node);
			else if (throw_on_bad_incl)
				throw Poco::Exception("Include not found: " + name);
			else
				LOG_WARNING(log, "Include not found: " << name);
		}
		else
		{
			if (replace)
				while (Node * child = node->firstChild())
					node->removeChild(child);

			NodeListPtr children = included_node->childNodes();
			for (size_t i = 0; i < children->length(); ++i)
			{
				NodePtr new_node = config->importNode(children->item(i), true);
				node->appendChild(new_node);
			}

			Element * element = dynamic_cast<Element *>(node);
			element->removeAttribute("incl");

			if (replace)
				element->removeAttribute("replace");

			NamedNodeMapPtr from_attrs = included_node->attributes();
			for (size_t i = 0; i < from_attrs->length(); ++i)
			{
				element->setAttributeNode(dynamic_cast<Attr *>(config->importNode(from_attrs->item(i), true)));
			}
		}
	}

	NodeListPtr children = node->childNodes();
	for (size_t i = 0; i < children->length(); ++i)
	{
		doIncludesRecursive(config, include_from, children->item(i));
	}
}

void ConfigProcessor::doIncludes(DocumentPtr config, DocumentPtr include_from)
{
	doIncludesRecursive(config, include_from, getRootNode(&*config));
}

ConfigProcessor::Files ConfigProcessor::getConfigMergeFiles(const std::string & config_path)
{
	Files res;

	Poco::Path merge_dir_path(config_path);
	merge_dir_path.setExtension("d");

	std::vector<std::string> merge_dirs;
	merge_dirs.push_back(merge_dir_path.toString());
	if (merge_dir_path.getBaseName() != "conf")	{
		merge_dir_path.setBaseName("conf");
		merge_dirs.push_back(merge_dir_path.toString());
	}

	for (const std::string & merge_dir_name : merge_dirs)
	{
		Poco::File merge_dir(merge_dir_name);
		if (!merge_dir.exists() || !merge_dir.isDirectory())
			continue;
		for (Poco::DirectoryIterator it(merge_dir_name); it != Poco::DirectoryIterator(); ++it)
		{
			Poco::File & file = *it;
			if (file.isFile() && (endsWith(file.path(), ".xml") || endsWith(file.path(), ".conf")))
			{
				res.push_back(file.path());
			}
		}
	}

	return res;
}

XMLDocumentPtr ConfigProcessor::processConfig(const std::string & path_str)
{
	/// We need larger name pool to allow to support vast amount of users in users.xml files for ClickHouse.
	/// Size is prime because Poco::XML::NamePool uses bad (inefficient, low quality)
	///  hash function internally, and its size was prime by default.
	Poco::AutoPtr<Poco::XML::NamePool> name_pool(new Poco::XML::NamePool(65521));
	Poco::XML::DOMParser dom_parser(name_pool);

	DocumentPtr config = dom_parser.parse(path_str);

	std::vector<std::string> contributing_files;
	contributing_files.push_back(path_str);

	for (auto & merge_file : getConfigMergeFiles(path_str))
	{
		try
		{
			DocumentPtr with = dom_parser.parse(merge_file);
			merge(config, with);
			contributing_files.push_back(merge_file);
		}
		catch (Poco::Exception & e)
		{
			throw Poco::Exception("Failed to merge config with " + merge_file + ": " + e.displayText());
		}
	}

	try
	{
		Node * node = config->getNodeByPath("yandex/include_from");
		DocumentPtr include_from;
		std::string include_from_path;
		if (node)
		{
			include_from_path = node->innerText();
		}
		else
		{
			std::string default_path = "/etc/metrika.xml";
			if (Poco::File(default_path).exists())
				include_from_path = default_path;
		}
		if (!include_from_path.empty())
		{
			contributing_files.push_back(include_from_path);
			include_from = dom_parser.parse(include_from_path);
		}

		doIncludes(config, include_from);
	}
	catch (Poco::Exception & e)
	{
		throw Poco::Exception("Failed to preprocess config: " + e.displayText());
	}

	std::stringstream comment;
	comment <<     " This file was generated automatically.\n";
	comment << "     Do not edit it: it is likely to be discarded and generated again before it's read next time.\n";
	comment << "     Files used to generate this file:";
	for (const std::string & path : contributing_files)
	{
		comment << "\n       " << path;
	}
	comment<<"      ";
	NodePtr new_node = config->createTextNode("\n\n");
	config->insertBefore(new_node, config->firstChild());
	new_node = config->createComment(comment.str());
	config->insertBefore(new_node, config->firstChild());

	return config;
}

ConfigurationPtr ConfigProcessor::loadConfig(const std::string & path)
{
	DocumentPtr res = processConfig(path);

	Poco::Path preprocessed_path(path);
	preprocessed_path.setBaseName(preprocessed_path.getBaseName() + "-preprocessed");
	try
	{
		DOMWriter().writeNode(preprocessed_path.toString(), res);
	}
	catch (Poco::Exception & e)
	{
		LOG_WARNING(log, "Couldn't save preprocessed config to " << preprocessed_path.toString() << ": " << e.displayText());
	}

	return new Poco::Util::XMLConfiguration(res);
}
