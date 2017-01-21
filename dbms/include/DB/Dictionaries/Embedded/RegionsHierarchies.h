#pragma once

#include <DB/Dictionaries/Embedded/RegionsHierarchy.h>
#include <Poco/Exception.h>
#include <unordered_map>


/** Содержит несколько иерархий регионов, загружаемых из нескольких разных файлов.
  * Используется, чтобы поддержать несколько разных точек зрения о принадлежности регионов странам.
  * В первую очередь, для Крыма (Российская и Украинская точки зрения).
  */
class RegionsHierarchies
{
private:
	using Container = std::unordered_map<std::string, RegionsHierarchy>;
	Container data;

public:
	/** path_to_regions_hierarchy_file in configuration file
	  * должен указывать на файл с иерархией регионов "по-умолчанию". Она будет доступна по пустому ключу.
	  * Кроме того, рядом ищутся файлы, к имени которых (до расширения, если есть) добавлен произвольный _suffix.
	  * Такие файлы загружаются, и иерархия регионов кладётся по ключу suffix.
	  *
	  * Например, если указано /opt/geo/regions_hierarchy.txt,
	  *  то будет также загружен файл /opt/geo/regions_hierarchy_ua.txt, если такой есть - он будет доступен по ключу ua.
	  */
	RegionsHierarchies();

	/// Has corresponding section in configuration file.
	static bool isConfigured();


	/** Перезагружает, при необходимости, все иерархии регионов.
	  */
	void reload()
	{
		for (auto & elem : data)
			elem.second.reload();
	}


	const RegionsHierarchy & get(const std::string & key) const
	{
		auto it = data.find(key);

		if (data.end() == it)
			throw Poco::Exception("There is no regions hierarchy for key " + key);

		return it->second;
	}
};
