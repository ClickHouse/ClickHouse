#pragma once

#include <DB/Dictionaries/Embedded/RegionsHierarchy.h>
#include <Poco/DirectoryIterator.h>


/** Содержит несколько иерархий регионов, загружаемых из нескольких разных файлов.
  * Используется, чтобы поддержать несколько разных точек зрения о принадлежности регионов странам.
  * В первую очередь, для Крыма (Российская и Украинская точки зрения).
  */
class RegionsHierarchies
{
private:
	using Container = std::unordered_map<std::string, RegionsHierarchy>;
	Container data;
	Logger * log = &Logger::get("RegionsHierarchies");

public:
	static constexpr auto required_key = "path_to_regions_hierarchy_file";

	/** path должен указывать на файл с иерархией регионов "по-умолчанию". Она будет доступна по пустому ключу.
	  * Кроме того, рядом ищутся файлы, к имени которых (до расширения, если есть) добавлен произвольный _suffix.
	  * Такие файлы загружаются, и иерархия регионов кладётся по ключу suffix.
	  *
	  * Например, если указано /opt/geo/regions_hierarchy.txt,
	  *  то будет также загружен файл /opt/geo/regions_hierarchy_ua.txt, если такой есть - он будет доступен по ключу ua.
	  */
	RegionsHierarchies(const std::string & default_path = Poco::Util::Application::instance().config().getString(required_key))
	{
		LOG_DEBUG(log, "Adding default regions hierarchy from " << default_path);

		data.emplace(std::piecewise_construct,
			std::forward_as_tuple(""),
			std::forward_as_tuple(default_path));

		std::string basename = Poco::Path(default_path).getBaseName();

		Poco::Path dir_path = Poco::Path(default_path).absolute().parent();

		Poco::DirectoryIterator dir_end;
		for (Poco::DirectoryIterator dir_it(dir_path); dir_it != dir_end; ++dir_it)
		{
			std::string other_basename = dir_it.path().getBaseName();

			if (0 == other_basename.compare(0, basename.size(), basename) && other_basename.size() > basename.size() + 1)
			{
				if (other_basename[basename.size()] != '_')
					continue;

				std::string suffix = other_basename.substr(basename.size() + 1);

				LOG_DEBUG(log, "Adding regions hierarchy from " << dir_it->path() << ", key: " << suffix);

				data.emplace(std::piecewise_construct,
					std::forward_as_tuple(suffix),
					std::forward_as_tuple(dir_it->path()));
			}
		}
	}


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
