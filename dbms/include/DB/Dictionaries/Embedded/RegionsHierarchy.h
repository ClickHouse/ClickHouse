#pragma once

#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/File.h>

#include <common/logger_useful.h>
#include <common/singleton.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/ReadHelpers.h>

#include <boost/noncopyable.hpp>

#define REGION_TYPE_CITY 	  6
#define REGION_TYPE_AREA 	  5
#define REGION_TYPE_DISTRICT  4
#define REGION_TYPE_COUNTRY   3
#define REGION_TYPE_CONTINENT 1


/** Класс, позволяющий узнавать, принадлежит ли регион с одним RegionID региону с другим RegionID.
  * Информацию об иерархии регионов загружает из текстового файла.
  * Умеет, по запросу, обновлять данные.
  */
class RegionsHierarchy : private boost::noncopyable
{
private:
	std::string path;
	time_t file_modification_time;
	Logger * log;

	using RegionID = Int32;
	using RegionType = Int8;
	using RegionDepth = Int8;
	using RegionPopulation = UInt32;

	/// отношение parent; 0, если родителей нет - обычная lookup таблица.
	using RegionParents = std::vector<RegionID>;
	/// тип региона
	using RegionTypes = std::vector<RegionType>;
	/// глубина в дереве, начиная от страны (страна: 1, корень: 0)
	using RegionDepths = std::vector<RegionDepth>;
	/// население региона. Если больше 2^32 - 1, то приравнивается к этому максимуму.
	using RegionPopulations = std::vector<RegionPopulation>;

	/// регион -> родительский регион
	RegionParents parents;
	/// регион -> город, включающий его или 0, если такого нет
	RegionParents city;
	/// регион -> страна, включающая его или 0, если такого нет
	RegionParents country;
	/// регион -> область, включающая его или 0, если такой нет
	RegionParents area;
	/// регион -> округ, включающий его или 0, если такого нет
	RegionParents district;
	/// регион -> континет (первый при подъёме по иерархии регионов), включающий его или 0, если такого нет
	RegionParents continent;
	/// регион -> континет (последний при подъёме по иерархии регионов), включающий его или 0, если такого нет
	RegionParents top_continent;

	/// регион -> население или 0, если неизвестно.
	RegionPopulations populations;

	/// регион - глубина в дереве
	RegionDepths depths;

public:
	RegionsHierarchy(const std::string & path_ = Poco::Util::Application::instance().config().getString("path_to_regions_hierarchy_file"))
		: path(path_), file_modification_time(0), log(&Logger::get("RegionsHierarchy"))
	{
	}


	/// Перезагружает, при необходимости, иерархию регионов. Непотокобезопасно.
	void reload()
	{
		time_t new_modification_time = Poco::File(path).getLastModified().epochTime();
		if (new_modification_time <= file_modification_time)
			return;
		file_modification_time = new_modification_time;

		LOG_DEBUG(log, "Reloading regions hierarchy");

		const size_t initial_size = 10000;

		RegionParents new_parents(initial_size);
		RegionParents new_city(initial_size);
		RegionParents new_country(initial_size);
		RegionParents new_area(initial_size);
		RegionParents new_district(initial_size);
		RegionParents new_continent(initial_size);
		RegionParents new_top_continent(initial_size);
		RegionPopulations new_populations(initial_size);
		RegionDepths  new_depths(initial_size);
		RegionTypes types(initial_size);

		DB::ReadBufferFromFile in(path);

		RegionID max_region_id = 0;
		while (!in.eof())
		{
			RegionID region_id = 0;
			RegionID parent_id = 0;
			RegionType type = 0;
			RegionPopulation population = 0;

			DB::readIntText(region_id, in);
			DB::assertChar('\t', in);
			DB::readIntText(parent_id, in);
			DB::assertChar('\t', in);
			DB::readIntText(type, in);

			/** Далее может быть перевод строки (старый вариант)
			  *  или таб, население региона, перевод строки (новый вариант).
			  */
			if (!in.eof() && *in.position() == '\t')
			{
				++in.position();
				UInt64 population_big = 0;
				DB::readIntText(population_big, in);
				population = population_big > std::numeric_limits<RegionPopulation>::max()
					? std::numeric_limits<RegionPopulation>::max()
					: population_big;
			}
			DB::assertChar('\n', in);

			if (region_id <= 0)
				continue;

			if (parent_id < 0)
				parent_id = 0;

			if (region_id > max_region_id)
			{
				max_region_id = region_id;

				while (region_id >= static_cast<int>(new_parents.size()))
				{
					new_parents.resize(new_parents.size() * 2);
					new_populations.resize(new_parents.size());
					types.resize(new_parents.size());
				}
			}

			new_parents[region_id] = parent_id;
			new_populations[region_id] = population;
			types[region_id] = type;
		}

		new_parents		.resize(max_region_id + 1);
		new_city		.resize(max_region_id + 1);
		new_country		.resize(max_region_id + 1);
		new_area		.resize(max_region_id + 1);
		new_district	.resize(max_region_id + 1);
		new_continent	.resize(max_region_id + 1);
		new_top_continent.resize(max_region_id + 1);
		new_populations .resize(max_region_id + 1);
		new_depths		.resize(max_region_id + 1);
		types			.resize(max_region_id + 1);

		/// пропишем города и страны для регионов
		for (RegionID i = 0; i <= max_region_id; ++i)
		{
			if (types[i] == REGION_TYPE_CITY)
				new_city[i] = i;

			if (types[i] == REGION_TYPE_AREA)
				new_area[i] = i;

			if (types[i] == REGION_TYPE_DISTRICT)
				new_district[i] = i;

			if (types[i] == REGION_TYPE_COUNTRY)
				new_country[i] = i;

			if (types[i] == REGION_TYPE_CONTINENT)
			{
				new_continent[i] = i;
				new_top_continent[i] = i;
			}

			RegionDepth depth = 0;
			RegionID current = i;
			while (true)
			{
				++depth;

				if (depth == std::numeric_limits<RegionDepth>::max())
					throw Poco::Exception("Logical error in regions hierarchy: region " + DB::toString(current) + " possible is inside infinite loop");

				current = new_parents[current];
				if (current == 0)
					break;

				if (current > max_region_id)
					throw Poco::Exception("Logical error in regions hierarchy: region " + DB::toString(current) + " (specified as parent) doesn't exist");

				if (types[current] == REGION_TYPE_CITY)
					new_city[i] = current;

				if (types[current] == REGION_TYPE_AREA)
					new_area[i] = current;

				if (types[current] == REGION_TYPE_DISTRICT)
					new_district[i] = current;

				if (types[current] == REGION_TYPE_COUNTRY)
					new_country[i] = current;

				if (types[current] == REGION_TYPE_CONTINENT)
				{
					if (!new_continent[i])
						new_continent[i] = current;
					new_top_continent[i] = current;
				}
			}

			new_depths[i] = depth;
		}

		parents.swap(new_parents);
		country.swap(new_country);
		city.swap(new_city);
		area.swap(new_area);
		district.swap(new_district);
		continent.swap(new_continent);
		top_continent.swap(new_top_continent);
		populations.swap(new_populations);
		depths.swap(new_depths);
	}


	bool in(RegionID lhs, RegionID rhs) const
	{
		if (static_cast<size_t>(lhs) >= parents.size())
			return false;

		while (lhs != 0 && lhs != rhs)
			lhs = parents[lhs];

		return lhs != 0;
	}

	RegionID toCity(RegionID region) const
	{
		if (static_cast<size_t>(region) >= city.size())
			return 0;
		return city[region];
	}

	RegionID toCountry(RegionID region) const
	{
		if (static_cast<size_t>(region) >= country.size())
			return 0;
		return country[region];
	}

	RegionID toArea(RegionID region) const
	{
		if (static_cast<size_t>(region) >= area.size())
			return 0;
		return area[region];
	}

	RegionID toDistrict(RegionID region) const
	{
		if (static_cast<size_t>(region) >= district.size())
			return 0;
		return district[region];
	}

	RegionID toContinent(RegionID region) const
	{
		if (static_cast<size_t>(region) >= continent.size())
			return 0;
		return continent[region];
	}

	RegionID toTopContinent(RegionID region) const
	{
		if (static_cast<size_t>(region) >= top_continent.size())
			return 0;
		return top_continent[region];
	}

	RegionID toParent(RegionID region) const
	{
		if (static_cast<size_t>(region) >= parents.size())
			return 0;
		return parents[region];
	}

	RegionDepth getDepth(RegionID region) const
	{
		if (static_cast<size_t>(region) >= depths.size())
			return 0;
		return depths[region];
	}

	RegionPopulation getPopulation(RegionID region) const
	{
		if (static_cast<size_t>(region) >= populations.size())
			return 0;
		return populations[region];
	}
};


class RegionsHierarchySingleton : public Singleton<RegionsHierarchySingleton>, public RegionsHierarchy
{
friend class Singleton<RegionsHierarchySingleton>;
protected:
	RegionsHierarchySingleton()
	{
	}
};
