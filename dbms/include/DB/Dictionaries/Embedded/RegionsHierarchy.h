#pragma once

#include <vector>
#include <boost/noncopyable.hpp>
#include <common/Common.h>
#include <common/singleton.h>


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
	time_t file_modification_time = 0;

	using RegionID = UInt32;
	using RegionType = UInt8;
	using RegionDepth = UInt8;
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

	/// path to file with data
	std::string path;

public:
	RegionsHierarchy();
	RegionsHierarchy(const std::string & path_);

	/// Перезагружает, при необходимости, иерархию регионов. Непотокобезопасно.
	void reload();


	bool in(RegionID lhs, RegionID rhs) const
	{
		if (lhs >= parents.size())
			return false;

		while (lhs != 0 && lhs != rhs)
			lhs = parents[lhs];

		return lhs != 0;
	}

	RegionID toCity(RegionID region) const
	{
		if (region >= city.size())
			return 0;
		return city[region];
	}

	RegionID toCountry(RegionID region) const
	{
		if (region >= country.size())
			return 0;
		return country[region];
	}

	RegionID toArea(RegionID region) const
	{
		if (region >= area.size())
			return 0;
		return area[region];
	}

	RegionID toDistrict(RegionID region) const
	{
		if (region >= district.size())
			return 0;
		return district[region];
	}

	RegionID toContinent(RegionID region) const
	{
		if (region >= continent.size())
			return 0;
		return continent[region];
	}

	RegionID toTopContinent(RegionID region) const
	{
		if (region >= top_continent.size())
			return 0;
		return top_continent[region];
	}

	RegionID toParent(RegionID region) const
	{
		if (region >= parents.size())
			return 0;
		return parents[region];
	}

	RegionDepth getDepth(RegionID region) const
	{
		if (region >= depths.size())
			return 0;
		return depths[region];
	}

	RegionPopulation getPopulation(RegionID region) const
	{
		if (region >= populations.size())
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
