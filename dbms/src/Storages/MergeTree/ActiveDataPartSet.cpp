#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int BAD_DATA_PART_NAME;
}


ActiveDataPartSet::ActiveDataPartSet(const Strings & names)
{
	for (const auto & name : names)
		addImpl(name);
}


void ActiveDataPartSet::add(const String & name)
{
	std::lock_guard<std::mutex> lock(mutex);
	addImpl(name);
}


void ActiveDataPartSet::addImpl(const String & name)
{
	if (getContainingPartImpl(name) != "")
		return;

	Part part;
	part.name = name;
	parsePartName(name, part);

	/// Куски, содержащиеся в part, идут в data_parts подряд, задевая место, куда вставился бы сам part.
	Parts::iterator it = parts.lower_bound(part);

	/// Пойдем влево.
	while (it != parts.begin())
	{
		--it;
		if (!part.contains(*it))
		{
			++it;
			break;
		}
		parts.erase(it++);
	}

	/// Пойдем вправо.
	while (it != parts.end() && part.contains(*it))
	{
		parts.erase(it++);
	}

	parts.insert(part);
}


String ActiveDataPartSet::getContainingPart(const String & part_name) const
{
	std::lock_guard<std::mutex> lock(mutex);
	return getContainingPartImpl(part_name);
}


String ActiveDataPartSet::getContainingPartImpl(const String & part_name) const
{
	Part part;
	parsePartName(part_name, part);

	/// Кусок может покрываться только предыдущим или следующим в parts.
	Parts::iterator it = parts.lower_bound(part);

	if (it != parts.end())
	{
		if (it->name == part_name)
			return it->name;
		if (it->contains(part))
			return it->name;
	}

	if (it != parts.begin())
	{
		--it;
		if (it->contains(part))
			return it->name;
	}

	return "";
}


Strings ActiveDataPartSet::getParts() const
{
	std::lock_guard<std::mutex> lock(mutex);

	Strings res;
	res.reserve(parts.size());
	for (const Part & part : parts)
		res.push_back(part.name);

	return res;
}


size_t ActiveDataPartSet::size() const
{
	std::lock_guard<std::mutex> lock(mutex);
	return parts.size();
}


String ActiveDataPartSet::getPartName(DayNum_t left_date, DayNum_t right_date, Int64 left_id, Int64 right_id, UInt64 level)
{
	const auto & date_lut = DateLUT::instance();

	/// Имя директории для куска иммет вид: YYYYMMDD_YYYYMMDD_N_N_L.
	String res;
	{
		unsigned left_date_id = date_lut.toNumYYYYMMDD(left_date);
		unsigned right_date_id = date_lut.toNumYYYYMMDD(right_date);

		WriteBufferFromString wb(res);

		writeIntText(left_date_id, wb);
		writeChar('_', wb);
		writeIntText(right_date_id, wb);
		writeChar('_', wb);
		writeIntText(left_id, wb);
		writeChar('_', wb);
		writeIntText(right_id, wb);
		writeChar('_', wb);
		writeIntText(level, wb);
	}

	return res;
}


bool ActiveDataPartSet::isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec * out_matches)
{
	Poco::RegularExpression::MatchVec matches;
	static Poco::RegularExpression file_name_regexp("^(\\d{8})_(\\d{8})_(-?\\d+)_(-?\\d+)_(\\d+)");
	bool res = (file_name_regexp.match(dir_name, 0, matches) && 6 == matches.size());
	if (out_matches)
		*out_matches = matches;
	return res;
}


void ActiveDataPartSet::parsePartName(const String & file_name, Part & part, const Poco::RegularExpression::MatchVec * matches_p)
{
	Poco::RegularExpression::MatchVec match_vec;
	if (!matches_p)
	{
		if (!isPartDirectory(file_name, &match_vec))
			throw Exception("Unexpected part name: " + file_name, ErrorCodes::BAD_DATA_PART_NAME);
		matches_p = &match_vec;
	}

	const Poco::RegularExpression::MatchVec & matches = *matches_p;

	const auto & date_lut = DateLUT::instance();

	part.left_date = date_lut.YYYYMMDDToDayNum(parse<UInt32>(file_name.substr(matches[1].offset, matches[1].length)));
	part.right_date = date_lut.YYYYMMDDToDayNum(parse<UInt32>(file_name.substr(matches[2].offset, matches[2].length)));
	part.left = parse<Int64>(file_name.substr(matches[3].offset, matches[3].length));
	part.right = parse<Int64>(file_name.substr(matches[4].offset, matches[4].length));
	part.level = parse<UInt32>(file_name.substr(matches[5].offset, matches[5].length));

	DayNum_t left_month = date_lut.toFirstDayNumOfMonth(part.left_date);
	DayNum_t right_month = date_lut.toFirstDayNumOfMonth(part.right_date);

	if (left_month != right_month)
		throw Exception("Part name " + file_name + " contains different months", ErrorCodes::BAD_DATA_PART_NAME);

	part.month = left_month;
}


bool ActiveDataPartSet::contains(const String & outer_part_name, const String & inner_part_name)
{
	Part outer, inner;
	parsePartName(outer_part_name, outer);
	parsePartName(inner_part_name, inner);
	return outer.contains(inner);
}

}
