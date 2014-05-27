#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <Yandex/time2str.h>

namespace DB
{

ActiveDataPartSet::ActiveDataPartSet() {}

void ActiveDataPartSet::add(const String & name)
{
	Poco::ScopedLock<Poco::Mutex> lock(mutex);

	if (getContainingPart(name) != "")
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
	Poco::ScopedLock<Poco::Mutex> lock(mutex);

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
	Poco::ScopedLock<Poco::Mutex> lock(mutex);

	Strings res;
	for (const Part & part : parts)
	{
		res.push_back(part.name);
	}

	return res;
}



String ActiveDataPartSet::getPartName(DayNum_t left_date, DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level)
{
	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	/// Имя директории для куска иммет вид: YYYYMMDD_YYYYMMDD_N_N_L.
	String res;
	{
		unsigned left_date_id = Date2OrderedIdentifier(date_lut.fromDayNum(left_date));
		unsigned right_date_id = Date2OrderedIdentifier(date_lut.fromDayNum(right_date));

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

bool ActiveDataPartSet::isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec & matches)
{
	static Poco::RegularExpression file_name_regexp("^(\\d{8})_(\\d{8})_(\\d+)_(\\d+)_(\\d+)");
	return (file_name_regexp.match(dir_name, 0, matches) && 6 == matches.size());
}

void ActiveDataPartSet::parsePartName(const String & file_name, Part & part, const Poco::RegularExpression::MatchVec * matches_p)
{
	Poco::RegularExpression::MatchVec match_vec;
	if (!matches_p)
	{
		if (!isPartDirectory(file_name, match_vec))
			throw Exception("Unexpected part name: " + file_name, ErrorCodes::BAD_DATA_PART_NAME);
		matches_p = &match_vec;
	}

	const Poco::RegularExpression::MatchVec & matches = *matches_p;

	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	part.left_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[1].offset, matches[1].length)));
	part.right_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[2].offset, matches[2].length)));
	part.left = parse<UInt64>(file_name.substr(matches[3].offset, matches[3].length));
	part.right = parse<UInt64>(file_name.substr(matches[4].offset, matches[4].length));
	part.level = parse<UInt32>(file_name.substr(matches[5].offset, matches[5].length));

	part.left_month = date_lut.toFirstDayNumOfMonth(part.left_date);
	part.right_month = date_lut.toFirstDayNumOfMonth(part.right_date);
}

}
