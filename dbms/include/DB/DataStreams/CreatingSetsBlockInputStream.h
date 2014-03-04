#pragma once
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/Set.h>

namespace DB
{

/** Отдает без изменений данные из потока блоков, но перед чтением первого блока инициализирует все переданные множества.
  */
class CreatingSetsBlockInputStream : public IProfilingBlockInputStream
{
public:
	CreatingSetsBlockInputStream(BlockInputStreamPtr input, const Sets & sets_)
		: sets(sets_), created(false), log(&Logger::get("CreatingSetsBlockInputStream"))
	{
		for (SetPtr set : sets)
		{
			children.push_back(set->getSource());
		}
		children.push_back(input);
	}

	String getName() const { return "CreatingSetsBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "CreatingSets(";

		Strings children_ids(children.size());
		for (size_t i = 0; i < children.size(); ++i)
			children_ids[i] = children[i]->getID();

		/// Будем считать, что порядок создания множеств не имеет значения.
		std::sort(children_ids.begin(), children_ids.end() - 1);

		for (size_t i = 0; i < children_ids.size(); ++i)
			res << (i == 0 ? "" : ", ") << children_ids[i];

		res << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	Sets sets;
	bool created;

	Logger * log;

	void createSet(SetPtr set);
	void logProfileInfo(Stopwatch & watch, IBlockInputStream & in, size_t entries);
};

}
