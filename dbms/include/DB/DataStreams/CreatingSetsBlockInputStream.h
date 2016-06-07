#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>


namespace DB
{

/** Отдаёт без изменений данные из потока блоков, но перед чтением первого блока инициализирует все переданные множества.
  */
class CreatingSetsBlockInputStream : public IProfilingBlockInputStream
{
public:
	CreatingSetsBlockInputStream(
		BlockInputStreamPtr input,
		const SubqueriesForSets & subqueries_for_sets_,
		const Limits & limits)
		: subqueries_for_sets(subqueries_for_sets_),
		max_rows_to_transfer(limits.max_rows_to_transfer),
		max_bytes_to_transfer(limits.max_bytes_to_transfer),
		transfer_overflow_mode(limits.transfer_overflow_mode)
	{
		for (auto & elem : subqueries_for_sets)
			if (elem.second.source)
				children.push_back(elem.second.source);

		children.push_back(input);
	}

	String getName() const override { return "CreatingSets"; }

	String getID() const override
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

	/// Берёт totals только из основного источника, а не из источников подзапросов.
	const Block & getTotals() override;

protected:
	Block readImpl() override;

private:
	SubqueriesForSets subqueries_for_sets;
	bool created = false;

	size_t max_rows_to_transfer;
	size_t max_bytes_to_transfer;
	OverflowMode transfer_overflow_mode;

	size_t rows_to_transfer = 0;
	size_t bytes_to_transfer = 0;

	Logger * log = &Logger::get("CreatingSetsBlockInputStream");

	void create(SubqueryForSet & subquery);
};

}
