#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Columns/ColumnConst.h>



namespace DB
{


/** Добавляет в блок недостающие столбцы со значениями по-умолчанию.
  * Эти столбцы - материалированные (не константы).
  */
class RemoveColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
	RemoveColumnsBlockInputStream(
		BlockInputStreamPtr input_,
		const Names & columns_to_remove_)
		: columns_to_remove(columns_to_remove_)
	{
		children.push_back(input_);
	}

	String getName() const { return "AddingDefaultBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "RemoveColumns(" << children.back()->getID();

		for (const auto & it : columns_to_remove)
			res << ", " << it;

		res << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		Block res = children.back()->read();
		if (!res)
			return res;

		for (const auto & it : columns_to_remove)
			if (res.has(it))
				res.erase(it);

		return res;
	}

private:
	Names columns_to_remove;
};

}
