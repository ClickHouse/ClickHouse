#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Core/VirtualColumnsList.h>


namespace DB
{

/** Добавляет в блок виртуальные столбцы.
  * Получает на вход VirtualColumnList и StoragePtr, относительно которого будут вычисляться значения столбцов.
  */
class AddingVirtualColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
	AddingVirtualColumnsBlockInputStream(
		BlockInputStreamPtr input_,
		VirtualColumnList virtual_columns_,
		StoragePtr storage_)
		: virtual_columns(virtual_columns_), storage(storage_)
	{
		children.push_back(input_);
	}

	String getName() const { return "AddingVirtualColumnsBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "AddingVirtualColumnsBlockInputStream(" << children.back()->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		Block res = children.back()->read();
		if (!res)
			return res;
		virtual_columns.populate(res, storage);
		return res;
	}

private:
	VirtualColumnList virtual_columns;
	StoragePtr storage;
};

}
