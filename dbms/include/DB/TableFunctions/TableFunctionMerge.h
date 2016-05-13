#pragma once

#include <DB/TableFunctions/ITableFunction.h>


namespace DB
{

/*
 * merge(db_name, tables_regexp)- создаёт временный StorageMerge.
 * Cтруктура таблицы берётся из первой попавшейся таблицы, подходящей под регексп.
 * Если такой таблицы нет - кидается исключение.
 */
class TableFunctionMerge: public ITableFunction
{
public:
	std::string getName() const override { return "merge"; }
	StoragePtr execute(ASTPtr ast_function, Context & context) const override;
};


}
