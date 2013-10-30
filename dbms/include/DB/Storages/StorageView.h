#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Parsers/ASTSelectQuery.h>


namespace DB
{

class StorageView : public IStorage {

public:
	static StoragePtr create(const std::string & name_, const ASTSelectQuery & inner_query_, NamesAndTypesListPtr columns_, const Context & context_);

	std::string getName() const { return "VIEW"; }
	std::string getTableName() const { return name; }
	const NamesAndTypesList & getColumnsList() const { return *columns; }
	DB::ASTPtr getInnerQuery() { return inner_query.clone(); };

private:
	String name;
	ASTSelectQuery inner_query;
	NamesAndTypesListPtr columns;
	const Context & context;

	StorageView(const std::string & name_, const ASTSelectQuery & inner_query_, NamesAndTypesListPtr columns_, const Context & context_): name(name_), inner_query(inner_query_), columns(columns_), context(context_) { }
};

}
