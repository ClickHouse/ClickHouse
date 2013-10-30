#include <DB/Storages/StorageView.h>

namespace DB
{


StoragePtr StorageView::create(const std::string & name_, const ASTSelectQuery & inner_query_, NamesAndTypesListPtr columns_, const Context & context_)
{
	return (new StorageView(name_, inner_query_, columns_, context_))->thisPtr();
}


}
