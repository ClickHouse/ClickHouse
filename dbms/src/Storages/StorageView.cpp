#include <set>
#include <boost/bind.hpp>

#include <sparsehash/dense_hash_set>
#include <sparsehash/dense_hash_map>

#include <DB/Columns/ColumnNested.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Interpreters/Context.h>

#include <DB/Storages/StorageView.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/Parsers/ASTIdentifier.h>

namespace DB
{


StoragePtr StorageView::create(const std::string & name_, const ASTSelectQuery & inner_query_, NamesAndTypesListPtr columns_, const Context & context_)
{
	return (new StorageView(name_, inner_query_, columns_, context_))->thisPtr();
}


}
