#include <DB/Storages/StorageJoin.h>


namespace DB
{


StorageJoin::StorageJoin(
	const String & path_,
	const String & name_,
	const Names & key_names_,
	ASTJoin::Kind kind_, ASTJoin::Strictness strictness_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
	: StorageSetOrJoinBase{path_, name_, columns_, materialized_columns_, alias_columns_, column_defaults_},
	key_names(key_names_), kind(kind_), strictness(strictness_)
{
	join = new Join(key_names, key_names, Limits(), kind, strictness);
	restore();
}


}
