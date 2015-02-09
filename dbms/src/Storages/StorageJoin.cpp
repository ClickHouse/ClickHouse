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
	/// Проверяем, что ключ существует в определении таблицы.
	const auto check_key_exists = [] (const NamesAndTypesList & columns, const String & key)
	{
		for (const auto & column : columns)
			if (column.name == key)
				return true;
		return false;
	};

	for (const auto & key : key_names)
		if (!check_key_exists(*columns, key) && !check_key_exists(materialized_columns, key))
			throw Exception{
				"Key column (" + key + ") does not exist in table declaration.",
				ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};

	join = new Join(key_names, key_names, Limits(), kind, strictness);
	restore();
}


void StorageJoin::assertCompatible(ASTJoin::Kind kind_, ASTJoin::Strictness strictness_) const
{
	/// NOTE Можно немного ослабить.
	if (!(kind == kind_ && strictness == strictness_))
		throw Exception("Table " + name + " has incompatible type of JOIN.", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);
}


}
