#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/** См. описание структуры данных в MergeTreeData.
  */
class StorageMergeTree : public IStorage
{
public:
	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов.
	  *
	  * primary_expr_ast	- выражение для сортировки;
	  * date_column_name 	- имя столбца с датой;
	  * index_granularity 	- на сколько строчек пишется одно значение индекса.
	  */
	static StoragePtr create(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
		const Context & context_,
		ASTPtr & primary_expr_ast_,
		const String & date_column_name_,
		const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
		size_t index_granularity_,
		MergeTreeData::Mode mode_ = MergeTreeData::Ordinary,
		const String & sign_column_ = "",
		const MergeTreeSettings & settings_ = MergeTreeSettings());

	void shutdown();
	~StorageMergeTree();

	std::string getName() const
	{
		return data.getModePrefix() + "MergeTree";
	}

	std::string getTableName() const { return data.getTableName(); }
	std::string getSignColumnName() const { return data.getSignColumnName(); }
	bool supportsSampling() const { return data.supportsSampling(); }
	bool supportsFinal() const { return data.supportsFinal(); }
	bool supportsPrewhere() const { return data.supportsPrewhere(); }

	const NamesAndTypesList & getColumnsList() const { return data.getColumnsList(); }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	BlockOutputStreamPtr write(ASTPtr query);

	/** Выполнить очередной шаг объединения кусков.
	  */
	bool optimize()
	{
		return data.optimize();
	}

	void dropImpl();

	void rename(const String & new_path_to_db, const String & new_name);

	/// Метод ALTER позволяет добавлять и удалять столбцы.
	/// Метод ALTER нужно применять, когда обращения к базе приостановлены.
	/// Например если параллельно с INSERT выполнить ALTER, то ALTER выполниться, а INSERT бросит исключение
	void alter(const ASTAlterQuery::Parameters & params);

	typedef MergeTreeData::BigLockPtr BigLockPtr;

	BigLockPtr lockAllOperations() { return data.lockAllOperations(); }

private:
	MergeTreeData data;

	StorageMergeTree(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
					const Context & context_,
					ASTPtr & primary_expr_ast_,
					const String & date_column_name_,
					const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
					size_t index_granularity_,
					MergeTreeData::Mode mode_ = MergeTreeData::Ordinary,
					const String & sign_column_ = "",
					const MergeTreeSettings & settings_ = MergeTreeSettings());
};

}
