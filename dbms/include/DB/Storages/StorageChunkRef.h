#pragma once

#include <DB/Storages/StorageChunks.h>


namespace DB
{

/** Ссылка на кусок данных в таблице типа Chunks.
	* Запись не поддерживается.
	*/
class StorageChunkRef : public IStorage
{
public:
	static StoragePtr create(const std::string & name_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach);

	std::string getName() const override { return "ChunkRef"; }
	std::string getTableName() const override { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return typeid_cast<const StorageChunks &>(*getSource()).getColumnsListImpl(); }
	/// В таблице, на которую мы ссылаемся, могут быть виртуальные столбцы.
	NameAndTypePair getColumn(const String & column_name) const override { return getSource()->getColumn(column_name); };
	bool hasColumn(const String & column_name) const override { return getSource()->hasColumn(column_name); };

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	ASTPtr getCustomCreateQuery(const Context & context) const override;

	void drop() override;

	String source_database_name;
	String source_table_name;

	bool checkData() const override;

private:
	String name;
	const Context & context;

	StorageChunkRef(const std::string & name_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach);

	/// TODO: может быть, можно просто хранить указатель на родительскую таблицу?
	StoragePtr getSource();
	const StoragePtr getSource() const;
};

}
