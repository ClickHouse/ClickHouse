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

	std::string getName() const { return "ChunkRef"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return getSource().getColumnsListAsterisk(); }
	/// В таблице, на которую мы ссылаемся, могут быть виртуальные столбцы.
	NameAndTypePair getColumn(const String &column_name) const { return getSource().getColumn(column_name); };
	bool hasColumn(const String &column_name) const { return getSource().hasColumn(column_name); };

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	ASTPtr getCustomCreateQuery(const Context & context) const;

	void drop() override;

	String source_database_name;
	String source_table_name;

	bool checkData() const override;

private:
	String name;
	const Context & context;

	StorageChunkRef(const std::string & name_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach);

	/// TODO: может быть, можно просто хранить указатель на родительскую таблицу?
	StorageChunks & getSource();
	const StorageChunks & getSource() const;
};

}
