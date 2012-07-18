#pragma once

#include <statdaemons/Increment.h>

#include <DB/Core/SortDescription.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Expression.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

/** Движок, использующий merge tree для инкрементальной сортировки данных.
  * Таблица представлена набором сортированных кусков.
  * При вставке, данные сортируются по указанному выражению (первичному ключу) и пишутся в новый кусок.
  * Куски объединяются в фоне, согласно некоторой эвристике.
  * Для каждого куска, создаётся индексный файл, содержащий значение первичного ключа для каждой n-ой строки.
  * Таким образом, реализуется эффективная выборка по диапазону первичного ключа.
  *
  * Дополнительно:
  * 
  *  Указывается столбец, содержащий дату.
  *  Для каждого куска пишется минимальная и максимальная дата.
  *  (по сути - ещё один индекс)
  * 
  *  Данные разделяются по разным месяцам (пишутся в разные куски для разных месяцев).
  *  Куски для разных месяцев не объединяются - для простоты эксплуатации.
  *  (дают локальность обновлений, что удобно для синхронизации и бэкапа)
  *
  * Структура файлов:
  *  / increment.txt - файл, содержащий одно число, увеличивающееся на 1 - для генерации идентификаторов кусков.
  *  / min-date _ max-date _ min-id _ max-id _ level / - директория с куском.
  *  / min-date _ max-date _ min-id _ max-id _ level / primary.idx - индексный файл.
  * Внутри директории с куском:
  *  Column.bin - данные столбца
  *  Column.mrk - засечки, указывающие, откуда начинать чтение, чтобы пропустить n * k строк.
  */
class StorageMergeTree : public IStorage
{
friend class MergeTreeBlockOutputStream;

public:
	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов.
	  *
	  * primary_expr 		- выражение для сортировки;
	  * date_column_name 	- имя столбца с датой;
	  * index_granularity 	- на сколько строчек пишется одно значение индекса.
	  */
	StorageMergeTree(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
		Context & context_,
		ASTPtr & primary_expr_ast_, const String & date_column_name_,
		size_t index_granularity_);

	std::string getName() const { return "MergeTree"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	/** При чтении, выбирается набор кусков, покрывающий нужный диапазон индекса.
	  */
/*	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);*/

	/** При записи, данные сортируются и пишутся в новые куски.
	  */
	BlockOutputStreamPtr write(
		ASTPtr query);

	/** Выполнить очередной шаг объединения кусков.
	  * Для нормальной работы, этот метод требуется вызывать постоянно.
	  * (С некоторой задержкой, если возвращено has_more_work = false.)
	  */
//	void optimize(bool & done_something, bool & has_more_work);

//	void drop();
	
//	void rename(const String & new_path_to_db, const String & new_name);

private:
	String path;
	String name;
	String full_path;
	NamesAndTypesListPtr columns;

	Context context;
	ASTPtr primary_expr_ast;
	String date_column_name;
	size_t index_granularity;

	SharedPtr<Expression> primary_expr;
	SortDescription sort_descr;

	Increment increment;

	static String getPartName(Yandex::DayNum_t left_month, Yandex::DayNum_t right_month, UInt64 left_id, UInt64 right_id, UInt64 level);
};

}
