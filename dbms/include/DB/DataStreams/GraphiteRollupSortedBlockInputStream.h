#pragma once

#include <common/logger_useful.h>

#include <DB/Core/Row.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Common/OptimizedRegularExpression.h>


namespace DB
{

/** Предназначен для реализации "rollup" - загрубления старых данных
  *  для таблицы с данными Графита - системы количественного мониторинга.
  *
  * Таблица с данными Графита имеет по крайней мере следующие столбцы (с точностью до названия):
  * Path, Time, Value, Version
  *
  * Path - имя метрики (сенсора);
  * Time - время, к которому относится измерение;
  * Value - значение;
  * Version - число, такое что для одинаковых пар Path, Time, следует брать значение максимальной версии.
  *
  * Каждая строчка в таблице соответствует одному значению одного сенсора.
  *
  * Правила загрубления задаются в следующем виде:
  *
  * pattern
  * 	regexp
  * 	function
  * 	age -> precision
  * 	age -> precision
  * 	...
  * pattern
  * 	...
  * default
  * 	function
  *		age -> precision
  * 	...
  *
  * regexp - шаблон на имя сенсора
  * default - если ни один шаблон не сработал
  *
  * age - минимальный возраст данных (в секундах), после которого следует применять округление до величины precision.
  * precision - величина округления (в секундах)
  *
  * function - имя агрегатной функции, которую следует применять к значениям, время которых округлилось до одинакового.
  *
  * Пример:
  *
  * <graphite_rollup>
  * 	<pattern>
  * 		<regexp>click_cost</regexp>
  * 		<function>any</function>
  * 		<retention>
  * 			<age>0</age>
  * 			<precision>5</precision>
  * 		</retention>
  * 		<retention>
  * 			<age>86400</age>
  * 			<precision>60</precision>
  * 		</retention>
  * 	</pattern>
  * 	<default>
  * 		<function>max</function>
  * 		<retention>
  * 			<age>0</age>
  * 			<precision>60</precision>
  * 		</retention>
  * 		<retention>
  * 			<age>3600</age>
  * 			<precision>300</precision>
  * 		</retention>
  * 		<retention>
  * 			<age>86400</age>
  * 			<precision>3600</precision>
  * 		</retention>
  * 	</default>
  * </graphite_rollup>
  */

namespace Graphite
{
	struct Retention
	{
		UInt32 age;
		UInt32 precision;
	};

	using Retentions = std::vector<Retention>;

	struct Pattern
	{
		std::shared_ptr<OptimizedRegularExpression> regexp;
		AggregateFunctionPtr function;
		Retentions retentions;	/// Должны быть упорядочены по убыванию age.
	};

	using Patterns = std::vector<Pattern>;

	struct Params
	{
		String path_column_name;
		String time_column_name;
		String value_column_name;
		String version_column_name;
		Graphite::Patterns patterns;
	};
}

/** Соединяет несколько сортированных потоков в один.
  *
  * При этом, для каждой группы идущих подряд одинаковых значений столбца path,
  *  и одинаковых значений time с округлением до некоторой точности
  *  (где точность округления зависит от набора шаблонов на path
  *   и количества времени, прошедшего от time до заданного времени),
  * оставляет одну строку,
  *  выполняя округление времени,
  *  слияние значений value, используя заданные агрегатные функции,
  *  а также оставляя максимальное значение столбца version.
  */
class GraphiteRollupSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
	GraphiteRollupSortedBlockInputStream(
		BlockInputStreams inputs_, const SortDescription & description_, size_t max_block_size_,
		const Graphite::Params & params, time_t time_of_merge)
		: MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
		params(params), time_of_merge(time_of_merge)
	{
	}

	String getName() const override { return "GraphiteRollupSorted"; }

	String getID() const override
	{
		std::stringstream res;
		res << "GraphiteRollupSorted(inputs";

		for (size_t i = 0; i < children.size(); ++i)
			res << ", " << children[i]->getID();

		res << ", description";

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

    ~GraphiteRollupSortedBlockInputStream()
	{
		if (current_pattern)
			current_pattern->function->destroy(place_for_aggregate_state.data());
	}

protected:
	Block readImpl() override;

private:
	Logger * log = &Logger::get("GraphiteRollupSortedBlockInputStream");

	const Graphite::Params params;

	size_t path_column_num;
	size_t time_column_num;
	size_t value_column_num;
	size_t version_column_num;

	/// Все столбцы кроме time, value, version. Они вставляются без модификации.
	ColumnNumbers unmodified_column_numbers;

	time_t time_of_merge;

	/// Прочитали до конца.
	bool finished = false;

	/// Владеет столбцом, пока на него ссылается current_path.
	SharedBlockPtr owned_current_block;

	bool is_first = true;
	StringRef current_path;
	time_t current_time;
	StringRef next_path;
	time_t next_time;
	UInt64 current_max_version = 0;

	const Graphite::Pattern * current_pattern = nullptr;
	std::vector<char> place_for_aggregate_state;

	const Graphite::Pattern * selectPatternForPath(StringRef path) const;
	UInt32 selectPrecision(const Graphite::Retentions & retentions, time_t time) const;


	template <typename TSortCursor>
	void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	/// Вставить значения в результирующие столбцы, которые не будут меняться в дальнейшем.
	template <class TSortCursor>
	void startNextRow(ColumnPlainPtrs & merged_columns, TSortCursor & cursor);

	/// Вставить в результирующие столбцы вычисленные значения time, value, version по последней группе строк.
	void finishCurrentRow(ColumnPlainPtrs & merged_columns);

	/// Обновить состояние агрегатной функции новым значением value.
	template <class TSortCursor>
	void accumulateRow(TSortCursor & cursor);
};

}
