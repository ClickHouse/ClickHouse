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

/** Intended for implementation of "rollup" - aggregation (rounding) of older data
  *  for a table with Graphite data (Graphite is the system for time series monitoring).
  *
  * Table with graphite data has at least the folowing columns (accurate to the name):
  * Path, Time, Value, Version
  *
  * Path - name of metric (sensor);
  * Time - time of measurement;
  * Value - value of measurement;
  * Version - a number, that for equal pairs of Path and Time, need to leave only record with maximum version.
  *
  * Each row in a table correspond to one value of one sensor.
  *
  * Rollup rules are specified in the following way:
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
  * regexp - pattern for sensor name
  * default - if no pattern has matched
  *
  * age - minimal data age (in seconds), to start rounding with specified precision.
  * precision - rounding precision (in seconds)
  *
  * function - name of aggregate function to be applied for values, that time was rounded to same.
  *
  * Example:
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
		Retentions retentions;	/// Must be ordered by 'age' descending.
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

	/// All columns other than 'time', 'value', 'version'. They are unmodified during rollup.
	ColumnNumbers unmodified_column_numbers;

	time_t time_of_merge;

	/// All data has been read.
	bool finished = false;

	RowRef selected_row;		/// Last row with maximum version for current primary key.
	UInt64 current_max_version = 0;

	bool is_first = true;
	StringRef current_path;
	time_t current_time = 0;
	time_t current_time_rounded = 0;
	StringRef next_path;
	time_t next_time = 0;
	time_t next_time_rounded = 0;

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
	void accumulateRow(RowRef & row);
};

}
