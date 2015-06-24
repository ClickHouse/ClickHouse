#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/MergeTree/RangesInDataPart.h>
#include <statdaemons/ext/range.hpp>
#include <mutex>


namespace DB
{


struct MergeTreeReadTask
{
	MergeTreeData::DataPartPtr data_part;
	MarkRanges mark_ranges;
	std::size_t part_index_in_query;
	const Names & ordered_names;
	const NameSet & column_name_set;
	const NamesAndTypesList & columns;
	const NamesAndTypesList & pre_columns;
	const bool remove_prewhere_column;

	MergeTreeReadTask(const MergeTreeData::DataPartPtr & data_part, const MarkRanges & ranges,
					  const std::size_t part_index_in_query, const Names & ordered_names,
					  const NameSet & column_name_set, const NamesAndTypesList & columns,
					  const NamesAndTypesList & pre_columns, const bool remove_prewhere_column)
		: data_part{data_part}, mark_ranges{ranges}, part_index_in_query{part_index_in_query},
		  ordered_names{ordered_names}, column_name_set{column_name_set}, columns{columns}, pre_columns{pre_columns},
		  remove_prewhere_column{remove_prewhere_column}
	{}
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

class MergeTreeReadPool
{
public:
	MergeTreeReadPool(const RangesInDataParts & parts, const std::vector<std::size_t> & per_part_sum_marks,
					  const std::size_t sum_marks, MergeTreeData & data, const ExpressionActionsPtr & prewhere_actions,
					  const String & prewhere_column_name, const bool check_columns, const Names & column_names)
		: parts{parts}, per_part_sum_marks{per_part_sum_marks}, sum_marks{sum_marks}, data{data}
	{
		fillPerPartInfo(column_names, prewhere_actions, prewhere_column_name, check_columns);
	}

	MergeTreeReadPool(const MergeTreeReadPool &) = delete;
	MergeTreeReadPool & operator=(const MergeTreeReadPool &) = delete;

	MergeTreeReadTaskPtr getTask(const std::size_t min_marks_to_read)
	{
		const std::lock_guard<std::mutex> lock{mutex};

		if (0 == sum_marks)
			return nullptr;

		/// @todo use map to speedup lookup
		/// find a part which has marks remaining
		std::size_t part_id = 0;
		for (; part_id < parts.size(); ++part_id)
			if (0 != per_part_sum_marks[part_id])
				break;

		auto & part = parts[part_id];
		const auto & ordered_names = per_part_ordered_names[part_id];
		const auto & column_name_set = per_part_column_name_set[part_id];
		const auto & columns = per_part_columns[part_id];
		const auto & pre_columns = per_part_pre_columns[part_id];
		const auto remove_prewhere_column = per_part_remove_prewhere_column[part_id];
		auto & marks_in_part = per_part_sum_marks[part_id];

		/// Берём весь кусок, если он достаточно мал
		auto need_marks = std::min(marks_in_part, min_marks_to_read);

		/// Не будем оставлять в куске слишком мало строк.
		if (marks_in_part > need_marks &&
			marks_in_part - need_marks < min_marks_to_read)
			need_marks = marks_in_part;

		MarkRanges ranges_to_get_from_part;

		/// Возьмем весь кусок, если он достаточно мал.
		if (marks_in_part <= need_marks)
		{
			const auto marks_to_get_from_range = marks_in_part;

			/// Восстановим порядок отрезков.
			std::reverse(part.ranges.begin(), part.ranges.end());

			ranges_to_get_from_part = part.ranges;

			marks_in_part -= marks_to_get_from_range;
			sum_marks -= marks_to_get_from_range;
		}
		else
		{
			/// Цикл по отрезкам куска.
			while (need_marks > 0 && !part.ranges.empty())
			{
				auto & range = part.ranges.back();

				const std::size_t marks_in_range = range.end - range.begin;
				const std::size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

				ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
				range.begin += marks_to_get_from_range;
				if (range.begin == range.end)
					part.ranges.pop_back();

				marks_in_part -= marks_to_get_from_range;
				need_marks -= marks_to_get_from_range;
				sum_marks -= marks_to_get_from_range;
			}
		}

		return std::make_unique<MergeTreeReadTask>(
			part.data_part, ranges_to_get_from_part, part.part_index_in_query, ordered_names, column_name_set, columns,
			pre_columns, remove_prewhere_column);
	}

public:
	void fillPerPartInfo(const Names & column_names, const ExpressionActionsPtr & prewhere_actions,
						 const String & prewhere_column_name, const bool check_columns)
	{
		for (const auto & part : parts)
		{
			per_part_columns_lock.push_back(std::make_unique<Poco::ScopedReadRWLock>(
				part.data_part->columns_lock));

			/// inject column names required for DEFAULT evaluation in current part
			auto required_column_names = column_names;

			const auto injected_columns = injectRequiredColumns(part.data_part, required_column_names);

			/// insert injected columns into ordered columns list to avoid exception about different block structures
			auto ordered_names = column_names;
			ordered_names.insert(std::end(ordered_names), std::begin(injected_columns), std::end(injected_columns));
			per_part_ordered_names.emplace_back(ordered_names);

			Names required_pre_column_names;

			if (prewhere_actions)
			{
				/// collect columns required for PREWHERE evaluation
				/// @todo minimum size column may be added here due to const condition, thus invalidating ordered_names
				required_pre_column_names = prewhere_actions->getRequiredColumns();

				/// there must be at least one column required for PREWHERE
				if (required_pre_column_names.empty())
					required_pre_column_names.push_back(required_column_names[0]);

				/// PREWHERE columns may require some additional columns for DEFAULT evaluation
				(void) injectRequiredColumns(part.data_part, required_pre_column_names);

				/// will be used to distinguish between PREWHERE and WHERE columns when applying filter
				const NameSet pre_name_set{
					std::begin(required_pre_column_names), std::end(required_pre_column_names)
				};
				/** Если выражение в PREWHERE - не столбец таблицы, не нужно отдавать наружу столбец с ним
				 *	(от storage ожидают получить только столбцы таблицы). */
				per_part_remove_prewhere_column.push_back(0 == pre_name_set.count(prewhere_column_name));

				Names post_column_names;
				for (const auto & name : required_column_names)
					if (!pre_name_set.count(name))
						post_column_names.push_back(name);

				required_column_names = post_column_names;
			}
			else
				per_part_remove_prewhere_column.push_back(false);

			per_part_column_name_set.emplace_back(std::begin(required_column_names), std::end(required_column_names));

			if (check_columns)
			{
				/** Под part->columns_lock проверим, что все запрошенные столбцы в куске того же типа, что в таблице.
				 *	Это может быть не так во время ALTER MODIFY. */
				if (!required_pre_column_names.empty())
					data.check(part.data_part->columns, required_pre_column_names);
				if (!required_column_names.empty())
					data.check(part.data_part->columns, required_column_names);

				per_part_pre_columns.push_back(data.getColumnsList().addTypes(required_pre_column_names));
				per_part_columns.push_back(data.getColumnsList().addTypes(required_column_names));
			}
			else
			{
				per_part_pre_columns.push_back(part.data_part->columns.addTypes(required_pre_column_names));
				per_part_columns.push_back(part.data_part->columns.addTypes(required_column_names));
			}
		}
	}

	/** Если некоторых запрошенных столбцов нет в куске,
	 *	то выясняем, какие столбцы может быть необходимо дополнительно прочитать,
	 *	чтобы можно было вычислить DEFAULT выражение для этих столбцов.
	 *	Добавляет их в columns. */
	NameSet injectRequiredColumns(const MergeTreeData::DataPartPtr & part, Names & columns) const
	{
		NameSet required_columns{std::begin(columns), std::end(columns)};
		NameSet injected_columns;

		for (size_t i = 0; i < columns.size(); ++i)
		{
			const auto & column_name = columns[i];

			/// column has files and hence does not require evaluation
			if (part->hasColumnFiles(column_name))
				continue;

			const auto default_it = data.column_defaults.find(column_name);
			/// columns has no explicit default expression
			if (default_it == std::end(data.column_defaults))
				continue;

			/// collect identifiers required for evaluation
			IdentifierNameSet identifiers;
			default_it->second.expression->collectIdentifierNames(identifiers);

			for (const auto & identifier : identifiers)
			{
				if (data.hasColumn(identifier))
				{
					/// ensure each column is added only once
					if (required_columns.count(identifier) == 0)
					{
						columns.emplace_back(identifier);
						required_columns.emplace(identifier);
						injected_columns.emplace(identifier);
					}
				}
			}
		}

		return injected_columns;
	}

	std::vector<std::unique_ptr<Poco::ScopedReadRWLock>> per_part_columns_lock;
	RangesInDataParts parts;
	std::vector<std::size_t> per_part_sum_marks;
	std::size_t sum_marks;
	MergeTreeData & data;
	std::vector<Names> per_part_ordered_names;
	std::vector<NameSet> per_part_column_name_set;
	std::vector<NamesAndTypesList> per_part_columns;
	std::vector<NamesAndTypesList> per_part_pre_columns;
	/// @todo actually all of these values are either true or false for the whole query, thus no vector required
	std::vector<bool> per_part_remove_prewhere_column;

	mutable std::mutex mutex;
};

using MergeTreeReadPoolPtr = std::shared_ptr<MergeTreeReadPool>;


}
