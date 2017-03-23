#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
	extern const int MEMORY_LIMIT_EXCEEDED;
}


MergeTreeBlockInputStream::~MergeTreeBlockInputStream() = default;


MergeTreeBlockInputStream::MergeTreeBlockInputStream(const String & path_,	/// path to part
	size_t block_size_, Names column_names,
	MergeTreeData & storage_, const MergeTreeData::DataPartPtr & owned_data_part_,
	const MarkRanges & mark_ranges_, bool use_uncompressed_cache_,
	ExpressionActionsPtr prewhere_actions_, String prewhere_column_, bool check_columns,
	size_t min_bytes_to_use_direct_io_, size_t max_read_buffer_size_,
	bool save_marks_in_cache_, bool quiet)
	:
	path(path_), block_size(block_size_),
	storage(storage_), owned_data_part(owned_data_part_),
	part_columns_lock(new Poco::ScopedReadRWLock(owned_data_part->columns_lock)),
	all_mark_ranges(mark_ranges_), remaining_mark_ranges(mark_ranges_),
	use_uncompressed_cache(use_uncompressed_cache_),
	prewhere_actions(prewhere_actions_), prewhere_column(prewhere_column_),
	log(&Logger::get("MergeTreeBlockInputStream")),
	ordered_names{column_names},
	min_bytes_to_use_direct_io(min_bytes_to_use_direct_io_), max_read_buffer_size(max_read_buffer_size_),
	save_marks_in_cache(save_marks_in_cache_)
{
	try
	{
		/** @note you could simply swap `reverse` in if and else branches of MergeTreeDataSelectExecutor,
		  * and remove this reverse. */
		std::reverse(remaining_mark_ranges.begin(), remaining_mark_ranges.end());

		/// inject columns required for defaults evaluation
		const auto injected_columns = injectRequiredColumns(column_names);
		should_reorder = !injected_columns.empty();

		Names pre_column_names;

		if (prewhere_actions)
		{
			pre_column_names = prewhere_actions->getRequiredColumns();

			if (pre_column_names.empty())
				pre_column_names.push_back(column_names[0]);

			const auto injected_pre_columns = injectRequiredColumns(pre_column_names);
			if (!injected_pre_columns.empty())
				should_reorder = true;

			const NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());
			/// If the expression in PREWHERE is not a column of the table, you do not need to output a column with it
			///  (from storage expect to receive only the columns of the table).
			remove_prewhere_column = !pre_name_set.count(prewhere_column);

			Names post_column_names;
			for (const auto & name : column_names)
				if (!pre_name_set.count(name))
					post_column_names.push_back(name);

			column_names = post_column_names;
		}

		/// will be used to distinguish between PREWHERE and WHERE columns when applying filter
		column_name_set = NameSet{column_names.begin(), column_names.end()};

		if (check_columns)
		{
			/// Under owned_data_part->columns_lock we check that all requested columns are of the same type as in the table.
			/// This may be not true in case of ALTER MODIFY.
			if (!pre_column_names.empty())
				storage.check(owned_data_part->columns, pre_column_names);
			if (!column_names.empty())
				storage.check(owned_data_part->columns, column_names);

			pre_columns = storage.getColumnsList().addTypes(pre_column_names);
			columns = storage.getColumnsList().addTypes(column_names);
		}
		else
		{
			pre_columns = owned_data_part->columns.addTypes(pre_column_names);
			columns = owned_data_part->columns.addTypes(column_names);
		}

		/// Let's estimate total number of rows for progress bar.
		size_t total_rows = 0;
		for (const auto & range : all_mark_ranges)
			total_rows += range.end - range.begin;
		total_rows *= storage.index_granularity;

		if (!quiet)
		LOG_TRACE(log, "Reading " << all_mark_ranges.size() << " ranges from part " << owned_data_part->name
			<< ", approx. " << total_rows
			<< (all_mark_ranges.size() > 1
				? ", up to " + toString((all_mark_ranges.back().end - all_mark_ranges.front().begin) * storage.index_granularity)
				: "")
			<< " rows starting from " << all_mark_ranges.front().begin * storage.index_granularity);

		setTotalRowsApprox(total_rows);
	}
	catch (const Exception & e)
	{
		/// Suspicion of the broken part. A part is added to the queue for verification.
		if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
			storage.reportBrokenPart(owned_data_part->name);
		throw;
	}
	catch (...)
	{
		storage.reportBrokenPart(owned_data_part->name);
		throw;
	}
}


String MergeTreeBlockInputStream::getID() const
{
	std::stringstream res;
	res << "MergeTree(" << path << ", columns";

	for (const NameAndTypePair & column : columns)
		res << ", " << column.name;

	if (prewhere_actions)
		res << ", prewhere, " << prewhere_actions->getID();

	res << ", marks";

	for (size_t i = 0; i < all_mark_ranges.size(); ++i)
		res << ", " << all_mark_ranges[i].begin << ", " << all_mark_ranges[i].end;

	res << ")";
	return res.str();
}


NameSet MergeTreeBlockInputStream::injectRequiredColumns(Names & columns) const
{
	NameSet required_columns{std::begin(columns), std::end(columns)};
	NameSet injected_columns;

	auto all_column_files_missing = true;

	for (size_t i = 0; i < columns.size(); ++i)
	{
		const auto & column_name = columns[i];

		/// column has files and hence does not require evaluation
		if (owned_data_part->hasColumnFiles(column_name))
		{
			all_column_files_missing = false;
			continue;
		}

		const auto default_it = storage.column_defaults.find(column_name);
		/// columns has no explicit default expression
		if (default_it == std::end(storage.column_defaults))
			continue;

		/// collect identifiers required for evaluation
		IdentifierNameSet identifiers;
		default_it->second.expression->collectIdentifierNames(identifiers);

		for (const auto & identifier : identifiers)
		{
			if (storage.hasColumn(identifier))
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

	/// If all files are missing read at least one column to determine correct column sizes
	if (all_column_files_missing)
	{
		const auto minimum_size_column_name = owned_data_part->getColumnNameWithMinumumCompressedSize();
		columns.push_back(minimum_size_column_name);
		injected_columns.insert(std::move(minimum_size_column_name));
	}

	return injected_columns;
}


Block MergeTreeBlockInputStream::readImpl()
{
	Block res;

	if (remaining_mark_ranges.empty())
		return res;

	if (!reader)
	{
		if (use_uncompressed_cache)
			owned_uncompressed_cache = storage.context.getUncompressedCache();

		owned_mark_cache = storage.context.getMarkCache();

		reader = std::make_unique<MergeTreeReader>(
			path, owned_data_part, columns, owned_uncompressed_cache.get(),
			owned_mark_cache.get(), save_marks_in_cache, storage,
			all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);

		if (prewhere_actions)
			pre_reader = std::make_unique<MergeTreeReader>(
				path, owned_data_part, pre_columns, owned_uncompressed_cache.get(),
				owned_mark_cache.get(), save_marks_in_cache, storage,
				all_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size);
	}

	if (prewhere_actions)
	{
		do
		{
			/// Let's read the full block of columns needed to calculate the expression in PREWHERE.
			size_t space_left = std::max(1LU, block_size / storage.index_granularity);
			MarkRanges ranges_to_read;
			while (!remaining_mark_ranges.empty() && space_left && !isCancelled())
			{
				MarkRange & range = remaining_mark_ranges.back();

				size_t marks_to_read = std::min(range.end - range.begin, space_left);
				pre_reader->readRange(range.begin, range.begin + marks_to_read, res);

				ranges_to_read.push_back(MarkRange(range.begin, range.begin + marks_to_read));
				space_left -= marks_to_read;
				range.begin += marks_to_read;
				if (range.begin == range.end)
					remaining_mark_ranges.pop_back();
			}

			/// In the case of isCancelled.
			if (!res)
				return res;

			progressImpl(Progress(res.rows(), res.bytes()));
			pre_reader->fillMissingColumns(res, ordered_names, should_reorder);

			/// Compute the expression in PREWHERE.
			prewhere_actions->execute(res);

			ColumnPtr column = res.getByName(prewhere_column).column;
			if (remove_prewhere_column)
				res.erase(prewhere_column);

			size_t pre_bytes = res.bytes();

			ColumnPtr observed_column;
			if (column->isNullable())
			{
				const ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*column);
				observed_column = nullable_col.getNestedColumn();
			}
			else
				observed_column = column;

			/** If the filter is a constant (for example, it says PREWHERE 1),
				*  then either return an empty block, or return the block unchanged.
				*/
			if (const ColumnConstUInt8 * column_const = typeid_cast<const ColumnConstUInt8 *>(observed_column.get()))
			{
				if (!column_const->getData())
				{
					res.clear();
					return res;
				}

				for (size_t i = 0; i < ranges_to_read.size(); ++i)
				{
					const MarkRange & range = ranges_to_read[i];
					reader->readRange(range.begin, range.end, res);
				}

				progressImpl(Progress(0, res.bytes() - pre_bytes));
			}
			else if (const ColumnUInt8 * column_vec = typeid_cast<const ColumnUInt8 *>(observed_column.get()))
			{
				size_t index_granularity = storage.index_granularity;

				const IColumn::Filter & pre_filter = column_vec->getData();
				IColumn::Filter post_filter(pre_filter.size());

				/// Let's read the rest of the columns in the required segments and compose our own filter for them.
				size_t pre_filter_pos = 0;
				size_t post_filter_pos = 0;
				for (size_t i = 0; i < ranges_to_read.size(); ++i)
				{
					const MarkRange & range = ranges_to_read[i];

					size_t begin = range.begin;
					size_t pre_filter_begin_pos = pre_filter_pos;
					for (size_t mark = range.begin; mark <= range.end; ++mark)
					{
						UInt8 nonzero = 0;
						if (mark != range.end)
						{
							size_t limit = std::min(pre_filter.size(), pre_filter_pos + index_granularity);
							for (size_t row = pre_filter_pos; row < limit; ++row)
								nonzero |= pre_filter[row];
						}
						if (!nonzero)
						{
							if (mark > begin)
							{
								memcpy(
									&post_filter[post_filter_pos],
									&pre_filter[pre_filter_begin_pos],
									pre_filter_pos - pre_filter_begin_pos);
								post_filter_pos += pre_filter_pos - pre_filter_begin_pos;
								reader->readRange(begin, mark, res);
							}
							begin = mark + 1;
							pre_filter_begin_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
						}
						if (mark < range.end)
							pre_filter_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
					}
				}

				if (!post_filter_pos)
				{
					res.clear();
					continue;
				}

				progressImpl(Progress(0, res.bytes() - pre_bytes));

				post_filter.resize(post_filter_pos);

				/// Filter the columns related to PREWHERE using pre_filter,
				///  other columns - using post_filter.
				size_t rows = 0;
				for (size_t i = 0; i < res.columns(); ++i)
				{
					ColumnWithTypeAndName & column = res.safeGetByPosition(i);
					if (column.name == prewhere_column && res.columns() > 1)
						continue;
					column.column = column.column->filter(column_name_set.count(column.name) ? post_filter : pre_filter, -1);
					rows = column.column->size();
				}

				/// Replace column with condition value from PREWHERE to constant.
				if (!remove_prewhere_column)
					res.getByName(prewhere_column).column = std::make_shared<ColumnConstUInt8>(rows, 1);
			}
			else
				throw Exception("Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

			if (res)
				reader->fillMissingColumnsAndReorder(res, ordered_names);
		}
		while (!remaining_mark_ranges.empty() && !res && !isCancelled());
	}
	else
	{
		size_t space_left = std::max(1LU, block_size / storage.index_granularity);
		while (!remaining_mark_ranges.empty() && space_left && !isCancelled())
		{
			MarkRange & range = remaining_mark_ranges.back();

			size_t marks_to_read = std::min(range.end - range.begin, space_left);
			reader->readRange(range.begin, range.begin + marks_to_read, res);

			space_left -= marks_to_read;
			range.begin += marks_to_read;
			if (range.begin == range.end)
				remaining_mark_ranges.pop_back();
		}

		/// In the case of isCancelled.
		if (!res)
			return res;

		progressImpl(Progress(res.rows(), res.bytes()));
		reader->fillMissingColumns(res, ordered_names);
	}

	if (remaining_mark_ranges.empty())
	{
		/** Close the files (before destroying the object).
		  * When many sources are created, but simultaneously reading only a few of them,
		  * buffers don't waste memory.
		  */
		reader.reset();
		pre_reader.reset();
		part_columns_lock.reset();
		owned_data_part.reset();
	}

	return res;
}


}
