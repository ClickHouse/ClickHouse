#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Storages/MergeTree/MergedBlockOutputStream.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>
#include <DB/DataStreams/SummingSortedBlockInputStream.h>
#include <DB/DataStreams/AggregatingSortedBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>


namespace DB
{

/// Не будем соглашаться мерджить куски, если места на диске менее чем во столько раз больше суммарного размера кусков.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 1.6;

/// Объединяя куски, зарезервируем столько места на диске. Лучше сделать немного меньше, чем DISK_USAGE_COEFFICIENT_TO_SELECT,
/// потому что между выбором кусков и резервированием места места может стать немного меньше.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.4;


/// Выбираем отрезок из не более чем max_parts_to_merge_at_once (или несколько больше, см merge_more_parts_if_sum_bytes_is_less_than)
///  кусков так, чтобы максимальный размер был меньше чем в max_size_ratio_to_merge_parts раз больше суммы остальных.
/// Это обеспечивает в худшем случае время O(n log n) на все слияния, независимо от выбора сливаемых кусков, порядка слияния и добавления.
/// При max_parts_to_merge_at_once >= log(max_bytes_to_merge_parts) / log(max_size_ratio_to_merge_parts),
/// несложно доказать, что всегда будет что сливать, пока количество кусков больше
/// log(max_bytes_to_merge_parts) / log(max_size_ratio_to_merge_parts) * (количество кусков размером больше max_bytes_to_merge_parts).
/// Дальше эвристики.
/// Будем выбирать максимальный по включению подходящий отрезок.
/// Из всех таких выбираем отрезок с минимальным максимумом размера.
/// Из всех таких выбираем отрезок с минимальным минимумом размера.
/// Из всех таких выбираем отрезок с максимальной длиной.
/// Дополнительно:
/// 1) С 1:00 до 5:00 ограничение сверху на размер куска в основном потоке увеличивается в несколько раз.
/// 2) В зависимоти от возраста кусков меняется допустимая неравномерность при слиянии.
/// 3) Молодые куски крупного размера (примерно больше 1 ГБ) можно сливать не меньше чем по три.
/// 4) Если в одном из потоков идет мердж крупных кусков, то во втором сливать только маленькие кусочки.
/// 5) С ростом логарифма суммарного размера кусочков в мердже увеличиваем требование сбалансированности.

bool MergeTreeDataMerger::selectPartsToMerge(MergeTreeData::DataPartsVector & parts, String & merged_name, size_t available_disk_space,
	bool merge_anything_for_old_months, bool aggressive, bool only_small, const AllowedMergingPredicate & can_merge_callback)
{
	MergeTreeData::DataParts data_parts = data.getDataParts();

	const auto & date_lut = DateLUT::instance();

	size_t min_max = -1U;
	size_t min_min = -1U;
	int max_len = 0;
	MergeTreeData::DataParts::iterator best_begin;
	bool found = false;

	DayNum_t now_day = date_lut.toDayNum(time(0));
	DayNum_t now_month = date_lut.toFirstDayNumOfMonth(now_day);
	int now_hour = date_lut.toHourInaccurate(time(0));

	/// Сколько кусков, начиная с текущего, можно включить в валидный отрезок, начинающийся левее текущего куска.
	/// Нужно для определения максимальности по включению.
	int max_count_from_left = 0;

	size_t cur_max_bytes_to_merge_parts = data.settings.max_bytes_to_merge_parts;
	size_t cur_max_sum_bytes_to_merge_parts = data.settings.max_sum_bytes_to_merge_parts;

	/// Если ночь, можем мерджить сильно большие куски
	bool tonight = now_hour >= 1 && now_hour <= 5;

	if (tonight)
	{
		cur_max_bytes_to_merge_parts *= data.settings.merge_parts_at_night_inc;
		cur_max_sum_bytes_to_merge_parts *= data.settings.merge_parts_at_night_inc;
	}

	if (only_small)
		cur_max_bytes_to_merge_parts = data.settings.max_bytes_to_merge_parts_small;

	/// Мемоизация для функции can_merge_callback. Результат вызова can_merge_callback для этого куска и предыдущего в data_parts.
	std::map<MergeTreeData::DataPartPtr, bool> can_merge_with_previous;
	auto can_merge = [&can_merge_with_previous, &can_merge_callback]
		(const MergeTreeData::DataPartPtr & first, const MergeTreeData::DataPartPtr & second) -> bool
	{
		auto it = can_merge_with_previous.find(second);
		if (it != can_merge_with_previous.end())
			return it->second;
		bool res = can_merge_callback(first, second);
		can_merge_with_previous[second] = res;
		return res;
	};

	/// Найдем суммарный размер еще не пройденных кусков (то есть всех).
	size_t size_in_bytes_of_remaining_parts = 0;
	for (const auto & part : data_parts)
		size_in_bytes_of_remaining_parts += part->size_in_bytes;

	/// Левый конец отрезка.
	for (MergeTreeData::DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const MergeTreeData::DataPartPtr & first_part = *it;

		max_count_from_left = std::max(0, max_count_from_left - 1);
		size_in_bytes_of_remaining_parts -= first_part->size_in_bytes;

		/// Кусок достаточно мал или слияние "агрессивное".
		if (first_part->size_in_bytes > cur_max_bytes_to_merge_parts
			&& !aggressive)
		{
			continue;
		}

		/// Самый длинный валидный отрезок, начинающийся здесь.
		size_t cur_longest_max = -1U;
		size_t cur_longest_min = -1U;
		int cur_longest_len = 0;

		/// Текущий отрезок, не обязательно валидный.
		size_t cur_max = first_part->size_in_bytes;
		size_t cur_min = first_part->size_in_bytes;
		size_t cur_sum = first_part->size_in_bytes;
		int cur_len = 1;

		DayNum_t month = first_part->month;
		Int64 cur_id = first_part->right;

		/// Этот месяц кончился хотя бы день назад.
		bool is_old_month = now_day - now_month >= 1 && now_month > month;

		time_t newest_modification_time = first_part->modification_time;

		/// Правый конец отрезка.
		MergeTreeData::DataParts::iterator jt = it;
		while (cur_len < static_cast<int>(data.settings.max_parts_to_merge_at_once)
			|| (cur_len < static_cast<int>(data.settings.max_parts_to_merge_at_once_if_small)
				&& cur_sum < data.settings.merge_more_parts_if_sum_bytes_is_less_than))
		{
			const MergeTreeData::DataPartPtr & prev_part = *jt;
			++jt;

			if (jt == data_parts.end())
				break;

			const MergeTreeData::DataPartPtr & last_part = *jt;

			/// Кусок разрешено сливать с предыдущим, и в одном правильном месяце.
			if (last_part->month != month
				|| !can_merge(prev_part, last_part))
			{
				break;
			}

			/// Кусок достаточно мал или слияние "агрессивное".
			if (last_part->size_in_bytes > cur_max_bytes_to_merge_parts
				&& !aggressive)
				break;

			/// Кусок правее предыдущего.
			if (last_part->left < cur_id)
			{
				LOG_WARNING(log, "Part " << last_part->name << " intersects previous part");
				break;
			}

			newest_modification_time = std::max(newest_modification_time, last_part->modification_time);
			cur_max = std::max(cur_max, static_cast<size_t>(last_part->size_in_bytes));
			cur_min = std::min(cur_min, static_cast<size_t>(last_part->size_in_bytes));
			cur_sum += last_part->size_in_bytes;
			++cur_len;
			cur_id = last_part->right;

			if (cur_sum > cur_max_sum_bytes_to_merge_parts
				&& !aggressive)
				break;

			int min_len = 2;
			int cur_age_in_sec = time(0) - newest_modification_time;

			/// Если куски больше 1 Gb и образовались меньше 6 часов назад, то мерджить не меньше чем по 3.
			if (cur_max > 1024 * 1024 * 1024 && cur_age_in_sec < 6 * 3600)
				min_len = 3;

			/// Размер кусков после текущих, делить на максимальный из текущих кусков. Чем меньше, тем новее текущие куски.
			size_t oldness_coef = (size_in_bytes_of_remaining_parts + first_part->size_in_bytes - cur_sum + 0.0) / cur_max;

			/// Эвристика: если после этой группы кусков еще накопилось мало строк, не будем соглашаться на плохо
			///  сбалансированные слияния, расчитывая, что после будущих вставок данных появятся более привлекательные слияния.
			double ratio = (oldness_coef + 1) * data.settings.size_ratio_coefficient_to_merge_parts;

			/// Если отрезок валидный, то он самый длинный валидный, начинающийся тут.
			if (cur_len >= min_len
				&& (/// Достаточная равномерность размеров или пошедшее время
					static_cast<double>(cur_max) / (cur_sum - cur_max) < ratio
					/// За старый месяц объединяем что угодно, если разрешено и если этим кускам хотя бы 5 дней
					|| (is_old_month && merge_anything_for_old_months && cur_age_in_sec > 3600 * 24 * 5)
					/// Или достаточно много мелких кусков
					|| cur_len > static_cast<int>(data.settings.max_parts_to_merge_at_once)
					/// Если слияние "агрессивное", то сливаем что угодно
					|| aggressive))
			{
				/// Достаточно места на диске, чтобы покрыть новый мердж с запасом.
				if (available_disk_space > cur_sum * DISK_USAGE_COEFFICIENT_TO_SELECT)
				{
					cur_longest_max = cur_max;
					cur_longest_min = cur_min;
					cur_longest_len = cur_len;
				}
				else
				{
					time_t now = time(0);
					if (now - disk_space_warning_time > 3600)
					{
						disk_space_warning_time = now;
						LOG_WARNING(log, "Won't merge parts from " << first_part->name << " to " << last_part->name
							<< " because not enough free space: "
							<< formatReadableSizeWithBinarySuffix(available_disk_space) << " free and unreserved "
							<< "(" << formatReadableSizeWithBinarySuffix(DiskSpaceMonitor::getReservedSpace()) << " reserved in "
							<< DiskSpaceMonitor::getReservationCount() << " chunks), "
							<< formatReadableSizeWithBinarySuffix(cur_sum)
							<< " required now (+" << static_cast<int>((DISK_USAGE_COEFFICIENT_TO_SELECT - 1.0) * 100)
							<< "% on overhead); suppressing similar warnings for the next hour");
					}
					break;
				}
			}
		}

		/// Это максимальный по включению валидный отрезок.
		if (cur_longest_len > max_count_from_left)
		{
			max_count_from_left = cur_longest_len;

			if (!found
				|| std::forward_as_tuple(cur_longest_max, cur_longest_min, -cur_longest_len)
				   < std::forward_as_tuple(min_max, min_min, -max_len))
			{
				found = true;
				min_max = cur_longest_max;
				min_min = cur_longest_min;
				max_len = cur_longest_len;
				best_begin = it;
			}
		}
	}

	if (found)
	{
		parts.clear();

		DayNum_t left_date = DayNum_t(std::numeric_limits<UInt16>::max());
		DayNum_t right_date = DayNum_t(std::numeric_limits<UInt16>::min());
		UInt32 level = 0;

		MergeTreeData::DataParts::iterator it = best_begin;
		for (int i = 0; i < max_len; ++i)
		{
			parts.push_back(*it);

			level = std::max(level, parts[i]->level);
			left_date = std::min(left_date, parts[i]->left_date);
			right_date = std::max(right_date, parts[i]->right_date);

			++it;
		}

		merged_name = ActiveDataPartSet::getPartName(
			left_date, right_date, parts.front()->left, parts.back()->right, level + 1);

		LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name
			<< (only_small ? " (only small)" : ""));
	}

	return found;
}


/// parts должны быть отсортированы.
MergeTreeData::DataPartPtr MergeTreeDataMerger::mergeParts(
	const MergeTreeData::DataPartsVector & parts, const String & merged_name, MergeList::Entry & merge_entry,
	size_t aio_threshold, MergeTreeData::Transaction * out_transaction,
	DiskSpaceMonitor::Reservation * disk_reservation)
{
	merge_entry->num_parts = parts.size();

	LOG_DEBUG(log, "Merging " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name << " into " << merged_name);

	String merged_dir = data.getFullPath() + merged_name;
	if (Poco::File(merged_dir).exists())
		throw Exception("Directory " + merged_dir + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

	NameSet union_columns_set;
	for (const MergeTreeData::DataPartPtr & part : parts)
	{
		Poco::ScopedReadRWLock part_lock(part->columns_lock);
		Names part_columns = part->columns.getNames();
		union_columns_set.insert(part_columns.begin(), part_columns.end());

		merge_entry->total_size_bytes_compressed += part->size_in_bytes;
		merge_entry->total_size_marks += part->size;
	}

	NamesAndTypesList columns_list = data.getColumnsList();
	NamesAndTypesList union_columns = columns_list.filter(union_columns_set);
	Names union_column_names = union_columns.getNames();

	MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
	if (aio_threshold > 0)
	{
		for (const MergeTreeData::DataPartPtr & part : parts)
			part->accumulateColumnSizes(merged_column_to_size);
	}

	MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data);
	ActiveDataPartSet::parsePartName(merged_name, *new_data_part);
	new_data_part->name = "tmp_" + merged_name;
	new_data_part->is_temp = true;

	/** Читаем из всех кусков, сливаем и пишем в новый.
	  * Попутно вычисляем выражение для сортировки.
	  */
	BlockInputStreams src_streams;

	size_t sum_rows_approx = 0;

	const auto rows_total = merge_entry->total_size_marks * data.index_granularity;

	for (size_t i = 0; i < parts.size(); ++i)
	{
		MarkRanges ranges(1, MarkRange(0, parts[i]->size));

		auto input = std::make_unique<MergeTreeBlockInputStream>(
			data.getFullPath() + parts[i]->name + '/', DEFAULT_MERGE_BLOCK_SIZE, union_column_names, data,
			parts[i], ranges, false, nullptr, "", true, aio_threshold, DBMS_DEFAULT_BUFFER_SIZE, false);

		input->setProgressCallback([&merge_entry, rows_total] (const Progress & value)
			{
				const auto new_rows_read = __sync_add_and_fetch(&merge_entry->rows_read, value.rows);
				merge_entry->progress = static_cast<Float64>(new_rows_read) / rows_total;
				__sync_add_and_fetch(&merge_entry->bytes_read_uncompressed, value.bytes);
			});

		if (data.mode != MergeTreeData::Unsorted)
			src_streams.push_back(new MaterializingBlockInputStream{
				new ExpressionBlockInputStream(input.release(), data.getPrimaryExpression())});
		else
			src_streams.push_back(input.release());

		sum_rows_approx += parts[i]->size * data.index_granularity;
	}

	/// Порядок потоков важен: при совпадении ключа элементы идут в порядке номера потока-источника.
	/// В слитом куске строки с одинаковым ключом должны идти в порядке возрастания идентификатора исходного куска,
	///  то есть (примерного) возрастания времени вставки.
	std::unique_ptr<IProfilingBlockInputStream> merged_stream;

	switch (data.mode)
	{
		case MergeTreeData::Ordinary:
			merged_stream = std::make_unique<MergingSortedBlockInputStream>(
				src_streams, data.getSortDescription(), DEFAULT_MERGE_BLOCK_SIZE);
			break;

		case MergeTreeData::Collapsing:
			merged_stream = std::make_unique<CollapsingSortedBlockInputStream>(
				src_streams, data.getSortDescription(), data.sign_column, DEFAULT_MERGE_BLOCK_SIZE);
			break;

		case MergeTreeData::Summing:
			merged_stream = std::make_unique<SummingSortedBlockInputStream>(
				src_streams, data.getSortDescription(), data.columns_to_sum, DEFAULT_MERGE_BLOCK_SIZE);
			break;

		case MergeTreeData::Aggregating:
			merged_stream = std::make_unique<AggregatingSortedBlockInputStream>(
				src_streams, data.getSortDescription(), DEFAULT_MERGE_BLOCK_SIZE);
			break;

		case MergeTreeData::Unsorted:
			merged_stream = std::make_unique<ConcatBlockInputStream>(src_streams);
			break;

		default:
			throw Exception("Unknown mode of operation for MergeTreeData: " + toString(data.mode), ErrorCodes::LOGICAL_ERROR);
	}

	const String new_part_tmp_path = data.getFullPath() + "tmp_" + merged_name + "/";

	auto compression_method = data.context.chooseCompressionMethod(
		merge_entry->total_size_bytes_compressed,
		static_cast<double>(merge_entry->total_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

	MergedBlockOutputStream to{data, new_part_tmp_path, union_columns, compression_method, merged_column_to_size, aio_threshold};

	merged_stream->readPrefix();
	to.writePrefix();

	size_t rows_written = 0;
	const size_t initial_reservation = disk_reservation ? disk_reservation->getSize() : 0;

	Block block;
	while (!isCancelled() && (block = merged_stream->read()))
	{
		rows_written += block.rows();
		to.write(block);

		merge_entry->rows_written = merged_stream->getInfo().rows;
		merge_entry->bytes_written_uncompressed = merged_stream->getInfo().bytes;

		if (disk_reservation)
			disk_reservation->update(static_cast<size_t>((1 - std::min(1., 1. * rows_written / sum_rows_approx)) * initial_reservation));
	}

	if (isCancelled())
		throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

	merged_stream->readSuffix();
	new_data_part->columns = union_columns;
	new_data_part->checksums = to.writeSuffixAndGetChecksums();
	new_data_part->index.swap(to.getIndex());

	/// Для удобства, даже CollapsingSortedBlockInputStream не может выдать ноль строк.
	if (0 == to.marksCount())
		throw Exception("Empty part after merge", ErrorCodes::LOGICAL_ERROR);

	new_data_part->size = to.marksCount();
	new_data_part->modification_time = time(0);
	new_data_part->size_in_bytes = MergeTreeData::DataPart::calcTotalSize(new_part_tmp_path);

	/// Переименовываем новый кусок, добавляем в набор и убираем исходные куски.
	auto replaced_parts = data.renameTempPartAndReplace(new_data_part, nullptr, out_transaction);

	if (new_data_part->name != merged_name)
		throw Exception("Unexpected part name: " + new_data_part->name + " instead of " + merged_name, ErrorCodes::LOGICAL_ERROR);

	/// Проверим, что удалились все исходные куски и только они.
	if (replaced_parts.size() != parts.size())
	{
		/** Это нормально, хотя такое бывает редко.
		  * Ситуация - было заменено 0 кусков вместо N может быть, например, в следующем случае:
		  * - у нас был кусок A, но не было куска B и C;
		  * - в очереди был мердж A, B -> AB, но его не делали, так как куска B нет;
		  * - в очереди был мердж AB, C -> ABC, но его не делали, так как куска AB и C нет;
		  * - мы выполнили задачу на скачивание куска B;
		  * - мы начали делать мердж A, B -> AB, так как все куски появились;
		  * - мы решили скачать с другой реплики кусок ABC, так как невозможно было сделать мердж AB, C -> ABC;
		  * - кусок ABC появился, при его добавлении, были удалены старые куски A, B, C;
		  * - мердж AB закончился. Добавился кусок AB. Но это устаревший кусок. В логе будет сообщение Obsolete part added,
		  *   затем попадаем сюда.
		  * Ситуация - было заменено M > N кусков тоже нормальная.
		  *
		  * Хотя это должно предотвращаться проверкой в методе StorageReplicatedMergeTree::shouldExecuteLogEntry.
		  */
		LOG_WARNING(log, "Unexpected number of parts removed when adding " << new_data_part->name << ": " << replaced_parts.size()
			<< " instead of " << parts.size());
	}
	else
	{
		for (size_t i = 0; i < parts.size(); ++i)
			if (parts[i]->name != replaced_parts[i]->name)
				throw Exception("Unexpected part removed when adding " + new_data_part->name + ": " + replaced_parts[i]->name
					+ " instead of " + parts[i]->name, ErrorCodes::LOGICAL_ERROR);
	}

	LOG_TRACE(log, "Merged " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name);

	return new_data_part;
}

size_t MergeTreeDataMerger::estimateDiskSpaceForMerge(const MergeTreeData::DataPartsVector & parts)
{
	size_t res = 0;
	for (const MergeTreeData::DataPartPtr & part : parts)
		res += part->size_in_bytes;

	return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

}
