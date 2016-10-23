#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{


/** Advanced settings of MergeTree.
  * Could be loaded from config.
  */
struct MergeTreeSettings
{
    /** Merge settings. */
	/** Настройки слияний. */

    /// Determines how unbalanced merges we could do.
    /// Bigger values for more unbalanced merges. It is advisable to be more than 1 / max_parts_to_merge_at_once.
	/// Опеределяет, насколько разбалансированные объединения мы готовы делать.
	/// Чем больше, тем более разбалансированные. Желательно, чтобы было больше, чем 1 / max_parts_to_merge_at_once.
	double size_ratio_coefficient_to_merge_parts = 0.25;

    /// How many parts could be merges at once.
    /// Labour coefficient of parts selection O(N * max_parts_to_merge_at_once).
	/// Сколько за раз сливать кусков.
	/// Трудоемкость выбора кусков O(N * max_parts_to_merge_at_once).
	size_t max_parts_to_merge_at_once = 10;

    /// But while total size of parts is too small(less than this number of bytes), we could merge more parts at once.
    /// This is intentionally to allow quicker merge of too small parts, which could be accumulated too quickly.
	/// Но пока суммарный размер кусков слишком маленький (меньше такого количества байт), можно сливать и больше кусков за раз.
	/// Это сделано, чтобы быстрее сливать очень уж маленькие куски, которых может быстро накопиться много.
	size_t merge_more_parts_if_sum_bytes_is_less_than = 100 * 1024 * 1024;
	size_t max_parts_to_merge_at_once_if_small = 100;

    /// Parts of more than this bytes couldn't be merged at all.
	/// Куски настолько большого размера объединять нельзя вообще.
	size_t max_bytes_to_merge_parts = 10ULL * 1024 * 1024 * 1024;

    /// No more than half of threads could execute merge of parts, if at least one part more than this size in bytes.
	/// Не больше половины потоков одновременно могут выполнять слияния, в которых участвует хоть один кусок хотя бы такого размера.
	size_t max_bytes_to_merge_parts_small = 250 * 1024 * 1024;

    /// Parts more than this size in bytes deny to merge at all.
	/// Куски настолько большого размера в сумме, объединять нельзя вообще.
	size_t max_sum_bytes_to_merge_parts = 25ULL * 1024 * 1024 * 1024;

    /// How much times we increase the coefficient at night.
	/// Во столько раз ночью увеличиваем коэффициент.
	size_t merge_parts_at_night_inc = 10;

    /// How many tasks of merging parts are allowed simultaneously in ReplicatedMergeTree queue.
	/// Сколько заданий на слияние кусков разрешено одновременно иметь в очереди ReplicatedMergeTree.
	size_t max_replicated_merges_in_queue = 6;

	/// How many seconds to keep obsolete parts.
	/// Через сколько секунд удалять ненужные куски.
	time_t old_parts_lifetime = 8 * 60;

    /// How many seconds to keep tmp_-directories.
	/// Через сколько секунд удалять tmp_-директории.
	time_t temporary_directories_lifetime = 86400;

    /** Inserts settings. */
	/** Настройки вставок. */

    /// If table contains at least that many active parts, artificially slow down insert into table.
	/// Если в таблице хотя бы столько активных кусков, искусственно замедлять вставки в таблицу.
	size_t parts_to_delay_insert = 150;

    /// If more than this number active parts, throw 'Too much parts ...' exception
	/// Если в таблице хотя бы столько активных кусков, выдавать ошибку 'Too much parts ...'
	size_t parts_to_throw_insert = 300;

    /// Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts.
	/// Насколько секунд можно максимально задерживать вставку в таблицу типа MergeTree, если в ней много недомердженных кусков.
	size_t max_delay_to_insert = 200;

    /** Replication settings. */
	/** Настройки репликации. */

    /// How many last blocks of hashes should be kept in ZooKeeper.
	/// Для скольки последних блоков хранить хеши в ZooKeeper.
	size_t replicated_deduplication_window = 100;

    /// Keep about this number of last records in ZooKeeper log, even if they are obsolete.
    /// It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.
	/// Хранить примерно столько последних записей в логе в ZooKeeper, даже если они никому уже не нужны.
	/// Не влияет на работу таблиц; используется только чтобы успеть посмотреть на лог в ZooKeeper глазами прежде, чем его очистят.
	size_t replicated_logs_to_keep = 100;

	/// After specified amount of time passed after replication log entry creation
	///  and sum size of parts is greater than threshold,
	///  prefer fetching merged part from replica instead of doing merge locally.
	/// To speed up very long merges.
	time_t prefer_fetch_merged_part_time_threshold = 3600;
	size_t prefer_fetch_merged_part_size_threshold = 10ULL * 1024 * 1024 * 1024;

    /// Max broken parts, if more - deny automatic deletion.
	/// Настройки минимального количества битых данных, при котором отказываться автоматически их удалять.
	size_t max_suspicious_broken_parts = 10;

    /// Not apply ALTER if number of files for modification(deletion, addition) more than this.
	/// Не выполнять ALTER, если количество файлов для модификации (удаления, добавления) больше указанного.
	size_t max_files_to_modify_in_alter_columns = 50;
	/// Not apply ALTER, if number of files for deletion more than this.
	/// Не выполнять ALTER, если количество файлов для удаления больше указанного.
	size_t max_files_to_remove_in_alter_columns = 10;

    /// Maximum number of errors during parts loading, while ReplicatedMergeTree still allowed to start.
	/// Максимальное количество ошибок при загрузке кусков, при котором ReplicatedMergeTree соглашается запускаться.
	size_t replicated_max_unexpected_parts = 3;
	size_t replicated_max_unexpectedly_merged_parts = 2;
	size_t replicated_max_missing_obsolete_parts = 5;
	size_t replicated_max_missing_active_parts = 20;

    /// If ration of wrong parts to total number of parts is less than this - allow to start anyway.
	/// Если отношение количества ошибок к общему количеству кусков меньше указанного значения, то всё-равно можно запускаться.
	double replicated_max_ratio_of_wrong_parts = 0.05;

	/** Check delay of replicas settings. */
	/** Настройки проверки отставания реплик. */

    /// Period to check replication delay and compare with other replicas.
	/// Периодичность для проверки отставания и сравнения его с другими репликами.
	size_t check_delay_period = 60;

    /// Minimal delay from other replicas to yield leadership. Here and further 0 means unlimited.
	/// Минимальное отставание от других реплик, при котором нужно уступить лидерство. Здесь и далее, если 0 - не ограничено.
	size_t min_relative_delay_to_yield_leadership = 120;

    /// Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.
	/// Минимальное отставание от других реплик, при котором нужно закрыться от запросов и не выдавать Ok для проверки статуса.
	size_t min_relative_delay_to_close = 300;

    /// Minimal absolute delay to close, stop serving requests and not return Ok during status check.
	/// Минимальное абсолютное отставание, при котором нужно закрыться от запросов и не выдавать Ok для проверки статуса.
	size_t min_absolute_delay_to_close = 0;


	void loadFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
	{
	#define SET_DOUBLE(NAME) \
		NAME = config.getDouble(config_elem + "." #NAME, NAME);

	#define SET_SIZE_T(NAME) \
		if (config.has(config_elem + "." #NAME)) NAME = parse<size_t>(config.getString(config_elem + "." #NAME));

		SET_DOUBLE(size_ratio_coefficient_to_merge_parts);
		SET_SIZE_T(max_parts_to_merge_at_once);
		SET_SIZE_T(merge_more_parts_if_sum_bytes_is_less_than);
		SET_SIZE_T(max_parts_to_merge_at_once_if_small);
		SET_SIZE_T(max_bytes_to_merge_parts);
		SET_SIZE_T(max_bytes_to_merge_parts_small);
		SET_SIZE_T(max_sum_bytes_to_merge_parts);
		SET_SIZE_T(merge_parts_at_night_inc);
		SET_SIZE_T(max_replicated_merges_in_queue);
		SET_SIZE_T(old_parts_lifetime);
		SET_SIZE_T(temporary_directories_lifetime);
		SET_SIZE_T(parts_to_delay_insert);
		SET_SIZE_T(parts_to_throw_insert);
		SET_SIZE_T(max_delay_to_insert);
		SET_SIZE_T(replicated_deduplication_window);
		SET_SIZE_T(replicated_logs_to_keep);
		SET_SIZE_T(prefer_fetch_merged_part_time_threshold);
		SET_SIZE_T(prefer_fetch_merged_part_size_threshold);
		SET_SIZE_T(max_suspicious_broken_parts);
		SET_SIZE_T(max_files_to_modify_in_alter_columns);
		SET_SIZE_T(max_files_to_remove_in_alter_columns);
		SET_SIZE_T(replicated_max_unexpected_parts);
		SET_SIZE_T(replicated_max_unexpectedly_merged_parts);
		SET_SIZE_T(replicated_max_missing_obsolete_parts);
		SET_SIZE_T(replicated_max_missing_active_parts);
		SET_DOUBLE(replicated_max_ratio_of_wrong_parts);
		SET_SIZE_T(check_delay_period);
		SET_SIZE_T(min_relative_delay_to_yield_leadership);
		SET_SIZE_T(min_relative_delay_to_close);
		SET_SIZE_T(min_absolute_delay_to_close);

	#undef SET_SIZE_T
	#undef SET_DOUBLE
	}
};

}
