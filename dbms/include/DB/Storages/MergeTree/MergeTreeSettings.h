#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{


/** Тонкие настройки работы MergeTree.
  * Могут быть загружены из конфига.
  */
struct MergeTreeSettings
{
	/** Настройки слияний. */

	/// Опеределяет, насколько разбалансированные объединения мы готовы делать.
	/// Чем больше, тем более разбалансированные. Желательно, чтобы было больше, чем 1 / max_parts_to_merge_at_once.
	double size_ratio_coefficient_to_merge_parts = 0.25;

	/// Сколько за раз сливать кусков.
	/// Трудоемкость выбора кусков O(N * max_parts_to_merge_at_once).
	size_t max_parts_to_merge_at_once = 10;

	/// Но пока суммарный размер кусков слишком маленький (меньше такого количества байт), можно сливать и больше кусков за раз.
	/// Это сделано, чтобы быстрее сливать очень уж маленькие куски, которых может быстро накопиться много.
	size_t merge_more_parts_if_sum_bytes_is_less_than = 100 * 1024 * 1024;
	size_t max_parts_to_merge_at_once_if_small = 100;

	/// Куски настолько большого размера объединять нельзя вообще.
	size_t max_bytes_to_merge_parts = 10ul * 1024 * 1024 * 1024;

	/// Не больше половины потоков одновременно могут выполнять слияния, в которых участвует хоть один кусок хотя бы такого размера.
	size_t max_bytes_to_merge_parts_small = 250 * 1024 * 1024;

	/// Куски настолько большого размера в сумме, объединять нельзя вообще.
	size_t max_sum_bytes_to_merge_parts = 25ul * 1024 * 1024 * 1024;

	/// Во столько раз ночью увеличиваем коэффициент.
	size_t merge_parts_at_night_inc = 10;

	/// Сколько заданий на слияние кусков разрешено одновременно иметь в очереди ReplicatedMergeTree.
	size_t max_replicated_merges_in_queue = 6;

	/// Через сколько секунд удалять ненужные куски.
	time_t old_parts_lifetime = 8 * 60;

	/** Настройки вставок. */

	/// Если в таблице хотя бы столько активных кусков, искусственно замедлять вставки в таблицу.
	size_t parts_to_delay_insert = 150;

	/// Если в таблице parts_to_delay_insert + k кусков, спать insert_delay_step^k миллисекунд перед вставкой каждого блока.
	/// Таким образом, скорость вставок автоматически замедлится примерно до скорости слияний.
	double insert_delay_step = 1.1;

	/** Настройки репликации. */

	/// Для скольки последних блоков хранить хеши в ZooKeeper.
	size_t replicated_deduplication_window = 100;

	/// Хранить примерно столько последних записей в логе в ZooKeeper, даже если они никому уже не нужны.
	/// Не влияет на работу таблиц; используется только чтобы успеть посмотреть на лог в ZooKeeper глазами прежде, чем его очистят.
	size_t replicated_logs_to_keep = 100;

	/// Настройки минимального количества битых данных, при котором отказываться автоматически их удалять.
	size_t max_suspicious_broken_parts = 10;

	/// Не выполнять ALTER, если количество файлов для модификации (удаления, добавления) больше указанного.
	size_t max_files_to_modify_in_alter_columns = 5;

	/// Максимальное количество ошибок при загрузке кусков, при котором ReplicatedMergeTree соглашается запускаться.
	size_t replicated_max_unexpected_parts = 3;
	size_t replicated_max_unexpectedly_merged_parts = 2;
	size_t replicated_max_missing_obsolete_parts = 5;
	size_t replicated_max_missing_active_parts = 20;

	/// Если отношение количества ошибок к общему количеству кусков меньше указанного значения, то всё-равно можно запускаться.
	double replicated_max_ratio_of_wrong_parts = 0.05;

	/** Настройки проверки отставания реплик. */

	/// Периодичность для проверки отставания и сравнения его с другими репликами.
	size_t check_delay_period = 60;

	/// Минимальное отставание от других реплик, при котором нужно уступить лидерство. Здесь и далее, если 0 - не ограничено.
	size_t min_relative_delay_to_yield_leadership = 120;

	/// Минимальное отставание от других реплик, при котором нужно закрыться от запросов и не выдавать Ok для проверки статуса.
	size_t min_relative_delay_to_close = 300;

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
		SET_SIZE_T(parts_to_delay_insert);
		SET_DOUBLE(insert_delay_step);
		SET_SIZE_T(replicated_deduplication_window);
		SET_SIZE_T(replicated_logs_to_keep);
		SET_SIZE_T(max_suspicious_broken_parts);
		SET_SIZE_T(max_files_to_modify_in_alter_columns);
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
