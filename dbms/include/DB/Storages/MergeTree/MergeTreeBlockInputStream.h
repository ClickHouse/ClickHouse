#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/Storages/MergeTree/MarkRange.h>


namespace DB
{


class MergeTreeReader;
class UncompressedCache;
class MarkCache;

/// Для чтения из одного куска. Для чтения сразу из многих, Storage использует сразу много таких объектов.
class MergeTreeBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergeTreeBlockInputStream(const String & path_,	/// Путь к куску
		size_t block_size_, Names column_names,
		MergeTreeData & storage_, const MergeTreeData::DataPartPtr & owned_data_part_,
		const MarkRanges & mark_ranges_, bool use_uncompressed_cache_,
		ExpressionActionsPtr prewhere_actions_, String prewhere_column_, bool check_columns,
		size_t min_bytes_to_use_direct_io_, size_t max_read_buffer_size_,
		bool save_marks_in_cache_, bool quiet = false);

    ~MergeTreeBlockInputStream() override;

	String getName() const override { return "MergeTree"; }

	String getID() const override;

protected:
	/// Будем вызывать progressImpl самостоятельно.
	void progress(const Progress & value) override {}


	/** Если некоторых запрошенных столбцов нет в куске,
	  *  то выясняем, какие столбцы может быть необходимо дополнительно прочитать,
	  *  чтобы можно было вычислить DEFAULT выражение для этих столбцов.
	  * Добавляет их в columns.
	  */
	NameSet injectRequiredColumns(Names & columns) const;

	Block readImpl() override;

private:
	const String path;
	size_t block_size;
	NamesAndTypesList columns;
	NameSet column_name_set;
	NamesAndTypesList pre_columns;
	MergeTreeData & storage;
	MergeTreeData::DataPartPtr owned_data_part;	/// Кусок не будет удалён, пока им владеет этот объект.
	std::unique_ptr<Poco::ScopedReadRWLock> part_columns_lock; /// Не дадим изменить список столбцов куска, пока мы из него читаем.
	MarkRanges all_mark_ranges; /// В каких диапазонах засечек читать. В порядке возрастания номеров.
	MarkRanges remaining_mark_ranges; /// В каких диапазонах засечек еще не прочли.
									  /// В порядке убывания номеров, чтобы можно было выбрасывать из конца.
	bool use_uncompressed_cache;
	std::unique_ptr<MergeTreeReader> reader;
	std::unique_ptr<MergeTreeReader> pre_reader;
	ExpressionActionsPtr prewhere_actions;
	String prewhere_column;
	bool remove_prewhere_column;

	Logger * log;

	/// column names in specific order as expected by other stages
	Names ordered_names;
	bool should_reorder{false};

	size_t min_bytes_to_use_direct_io;
	size_t max_read_buffer_size;

	std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
	std::shared_ptr<MarkCache> owned_mark_cache;
	/// Если выставлено в false - при отсутствии засечек в кэше, считавать засечки, но не сохранять их в кэш, чтобы не вымывать оттуда другие данные.
	bool save_marks_in_cache;
};

}
