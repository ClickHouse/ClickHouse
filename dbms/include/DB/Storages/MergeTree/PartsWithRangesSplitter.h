#pragma once

#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

/** Этот класс разбивает объект типа RangesInDataParts (см. MergeTreeDataSelectExecutor)
  * на не больше, чем указанное количество сегментов.
  */
class PartsWithRangesSplitter final
{
public:
	PartsWithRangesSplitter(const MergeTreeDataSelectExecutor::RangesInDataParts & input_,
							size_t granularity_, size_t min_segment_size_, size_t max_segments_count_);

	~PartsWithRangesSplitter() = default;
	PartsWithRangesSplitter(const PartsWithRangesSplitter &) = delete;
	PartsWithRangesSplitter & operator=(const PartsWithRangesSplitter &) = delete;

	std::vector<MergeTreeDataSelectExecutor::RangesInDataParts> perform();

private:
	void init();
	bool emitRange();
	bool switchToNextSegment();
	bool switchToNextRange(bool add_part);
	void initRangeInfo();
	void initSegmentInfo();
	void addPart();
	bool isRangeConsumed() const { return range_begin == range_end; }
	bool isSegmentConsumed() const { return segment_begin == segment_end; }

private:
	// Входные данные.
	const MergeTreeDataSelectExecutor::RangesInDataParts & input;
	MergeTreeDataSelectExecutor::RangesInDataParts::const_iterator input_part;
	std::vector<MarkRange>::const_iterator input_range;

	// Выходные данные.
	std::vector<MergeTreeDataSelectExecutor::RangesInDataParts> output_segments;
	std::vector<MergeTreeDataSelectExecutor::RangesInDataParts>::iterator current_output_segment;
	MergeTreeDataSelectExecutor::RangesInDataPart * current_output_part;

	size_t total_size;

	const size_t granularity;
	const size_t min_segment_size;
	const size_t max_segments_count;

	size_t segment_size;

	size_t range_begin;
	size_t range_end;

	size_t segment_begin;
	size_t segment_end;

	size_t part_index_in_query;
};

}
