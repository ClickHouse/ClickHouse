#pragma once

#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

/** Этот класс разбивает объект типа RangesInDataParts (см. MergeTreeDataSelectExecutor)
  * на указанное количество частей. 
  */
class PartsWithRangesSplitter
{
public:
	PartsWithRangesSplitter(const MergeTreeDataSelectExecutor::RangesInDataParts & input_, 
							size_t total_size_, size_t min_segment_size_, size_t max_segments_count_);

	~PartsWithRangesSplitter() = default;
	PartsWithRangesSplitter(const PartsWithRangesSplitter &) = delete;
	PartsWithRangesSplitter & operator=(const PartsWithRangesSplitter &) = delete;

	std::vector<MergeTreeDataSelectExecutor::RangesInDataParts> perform();

private:
	void init();
	bool emit();
	bool updateSegment();
	bool updateRange(bool add_part);
	void addPart();
	void initRangeInfo();
	void initSegmentInfo();
	bool isRangeConsumed() const { return range_begin == range_end; }
	bool isSegmentConsumed() const { return segment_begin == segment_end; }

private:
	// Input data.
	const MergeTreeDataSelectExecutor::RangesInDataParts & input;
	MergeTreeDataSelectExecutor::RangesInDataParts::const_iterator input_part;
	std::vector<MarkRange>::const_iterator input_range;

	// Output data.
	std::vector<MergeTreeDataSelectExecutor::RangesInDataParts> output_segments;
	std::vector<MergeTreeDataSelectExecutor::RangesInDataParts>::iterator current_output_segment;
	MergeTreeDataSelectExecutor::RangesInDataPart * current_output_part;

	size_t total_size;
	size_t remaining_size;
	size_t min_segment_size;
	size_t max_segments_count;
	size_t segment_size;

	size_t range_begin;
	size_t range_end;

	size_t segment_begin;
	size_t segment_end;
};

}
