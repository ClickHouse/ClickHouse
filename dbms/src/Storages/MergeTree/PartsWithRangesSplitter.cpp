#include <DB/Storages/MergeTree/PartsWithRangesSplitter.h>

namespace DB
{

PartsWithRangesSplitter::PartsWithRangesSplitter(const MergeTreeDataSelectExecutor::RangesInDataParts & input_, 
						size_t total_size_, size_t min_segment_size_, size_t max_segments_count_)
	: input(input_),
	total_size(total_size_),
	remaining_size(total_size_),
	min_segment_size(min_segment_size_),
	max_segments_count(max_segments_count_)
{
	if ((total_size == 0) || (min_segment_size == 0) || (max_segments_count < 2)
		|| (total_size < min_segment_size))
		throw Exception("One or more parameters are out of bound.", ErrorCodes::PARAMETER_OUT_OF_BOUND);
}

std::vector<MergeTreeDataSelectExecutor::RangesInDataParts> PartsWithRangesSplitter::perform()
{
	init();
	while (emit()) {}
	return output_segments;
}

void PartsWithRangesSplitter::init()
{
	size_t segments_count = max_segments_count;
	while ((segments_count > 0) && (total_size < (min_segment_size * segments_count)))
		--segments_count;

	segment_size = total_size / segments_count;
	output_segments.resize(segments_count);

	/// Инициализируем информацию про первый диапазон.
	input_part = input.begin();
	input_range = input_part->ranges.begin();
	initRangeInfo();

	/// Инициализируем информацию про первый выходной сегмент.
	current_output_segment = output_segments.begin();
	addPart();
	initSegmentInfo();
}

bool PartsWithRangesSplitter::emit()
{
	size_t new_size = std::min(range_end - range_begin, segment_end - segment_begin);
	current_output_part->ranges.push_back(MarkRange(range_begin, range_begin + new_size));

	range_begin += new_size;
	segment_begin += new_size;

	if (isSegmentConsumed())
		return updateSegment();
	else if (isRangeConsumed())
		return updateRange(true);
	else
		return false;
}

bool PartsWithRangesSplitter::updateSegment()
{
	++current_output_segment;
	if (current_output_segment == output_segments.end())
		return false;

	if (isRangeConsumed())
		if (!updateRange(false))
			return false;

	addPart();
	initSegmentInfo();
	return true;
}

bool PartsWithRangesSplitter::updateRange(bool add_part)
{
	++input_range;
	if (input_range == input_part->ranges.end())
	{
		++input_part;
		if (input_part == input.end())
			return false;

		input_range = input_part->ranges.begin();

		if (add_part)
			addPart();
	}

	initRangeInfo();
	return true;
}

void PartsWithRangesSplitter::addPart()
{
	MergeTreeDataSelectExecutor::RangesInDataPart new_part;
	new_part.data_part = input_part->data_part;
	current_output_segment->push_back(new_part);
	current_output_part = &(current_output_segment->back());
}

void PartsWithRangesSplitter::initRangeInfo()
{
	range_begin = 0;
	range_end = input_range->end - input_range->begin;
}

void PartsWithRangesSplitter::initSegmentInfo()
{
	segment_begin = 0;
	segment_end = segment_size;

	remaining_size -= segment_size;
	if (remaining_size < segment_size)
		segment_end += remaining_size;
}

}
