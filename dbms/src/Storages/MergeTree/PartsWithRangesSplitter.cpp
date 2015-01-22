#include <DB/Storages/MergeTree/PartsWithRangesSplitter.h>

namespace DB
{

PartsWithRangesSplitter::PartsWithRangesSplitter(const MergeTreeDataSelectExecutor::RangesInDataParts & input_, 
						size_t min_segment_size_, size_t max_segments_count_)
	: input(input_),
	current_output_part(nullptr),
	total_size(0),
	remaining_size(0),
	min_segment_size(min_segment_size_),
	max_segments_count(max_segments_count_),
	segment_size(0),
	range_begin(0),
	range_end(0),
	segment_begin(0),
	segment_end(0),
	part_index_in_query(0)
{
	total_size = 0;
	for (const auto & part_with_ranges : input)
	{
		const auto & ranges = part_with_ranges.ranges;
		if (ranges.empty())
			throw Exception("Missing range in chunk.", ErrorCodes::MISSING_RANGE_IN_CHUNK);
		for (const auto & range : ranges)
			total_size += range.end - range.begin;
	}

	if ((total_size == 0) || (min_segment_size == 0) || (max_segments_count < 2)
		|| (total_size < min_segment_size))
		throw Exception("One or more arguments are invalid.", ErrorCodes::BAD_ARGUMENTS);
}

std::vector<MergeTreeDataSelectExecutor::RangesInDataParts> PartsWithRangesSplitter::perform()
{
	init();
	while (emitRange()) {}
	return output_segments;
}

void PartsWithRangesSplitter::init()
{
	remaining_size = total_size;

	size_t segments_count = max_segments_count;
	while ((segments_count > 0) && (total_size < (min_segment_size * segments_count)))
		--segments_count;

	segment_size = total_size / segments_count;
	output_segments.resize(segments_count);

	input_part = input.begin();
	part_index_in_query = input_part->part_index_in_query;

	/// Инициализируем информацию про первый диапазон.
	input_range = input_part->ranges.begin();
	initRangeInfo();

	/// Инициализируем информацию про первый выходной сегмент.
	current_output_segment = output_segments.begin();
	initSegmentInfo();
}

bool PartsWithRangesSplitter::emitRange()
{
	size_t new_size = std::min(range_end - range_begin, segment_end - segment_begin);
	current_output_part->ranges.push_back(MarkRange(range_begin, range_begin + new_size));

	range_begin += new_size;
	segment_begin += new_size;

	if (isSegmentConsumed())
		return switchToNextSegment();
	else if (isRangeConsumed())
		return switchToNextRange(true);
	else
		return false;
}

bool PartsWithRangesSplitter::switchToNextSegment()
{
	++current_output_segment;
	if (current_output_segment == output_segments.end())
		return false;

	if (isRangeConsumed())
		if (!switchToNextRange(false))
			return false;

	initSegmentInfo();
	return true;
}

bool PartsWithRangesSplitter::switchToNextRange(bool add_part)
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

void PartsWithRangesSplitter::initRangeInfo()
{
	range_begin = input_range->begin;
	range_end = input_range->end;
}

void PartsWithRangesSplitter::initSegmentInfo()
{
	addPart();

	segment_begin = 0;
	segment_end = segment_size;

	remaining_size -= segment_size;
	if (remaining_size < segment_size)
		segment_end += remaining_size;
}

void PartsWithRangesSplitter::addPart()
{
	MergeTreeDataSelectExecutor::RangesInDataPart part(input_part->data_part, part_index_in_query);
	++part_index_in_query;
	current_output_segment->push_back(part);
	current_output_part = &(current_output_segment->back());
}

}
