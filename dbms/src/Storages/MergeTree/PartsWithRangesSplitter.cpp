#include <DB/Storages/MergeTree/PartsWithRangesSplitter.h>

namespace DB
{

PartsWithRangesSplitter::PartsWithRangesSplitter(const MergeTreeDataSelectExecutor::RangesInDataParts & input_,
						UInt64 parallel_replica_offset,
						size_t granularity_, size_t min_segment_size_, size_t max_segments_count_)
	: input(input_),
	granularity(granularity_),
	min_segment_size(min_segment_size_),
	max_segments_count(max_segments_count_)
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
	part_index_in_query = parallel_replica_offset * total_size;
	total_size *= granularity;

	if ((granularity == 0) || (min_segment_size == 0) || (max_segments_count == 0) || (total_size == 0))
		throw Exception("One or more arguments are invalid.", ErrorCodes::BAD_ARGUMENTS);
}

Segments PartsWithRangesSplitter::perform()
{
	if ((max_segments_count > 1) && (total_size > min_segment_size))
	{
		init();
		while (emitRange()) {}
	}
	return output_segments;
}

void PartsWithRangesSplitter::init()
{
	output_segments.clear();

	// Вычислить размер сегментов так, чтобы он был кратен granularity
	segment_size = total_size / std::min(max_segments_count, (total_size / min_segment_size));
	size_t scale = segment_size / granularity;
	if (segment_size % granularity != 0) {
		++scale;
	}
	segment_size = granularity * scale;

	// Посчитать количество сегментов.
	size_t segments_count = total_size / segment_size;
	if (total_size % segment_size != 0) {
		++segments_count;
	}

	output_segments.resize(segments_count);

	input_part = input.begin();
	part_index_in_query += input_part->part_index_in_query;

	/// Инициализируем информацию про первый диапазон.
	input_range = input_part->ranges.begin();
	initRangeInfo();

	/// Инициализируем информацию про первый выходной сегмент.
	current_output_segment = output_segments.begin();
	initSegmentInfo();
}

bool PartsWithRangesSplitter::emitRange()
{
	size_t new_size = std::min((range_end - range_begin) * granularity, segment_end - segment_begin);
	size_t end = range_begin + new_size / granularity;

	current_output_part->ranges.push_back(MarkRange(range_begin, end));

	range_begin = end;
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
}

void PartsWithRangesSplitter::addPart()
{
	MergeTreeDataSelectExecutor::RangesInDataPart part(input_part->data_part, part_index_in_query);
	++part_index_in_query;
	current_output_segment->push_back(part);
	current_output_part = &(current_output_segment->back());
}

}
