#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

#include <Interpreters/ExpressionAnalyzer.h>

#include <Common/FieldAccurateComparison.h>
#include <Common/quoteString.h>

#include <Columns/ColumnNullable.h>

#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());
    }
    datatypes = index_sample_block.getDataTypes();
}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(
    const String & index_name_,
    const Block & index_sample_block_,
    std::vector<Range> && hyperrectangle_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , hyperrectangle(std::move(hyperrectangle_))
{
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());
    }
    datatypes = index_sample_block.getDataTypes();
}

void MergeTreeIndexGranuleMinMax::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty minmax index {}", backQuote(index_name));

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        serializations[i]->serializeBinary(hyperrectangle[i].left, ostr, {});
        serializations[i]->serializeBinary(hyperrectangle[i].right, ostr, {});
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    hyperrectangle.clear();
    Field min_val;
    Field max_val;

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        switch (version)
        {
            case 1:
                if (!datatypes[i]->isNullable())
                {
                    serializations[i]->deserializeBinary(min_val, istr, format_settings);
                    serializations[i]->deserializeBinary(max_val, istr, format_settings);
                }
                else
                {
                    /// NOTE: that this serialization differs from
                    /// IMergeTreeDataPart::MinMaxIndex::load() to preserve
                    /// backward compatibility.
                    ///
                    /// But this is deprecated format, so this is OK.

                    bool is_null;
                    readBinary(is_null, istr);
                    if (!is_null)
                    {
                        serializations[i]->deserializeBinary(min_val, istr, format_settings);
                        serializations[i]->deserializeBinary(max_val, istr, format_settings);
                    }
                    else
                    {
                        min_val = Null();
                        max_val = Null();
                    }
                }
                break;

            /// New format with proper Nullable support for values that includes Null values
            case 2:
                serializations[i]->deserializeBinary(min_val, istr, format_settings);
                serializations[i]->deserializeBinary(max_val, istr, format_settings);

                // NULL_LAST
                if (min_val.isNull())
                    min_val = POSITIVE_INFINITY;
                if (max_val.isNull())
                    max_val = POSITIVE_INFINITY;

                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
        }

        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
}

MergeTreeIndexAggregatorMinMax::MergeTreeIndexAggregatorMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorMinMax::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index_name, index_sample_block, std::move(hyperrectangle));
}

void MergeTreeIndexAggregatorMinMax::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    FieldRef field_min;
    FieldRef field_max;
    size_t range_start = *pos;
    size_t range_end = *pos + rows_read;
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        auto index_column_name = index_sample_block.getByPosition(i).name;
        const auto & column = block.getByName(index_column_name).column;
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
            column_nullable->getExtremesNullLast(field_min, field_max, range_start, range_end);
        else
            column->getExtremes(field_min, field_max, range_start, range_end);

        if (hyperrectangle.size() <= i)
        {
            hyperrectangle.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            hyperrectangle[i].left
                = accurateLess(hyperrectangle[i].left, field_min) ? hyperrectangle[i].left : field_min;
            hyperrectangle[i].right
                = accurateLess(hyperrectangle[i].right, field_max) ? field_max : hyperrectangle[i].right;
        }
    }

    *pos += rows_read;
}

namespace
{

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

}

MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
    : index_data_types(index.data_types)
    , condition(buildCondition(index, filter_dag, context))
{
}

bool MergeTreeIndexConditionMinMax::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        condition.getRPN(),
        {KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE,
         KeyCondition::RPNElement::FUNCTION_IN_RANGE,
         KeyCondition::RPNElement::FUNCTION_IN_SET,
         KeyCondition::RPNElement::FUNCTION_NOT_IN_SET,
         KeyCondition::RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE,
         KeyCondition::RPNElement::FUNCTION_POINT_IN_POLYGON,
         KeyCondition::RPNElement::FUNCTION_IS_NULL,
         KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL,
         KeyCondition::RPNElement::ALWAYS_FALSE});
}

bool MergeTreeIndexConditionMinMax::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const
{
    const MergeTreeIndexGranuleMinMax & granule = typeid_cast<const MergeTreeIndexGranuleMinMax &>(*idx_granule);
    return condition.checkInHyperrectangle(granule.hyperrectangle, index_data_types, {}, update_partial_disjunction_result_fn).can_be_true;
}

std::string MergeTreeIndexConditionMinMax::getDescription() const
{
    return condition.getDescription().condition;
}

MergeTreeIndexGranulePtr MergeTreeIndexMinMax::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexMinMax::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorMinMax>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexMinMax::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context);
    return std::make_shared<MergeTreeIndexConditionMinMax>(index, filter_dag, context);
}

MergeTreeIndexFormat MergeTreeIndexMinMax::getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2"))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx"))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};
    return {0 /* unknown */, {}};
}

MergeTreeIndexBulkGranulesMinMax::MergeTreeIndexBulkGranulesMinMax(const String & index_name_, const Block & index_sample_block_, int direction_, size_t size_hint_, bool store_map_) :
    index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , direction(direction_)
    , store_map(store_map_)
{
    const DataTypePtr & type = index_sample_block.getByPosition(0).type;
    serialization = type->getDefaultSerialization();
    granules.reserve(size_hint_);
}

void MergeTreeIndexBulkGranulesMinMax::deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    Field value;
    Field scratch;

    /// The order in which values are read depends on 'direction':
    /// If direction == ASC, we need only min value, discard max value
    /// If direction == DESC, we need only max value, discard min value
    if (direction == 1)
    {
        serialization->deserializeBinary(value, istr, format_settings);
        serialization->deserializeBinary(scratch, istr, format_settings);
    }
    else
    {
        serialization->deserializeBinary(scratch, istr, format_settings);
        serialization->deserializeBinary(value, istr, format_settings);
    }
    granules.emplace_back(MinMaxGranule{granule_num, value});
    if (store_map)
        granules_map.emplace(granule_num, granules.size() - 1);
    empty = false;
}

void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(size_t n, std::vector<MinMaxGranule> & result)
{
    if (n == 0)
        return;

    if (n >= granules.size())
    {
        result.insert(result.end(), granules.begin(), granules.end());
        return;
    }

    std::priority_queue<MinMaxGranuleItem> queue;

    for (const auto & granule : granules)
    {
        MinMaxGranuleItem item{direction, 0, granule.granule_num, granule.min_or_max_value};
        if (queue.size() < n)
            queue.push({direction, 0, granule.granule_num, granule.min_or_max_value});
        else if ((direction == 1 && granule.min_or_max_value < queue.top().min_or_max_value) ||
                    (direction == -1 && granule.min_or_max_value > queue.top().min_or_max_value))
        {
            queue.pop();
            queue.push({direction, 0, granule.granule_num, granule.min_or_max_value});
        }
    }

    while (!queue.empty())
    {
        result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
        queue.pop();
    }
}

/// This routine is for top-N of top-N granules from all parts
void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(int direction,
                                                    size_t n,
                                                    const std::vector<std::vector<MinMaxGranule>> & parts,
                                                    std::vector<MarkRanges> & result)
{
    if (n == 0)
        return;

    std::priority_queue<MinMaxGranuleItem> queue;

    for (size_t part_index = 0; part_index < parts.size(); ++part_index)
    {
        for (const auto & granule : parts[part_index])
        {
            if (queue.size() < n)
                queue.push({direction, part_index, granule.granule_num, granule.min_or_max_value});
            else if ((direction == 1 && granule.min_or_max_value < queue.top().min_or_max_value) ||
                        (direction == -1 && granule.min_or_max_value > queue.top().min_or_max_value))
            {
                queue.pop();
                queue.push({direction, part_index, granule.granule_num, granule.min_or_max_value});
            }
        }
    }

    result.resize(parts.size(), {});
    while (!queue.empty())
    {
        const auto & item = queue.top();
        result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
        queue.pop();
    }

    for (auto & part_ranges : result)
        std::sort(part_ranges.begin(), part_ranges.end());
}

MergeTreeIndexPtr minmaxIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexMinMax>(index);
}

void minmaxIndexValidator(const IndexDescription & index, bool attach)
{
    if (attach)
        return;

    for (const auto & column : index.sample_block)
    {
        if (!column.type->isComparable())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Data type of argument for minmax index must be comparable, got {} type for column {} instead",
                column.type->getName(), column.name);
        }

        if (isDynamic(column.type) || isVariant(column.type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{} data type of column {} is not allowed in minmax index because the column of that type can contain values with different data "
                "types. Consider using typed subcolumns or cast column to a specific data type",
                column.type->getName(), column.name);
        }
    }
}

}
