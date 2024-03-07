#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>

#include <Poco/Logger.h>
#include <Common/FieldVisitorsAccurateComparison.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(
    const String & index_name_,
    const Block & index_sample_block_,
    std::vector<Range> && hyperrectangle_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , hyperrectangle(std::move(hyperrectangle_)) {}

void MergeTreeIndexGranuleMinMax::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty minmax index {}", backQuote(index_name));

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        auto serialization = type->getDefaultSerialization();

        serialization->serializeBinary(hyperrectangle[i].left, ostr, {});
        serialization->serializeBinary(hyperrectangle[i].right, ostr, {});
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    hyperrectangle.clear();
    Field min_val;
    Field max_val;

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        auto serialization = type->getDefaultSerialization();

        switch (version)
        {
            case 1:
                if (!type->isNullable())
                {
                    serialization->deserializeBinary(min_val, istr, {});
                    serialization->deserializeBinary(max_val, istr, {});
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
                        serialization->deserializeBinary(min_val, istr, {});
                        serialization->deserializeBinary(max_val, istr, {});
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
                serialization->deserializeBinary(min_val, istr, {});
                serialization->deserializeBinary(max_val, istr, {});

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
{}

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
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        auto index_column_name = index_sample_block.getByPosition(i).name;
        const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
            column_nullable->getExtremesNullLast(field_min, field_max);
        else
            column->getExtremes(field_min, field_max);

        if (hyperrectangle.size() <= i)
        {
            hyperrectangle.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            hyperrectangle[i].left
                = applyVisitor(FieldVisitorAccurateLess(), hyperrectangle[i].left, field_min) ? hyperrectangle[i].left : field_min;
            hyperrectangle[i].right
                = applyVisitor(FieldVisitorAccurateLess(), hyperrectangle[i].right, field_max) ? field_max : hyperrectangle[i].right;
        }
    }

    *pos += rows_read;
}

namespace
{

KeyCondition buildCondition(const IndexDescription & index, const SelectQueryInfo & query_info, ContextPtr context)
{
    if (context->getSettingsRef().allow_experimental_analyzer)
    {
        NameSet array_join_name_set;
        if (query_info.syntax_analyzer_result)
            array_join_name_set = query_info.syntax_analyzer_result->getArrayJoinSourceNameSet();

        return KeyCondition{query_info.filter_actions_dag, context, index.column_names, index.expression, array_join_name_set};
    }

    return KeyCondition{query_info, context, index.column_names, index.expression};
}

}

MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const IndexDescription & index, const SelectQueryInfo & query_info, ContextPtr context)
    : index_data_types(index.data_types)
    , condition(buildCondition(index, query_info, context))
{
}

bool MergeTreeIndexConditionMinMax::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexConditionMinMax::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleMinMax> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleMinMax>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Minmax index condition got a granule with the wrong type.");
    return condition.checkInHyperrectangle(granule->hyperrectangle, index_data_types).can_be_true;
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
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionMinMax>(index, query, context);
}

bool MergeTreeIndexMinMax::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    const String column_name = node->getColumnName();

    for (const auto & cname : index.column_names)
        if (column_name == cname)
            return true;

    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
        if (func->arguments->children.size() == 1)
            return mayBenefitFromIndexForIn(func->arguments->children.front());

    return false;
}

MergeTreeIndexFormat MergeTreeIndexMinMax::getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & relative_path_prefix) const
{
    if (data_part_storage.exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (data_part_storage.exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr minmaxIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexMinMax>(index);
}

void minmaxIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{
}
}
