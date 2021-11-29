#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Poco/Logger.h>

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
        throw Exception(
            "Attempt to write empty minmax index " + backQuote(index_name), ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        auto serialization = type->getDefaultSerialization();

        if (!type->isNullable())
        {
            serialization->serializeBinary(hyperrectangle[i].left, ostr);
            serialization->serializeBinary(hyperrectangle[i].right, ostr);
        }
        else
        {
            bool is_null = hyperrectangle[i].left.isNull() || hyperrectangle[i].right.isNull(); // one is enough
            writeBinary(is_null, ostr);
            if (!is_null)
            {
                serialization->serializeBinary(hyperrectangle[i].left, ostr);
                serialization->serializeBinary(hyperrectangle[i].right, ostr);
            }
        }
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr)
{
    hyperrectangle.clear();
    Field min_val;
    Field max_val;


    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        auto serialization = type->getDefaultSerialization();

        if (!type->isNullable())
        {
            serialization->deserializeBinary(min_val, istr);
            serialization->deserializeBinary(max_val, istr);
        }
        else
        {
            bool is_null;
            readBinary(is_null, istr);
            if (!is_null)
            {
                serialization->deserializeBinary(min_val, istr);
                serialization->deserializeBinary(max_val, istr);
            }
            else
            {
                min_val = Null();
                max_val = Null();
            }
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
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    FieldRef field_min;
    FieldRef field_max;
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        auto index_column_name = index_sample_block.getByPosition(i).name;
        const auto & column = block.getByName(index_column_name).column;
        column->cut(*pos, rows_read)->getExtremes(field_min, field_max);

        if (hyperrectangle.size() <= i)
        {
            hyperrectangle.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, field_min);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, field_max);
        }
    }

    *pos += rows_read;
}


MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
    , condition(query, context, index.column_names, index.expression)
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
        throw Exception(
            "Minmax index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    for (const auto & range : granule->hyperrectangle)
        if (range.left.isNull() || range.right.isNull())
            return true;
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
};

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

MergeTreeIndexPtr minmaxIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexMinMax>(index);
}

void minmaxIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{
}
}
