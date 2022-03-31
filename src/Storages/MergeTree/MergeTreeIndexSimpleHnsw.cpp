#include <cstddef>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <spacefactory.h>
#include <Storages/MergeTree/MergeTreeIndexSimpleHnsw.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, 
    const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}


MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_,
    const Block & index_sample_block_, 
    std::unique_ptr<similarity::Hnsw<float>> index_impl_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_), index_impl(std::move(index_impl_))
{}


void MergeTreeIndexGranuleSimpleHnsw::serializeBinary(WriteBuffer & ostr) const {
     std::ostringstream ost;
     index_impl->SaveIndex(ost);
     ostr.write(ost.str().data(), ost.str().size());
}

void MergeTreeIndexGranuleSimpleHnsw::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version){
    if (version != 1){
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
    }
    size_t index_size = istr.available();
    std::string str(index_size, '\0');
    istr.read(str.data(), index_size);
    std::istringstream ist(str);
    index_impl->LoadIndex(ist);
}

MergeTreeIndexAggregatorSimpleHnsw::MergeTreeIndexAggregatorSimpleHnsw(const String & index_name_, 
const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSimpleHnsw::getGranuleAndReset()
{
    std::string space_type = "l2";
    auto space_params = std::shared_ptr<similarity::AnyParams>(new similarity::AnyParams(std::vector<std::string>()));
    auto* space(similarity::SpaceFactoryRegistry<float>::Instance().CreateSpace(space_type, *space_params));
    auto index_impl = std::make_unique<similarity::Hnsw<float>>(false, *space, data);
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index_name, index_sample_block, std::move(index_impl));
}

void MergeTreeIndexAggregatorSimpleHnsw::update(const Block & block, size_t * pos, size_t limit){
     if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

   auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

    for (size_t i = 0; i < rows_read; ++i) {
        Field field;
        column->get(i, field);

        auto field_array = field.safeGet<Tuple>();
        float *raw_vector = new float[field_array.size()];

        // Store vectors in the flatten arrays
        for (size_t j = 0; j < field_array.size();++j) {
            auto num = field_array.at(j).safeGet<Float32>();
            raw_vector[j] = num;
        }
         // true here means that the element in vector is responsible for deleting
        data.push_back(new similarity::Object(reinterpret_cast<char*>(raw_vector), true)); 
    }

    *pos += rows_read;
}

MergeTreeIndexConditionSimpleHnsw::MergeTreeIndexConditionSimpleHnsw(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
    , condition(query, context, index.column_names, index.expression)
{
}

bool MergeTreeIndexConditionSimpleHnsw::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexConditionSimpleHnsw::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    // std::shared_ptr<MergeTreeIndexGranuleSimpleHnsw> granule
    //     = std::dynamic_pointer_cast<MergeTreeIndexGranuleSimpleHnsw>(idx_granule);
    // if (!granule)
    //     throw Exception(
    //         "Minmax index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    // return condition.checkInHyperrectangle(granule->hyperrectangle, index_data_types).can_be_true;
    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexSimpleHnsw::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexSimpleHnsw::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSimpleHnsw>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexSimpleHnsw::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionSimpleHnsw>(index, query, context);
};

bool MergeTreeIndexSimpleHnsw::mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const
{
    // const String column_name = node->getColumnName();

    // for (const auto & cname : index.column_names)
    //     if (column_name == cname)
    //         return true;

    // if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    //     if (func->arguments->children.size() == 1)
    //         return mayBenefitFromIndexForIn(func->arguments->children.front());

    return false;
}

MergeTreeIndexFormat MergeTreeIndexSimpleHnsw::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}


MergeTreeIndexPtr simpleHnswIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexSimpleHnsw>(index);
}

void simpleHnswIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{
}

}
