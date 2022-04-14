#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <method/hnsw.h>

#include <cstddef>
#include <iterator>
#include <memory>
#include <sstream>
#include <index.h>
#include <knnqueue.h>
#include <methodfactory.h>
#include <params.h>
#include <space.h>
#include <spacefactory.h>
#include "object.h"

namespace similarity{
    EfficientDistFunc getDistFunc(DistFuncType funcType);
}

namespace HnswWrapper{

    

    using namespace similarity;
    using  namespace DB;
    

    template<typename Dist>
    struct IndexWrap {
    explicit IndexWrap(const std::string &space_type_,const AnyParams &space_params = AnyParams());


    void createIndex(const AnyParams& params = AnyParams()); 

    void loadIndex(ReadBuffer & istr, bool load_data = true);


    void saveIndex(WriteBuffer & ostr, bool save_data = true);

    KNNQueue<Dist> *knnQuery(const Object &obj, size_t k);

    void addPoint(const Object &point);

    void addPointUnsafe(const Object* obj);

    void addBatch(const ObjectVector &new_data);

    void addBatchUnsafe(ObjectVector &&new_data);

    void freeAndClearObjectVector();

    ~IndexWrap();

private:
    std::string space_type;
    std::unique_ptr<similarity::Space<Dist>> space;
    ObjectVector data;
    std::unique_ptr<Hnsw<Dist>> index;
};

}

namespace DB
{

struct MergeTreeIndexGranuleSimpleHnsw final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, const Block & index_sample_block_,
    std::unique_ptr<HnswWrapper::IndexWrap<float>> index_impl_);

    ~MergeTreeIndexGranuleSimpleHnsw() override = default;
 
    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return false;}

    String index_name;
    Block index_sample_block;
    std::unique_ptr<HnswWrapper::IndexWrap<float>> index_impl;
};

struct MergeTreeIndexAggregatorSimpleHnsw final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorSimpleHnsw(const String & index_name_, const Block & index_sample_block);
    ~MergeTreeIndexAggregatorSimpleHnsw() override = default;

    bool empty() const override { return data.empty();}
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    similarity::ObjectVector data;
};


class MergeTreeIndexConditionSimpleHnsw final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSimpleHnsw(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionSimpleHnsw() override = default;
private:
    DataTypes index_data_types;
    KeyCondition condition;
};

class MergeTreeIndexSimpleHnsw : public IMergeTreeIndex
{
public:
    MergeTreeIndexSimpleHnsw(const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexSimpleHnsw() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;
};

}
