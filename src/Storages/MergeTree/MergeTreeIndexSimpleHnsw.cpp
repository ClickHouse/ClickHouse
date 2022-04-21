#include <cstddef>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <knnquery.h>
#include <methodfactory.h>
#include <object.h>
#include <params.h>
#include <space.h>
#include <spacefactory.h>
#include <Storages/MergeTree/MergeTreeIndexSimpleHnsw.h>
#include <space/space_lp.h>
#include <space/space_scalar.h>
#include <Poco/Logger.h>
#include "Storages/MergeTree/KeyCondition.h"

namespace HnswWrapper{

    using namespace similarity;
    using  namespace DB;

    template<typename Dist>
    IndexWrap<Dist>::IndexWrap(const std::string &space_type_,const AnyParams &space_params) :
            space_type(space_type_),
            space(SpaceFactoryRegistry<Dist>::Instance().CreateSpace(space_type_, space_params)) {
    }

    template<typename Dist>
    void IndexWrap<Dist>::createIndex(const AnyParams& params) {
        index = std::make_unique<Hnsw<Dist>>(false, *space, data);
        index->CreateIndex(params);
    }


    template<typename Dist>
    void IndexWrap<Dist>::loadIndex(ReadBuffer & istr, bool load_data) {
       if (load_data) {
            std::vector<std::string> dummy;
            freeAndClearObjectVector();
            size_t qty;
            size_t obj_size;
            istr.read(reinterpret_cast<char*>(&qty), sizeof(qty));
            for (size_t i = 0; i < qty; ++i) {
                istr.read(reinterpret_cast<char*>(&obj_size), sizeof(size_t));
                unique_ptr<char[]> buf(new char[obj_size]);
                istr.read(&buf[0], obj_size);
                data.push_back(new Object(buf.release(), true));
            }
        }
        index = std::make_unique<Hnsw<Dist>>(false, *space, data);

        LOG(LIB_INFO) << "Loading optimized index.";
        istr.read(reinterpret_cast<char*>(&index->totalElementsStored_), sizeof(index->totalElementsStored_));
        istr.read(reinterpret_cast<char*>(&index->memoryPerObject_), sizeof(index->memoryPerObject_));
        istr.read(reinterpret_cast<char*>(&index->offsetLevel0_), sizeof(index->offsetLevel0_));
        istr.read(reinterpret_cast<char*>(&index->offsetData_), sizeof(index->offsetData_));
        istr.read(reinterpret_cast<char*>(&index->maxlevel_), sizeof(index->maxlevel_));
        istr.read(reinterpret_cast<char*>(&index->enterpointId_), sizeof(index->enterpointId_));
        istr.read(reinterpret_cast<char*>(&index->maxM_), sizeof(index->maxM_));
        istr.read(reinterpret_cast<char*>(&index->maxM0_), sizeof(index->maxM0_));
        istr.read(reinterpret_cast<char*>(&index->dist_func_type_), sizeof(index->dist_func_type_));
        istr.read(reinterpret_cast<char*>(&index->searchMethod_), sizeof(index->searchMethod_));


        LOG(LIB_INFO) << "searchMethod: " << index->searchMethod_;

        index->fstdistfunc_ = getDistFunc(index->dist_func_type_);
        index->iscosine_ = (index->dist_func_type_ == kNormCosine);
        size_t data_plus_links0_size = index->memoryPerObject_ * index->totalElementsStored_;
        index->data_level0_memory_ = static_cast<char*>(malloc(data_plus_links0_size + 64));
        istr.read(index->data_level0_memory_, data_plus_links0_size);
        index->linkLists_ = static_cast<char**>(malloc((sizeof(void *) * index->totalElementsStored_) + 64));
        index->data_rearranged_.resize(index->totalElementsStored_);

        for (size_t i = 0; i <index->totalElementsStored_; i++) {
            size_t link_list_size;
             istr.read(reinterpret_cast<char*>(&link_list_size), sizeof(link_list_size));

            if (link_list_size == 0) {
                index->linkLists_[i] = nullptr;
            } else {
                index->linkLists_[i] =  static_cast<char*>(malloc(link_list_size));
                istr.read(index->linkLists_[i], link_list_size);
            }
            index->data_rearranged_[i] = new Object(index->data_level0_memory_ +
            (i) * index->memoryPerObject_ + index->offsetData_);
        }
        index->visitedlistpool = new VisitedListPool(1, index->totalElementsStored_);
        index->ResetQueryTimeParams();
    }



    template<typename Dist>
    void IndexWrap<Dist>::saveIndex(WriteBuffer & ostr, bool save_data) {
        if (save_data) {
            size_t size = data.size();
            ostr.write(reinterpret_cast<char*>(&size),sizeof(size_t));
            for (auto &obj: data) {
                size_t obj_size = obj->bufferlength();
                ostr.write(reinterpret_cast<char*>(&obj_size),sizeof(size_t));
                ostr.write(obj->buffer(), obj_size);
            }
        }

        index->totalElementsStored_ =  index->ElList_.size();
        ostr.write(reinterpret_cast<char*>(&index->totalElementsStored_),sizeof(index->totalElementsStored_));
        ostr.write(reinterpret_cast<char*>(&index->memoryPerObject_), sizeof(index->memoryPerObject_));
        ostr.write(reinterpret_cast<char*>(&index->offsetLevel0_), sizeof(index->offsetLevel0_));
        ostr.write(reinterpret_cast<char*>(&index->offsetData_), sizeof(index->offsetData_));
        ostr.write(reinterpret_cast<char*>(&index->maxlevel_), sizeof(index->maxlevel_));
        ostr.write(reinterpret_cast<char*>(&index->enterpointId_), sizeof(index->enterpointId_));
        ostr.write(reinterpret_cast<char*>(&index->maxM_), sizeof(index->maxM_));
        ostr.write(reinterpret_cast<char*>(&index->maxM0_), sizeof(index->maxM0_));
        ostr.write(reinterpret_cast<char*>(&index->dist_func_type_), sizeof(index->dist_func_type_));
        ostr.write(reinterpret_cast<char*>(&index->searchMethod_), sizeof(index->searchMethod_));

        size_t data_plus_links0_size = index->memoryPerObject_ * index->totalElementsStored_;

        ostr.write(index->data_level0_memory_, data_plus_links0_size);
        for (size_t i = 0; i < index->totalElementsStored_;++i){
            size_t sizemass = ((index->ElList_[i]->level) * (index->maxM_ + 1)) * sizeof(int);
             ostr.write(reinterpret_cast<char*>(&sizemass), sizeof(sizemass));
             if(sizemass){
                  ostr.write(index->linkLists_[i], sizemass);
             }
        }
    }

    template<typename Dist>
    KNNQueue<Dist> * IndexWrap<Dist>::knnQuery(const Object &obj, size_t k) {
        KNNQuery<Dist> knn(*space, &obj, k);
        index->Search(&knn, -1);
        return knn.Result()->Clone();
    }

    template<typename Dist>
    void IndexWrap<Dist>::addPoint(const Object &point) {
        data.push_back(new Object(data.size(), -1, point.datalength(), point.data()));
    }


    template<typename Dist>
    void IndexWrap<Dist>::addPointUnsafe(const Object* obj){
        data.push_back(obj);
    }


    template<typename Dist>
    void IndexWrap<Dist>::addBatch(const ObjectVector &new_data) {
        for (const auto &elem: new_data) {
            addPoint(*elem);
        }
    }

    template<typename Dist>
    void IndexWrap<Dist>::addBatchUnsafe(ObjectVector &&new_data){
        data = std::move(new_data);
    }

    template<typename Dist>
    void IndexWrap<Dist>::freeAndClearObjectVector() {
        for (auto & i : data){
            delete i;
        }
        data.clear();
    }

    template<typename Dist>
    IndexWrap<Dist>::~IndexWrap(){
        freeAndClearObjectVector();
    }
}

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


MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, const Block & index_sample_block_,
     std::unique_ptr<HnswWrapper::IndexWrap<float>> index_impl_)
    : index_name(index_name_),
    index_sample_block(index_sample_block_), index_impl(std::move(index_impl_))
{}


void MergeTreeIndexGranuleSimpleHnsw::serializeBinary(WriteBuffer & ostr) const {
     index_impl->saveIndex(ostr, true);
}

void MergeTreeIndexGranuleSimpleHnsw::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/){
    if(!index_impl){
        index_impl = std::make_unique<HnswWrapper::IndexWrap<float>>("l2");
    }
    index_impl->loadIndex(istr, true); // true for loading data
}

MergeTreeIndexAggregatorSimpleHnsw::MergeTreeIndexAggregatorSimpleHnsw(const String & index_name_, 
const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSimpleHnsw::getGranuleAndReset()
{
    auto impl = std::make_unique<HnswWrapper::IndexWrap<float>>("l2");
    impl->addBatchUnsafe(std::move(data));
    impl->createIndex();
    LOG_DEBUG(&Poco::Logger::get("Hnsw"), "Create here");
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index_name, index_sample_block,std::move(impl));
}

void MergeTreeIndexAggregatorSimpleHnsw::update(const Block & block, size_t * pos, size_t limit){
     if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(&Poco::Logger::get("Hnsw"), "Update here");
    size_t rows_read = std::min(limit, block.rows() - *pos);

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

    for (size_t i = 0; i < rows_read; ++i) {
        Field field;
        column->get(i, field);

        auto field_array = field.safeGet<Tuple>();

        auto *obj = new similarity::Object(data.size(), -1, field_array.size() * sizeof(float), nullptr); 

        float *raw_vector = reinterpret_cast<float*>(obj->data());

        // Store vectors in the flatten arrays
        for (size_t j = 0; j < field_array.size();++j) {
            auto num = field_array.at(j).safeGet<Float32>();
            raw_vector[j] = num;
        }
         // true here means that the element in vector is responsible for deleting
        data.push_back(obj); 
    }

    *pos += rows_read;
}

MergeTreeIndexConditionSimpleHnsw::MergeTreeIndexConditionSimpleHnsw(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
    , condition(query, context)
{
}

bool MergeTreeIndexConditionSimpleHnsw::alwaysUnknownOrTrue() const
{
    LOG_DEBUG(&Poco::Logger::get("SimpleHnsw"), "Condition -- {}", condition.alwaysUnknownOrTrue());
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexConditionSimpleHnsw::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{

    LOG_DEBUG(&Poco::Logger::get("SimpleHnsw"), "Checking granule begin");
    float comp_dist = condition.getComparisonDistance();
    std::vector<float> target_vec = condition.getTargetVector();
    similarity::Object target(-1,-1, target_vec.size() * sizeof(float), target_vec.data());
    std::shared_ptr<MergeTreeIndexGranuleSimpleHnsw> granule
         = std::dynamic_pointer_cast<MergeTreeIndexGranuleSimpleHnsw>(idx_granule);
    if (!granule)
         throw Exception(
             "SimpleHnsw index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    auto result = std::unique_ptr<similarity::KNNQueue<float>>(granule->index_impl->knnQuery(target, 1));
    LOG_DEBUG(&Poco::Logger::get("SimpleHnsw"), "check res -- {}", result->TopDistance());
    return result->TopDistance() < comp_dist;
    // auto center_obj = std::make_unique<similarity::Object>(-1, -1, 3 * sizeof(float), nullptr);
    // auto *raw_center_obj = reinterpret_cast<float*>(center_obj->data());
    // raw_center_obj[0] = 0.2;
    // raw_center_obj[1] = 0.2;
    // raw_center_obj[2] = 0.2;
    // std::shared_ptr<MergeTreeIndexGranuleSimpleHnsw> granule
    //      = std::dynamic_pointer_cast<MergeTreeIndexGranuleSimpleHnsw>(idx_granule);
    // if (!granule)
    //      throw Exception(
    //          "SimpleHnsw index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    // auto result = std::unique_ptr<similarity::KNNQueue<float>>(granule->index_impl->knnQuery(*center_obj, 1));
    // return result->TopDistance() < 10;
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

    return true;
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
