#pragma once

#include <atomic>
#include <variant>
#include <Core/Block.h>
#include <Common/Arena.h>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/multi_polygon.hpp>

#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"

namespace DB
{

namespace bg = boost::geometry;

class IPolygonDictionary : public IDictionaryBase
{
public:
    IPolygonDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_);

    std::string getName() const override;

    std::string getTypeName() const override;

    size_t getBytesAllocated() const override;

    size_t getQueryCount() const override;

    double getHitRate() const override;

    size_t getElementCount() const override;

    double getLoadFactor() const override;

    const IDictionarySource * getSource() const override;

    const DictionaryStructure & getStructure() const override;

    const DictionaryLifetime  & getLifetime() const override;

    bool isInjective(const std::string & attribute_name) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

    std::shared_ptr<const IExternalLoadable> clone() const override;

    // TODO: Refactor design to perform stronger checks, i.e. make this an override.
    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) ;

protected:
    using Point = bg::model::point<Float64, 2, bg::cs::cartesian>;
    using Polygon = bg::model::polygon<Point>;
    using MultiPolygon = bg::model::multi_polygon<Polygon>;

    std::vector<MultiPolygon> polygons;

private:
    virtual void generate() = 0;
    virtual bool find(const Point & point, size_t & id) const = 0;

    void createAttributes();
    void blockToAttributes(const Block & block);
    void loadData();

    void calculateBytesAllocated();

    const std::string name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<Block> blocks;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    mutable std::atomic<size_t> query_count{0};


    static Point fieldToPoint(const Field & field);
    static Polygon fieldToPolygon(const Field & field);
    static MultiPolygon fieldToMultiPolygon(const Field & field);

    static constexpr size_t DIM = 2;
};

class SimplePolygonDictionary : public IPolygonDictionary
{
private:
    void generate() override;
    bool find(const Point & point, size_t & id) const override;
};

}