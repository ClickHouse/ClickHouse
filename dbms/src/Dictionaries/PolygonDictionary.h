#pragma once

#include <atomic>
#include <variant>
#include <Core/Block.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
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

    std::string getKeyDescription() const;

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

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ResultArrayType<TYPE> & out) const;
        DECLARE(UInt8)
        DECLARE(UInt16)
        DECLARE(UInt32)
        DECLARE(UInt64)
        DECLARE(UInt128)
        DECLARE(Int8)
        DECLARE(Int16)
        DECLARE(Int32)
        DECLARE(Int64)
        DECLARE(Float32)
        DECLARE(Float64)
        DECLARE(Decimal32)
        DECLARE(Decimal64)
        DECLARE(Decimal128)
#undef DECLARE

        void getString(const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const;
        DECLARE(UInt8)
        DECLARE(UInt16)
        DECLARE(UInt32)
        DECLARE(UInt64)
        DECLARE(UInt128)
        DECLARE(Int8)
        DECLARE(Int16)
        DECLARE(Int32)
        DECLARE(Int64)
        DECLARE(Float32)
        DECLARE(Float64)
        DECLARE(Decimal32)
        DECLARE(Decimal64)
        DECLARE(Decimal128)
#undef DECLARE

        void getString(
                const std::string & attribute_name,
                const Columns & key_columns,
                const DataTypes & key_types,
                const ColumnString * const def,
                ColumnString * const out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const;
        DECLARE(UInt8)
        DECLARE(UInt16)
        DECLARE(UInt32)
        DECLARE(UInt64)
        DECLARE(UInt128)
        DECLARE(Int8)
        DECLARE(Int16)
        DECLARE(Int32)
        DECLARE(Int64)
        DECLARE(Float32)
        DECLARE(Float64)
        DECLARE(Decimal32)
        DECLARE(Decimal64)
        DECLARE(Decimal128)
#undef DECLARE

        void getString(
                const std::string & attribute_name,
                const Columns & key_columns,
                const DataTypes & key_types,
                const String & def,
                ColumnString * const out) const;

    // TODO: Refactor design to perform stronger checks, i.e. make this an override.
    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

protected:
    using Point = bg::model::point<Float64, 2, bg::cs::cartesian>;
    using Polygon = bg::model::polygon<Point>;
    using MultiPolygon = bg::model::multi_polygon<Polygon>;

    std::vector<MultiPolygon> polygons;

    virtual bool find(const Point & point, size_t & id) const = 0;

    const std::string name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

private:
    void createAttributes();
    void blockToAttributes(const Block & block);
    void loadData();

    void calculateBytesAllocated();

    size_t getAttributeIndex(const std::string & attribute_name) const;
    template <typename T>
    static T getNullValue(const Field & field) const;

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsImpl(size_t attribute_ind, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const;


        std::map<std::string, size_t> attribute_index_by_name;
    Columns attributes;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    mutable std::atomic<size_t> query_count{0};

    static std::vector<Point> extractPoints(const Columns &key_columns);
    static Point fieldToPoint(const Field & field);
    static Polygon fieldToPolygon(const Field & field);
    static MultiPolygon fieldToMultiPolygon(const Field & field);

    static constexpr size_t DIM = 2;
};

class SimplePolygonDictionary : public IPolygonDictionary
{
public:
    SimplePolygonDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_);

    std::shared_ptr<const IExternalLoadable> clone() const override;

private:
    bool find(const Point & point, size_t & id) const override;
};

}