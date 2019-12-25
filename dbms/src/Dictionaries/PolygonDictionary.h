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

/** An interface for polygon dictionaries.
 *  Polygons are read and stored as multi_polygons from boost::geometry in Euclidean coordinates.
 *  An implementation should inherit from this base class and preprocess the data upon construction if needed.
 *  It must override the find method of this class which retrieves the polygon containing a single point.
 */
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

    /** Functions used to retrieve attributes of specific type by key. */

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes &, ResultArrayType<TYPE> & out) const;
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

    void getString(const std::string & attribute_name, const Columns & key_columns, const DataTypes &, ColumnString * out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes &, \
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
            const DataTypes &,
            const ColumnString * const def,
            ColumnString * const out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes &, \
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

    /** Checks whether or not a point can be found in one of the polygons in the dictionary.
     *  The check is performed for multiple points represented by columns of their x and y coordinates.
     *  The boolean result is written to out.
     */
    // TODO: Refactor the whole dictionary design to perform stronger checks, i.e. make this an override.
    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

protected:
    /** A simple two-dimensional point in Euclidean coordinates. */
    using Point = bg::model::point<Float64, 2, bg::cs::cartesian>;
    /** A polygon in boost is a an outer ring of points with zero or more cut out inner rings. */
    using Polygon = bg::model::polygon<Point>;
    /** A multi_polygon in boost is a collection of polygons. */
    using MultiPolygon = bg::model::multi_polygon<Polygon>;

    /** Returns true if the given point can be found in the polygon dictionary.
     *  If true id is set to the index of a polygon containing the given point.
     *  Overridden in different implementations of this interface.
     */
    virtual bool find(const Point & point, size_t & id) const = 0;

    std::vector<MultiPolygon> polygons;

    const std::string name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

private:
    /** Helper functions for loading the data from the configuration.
     *  The polygons serving as keys are extracted into boost types.
     *  All other values are stored in one column per attribute.
     */
    void createAttributes();
    void blockToAttributes(const Block & block);
    void loadData();

    void calculateBytesAllocated();

    /** Checks whether a given attribute exists and returns its index */
    size_t getAttributeIndex(const std::string & attribute_name) const;

    /** Return the default type T value of field. */
    template <typename T>
    static T getNullValue(const Field & field);

    /** Helper function for retrieving the value of an attribute by key. */
    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsImpl(size_t attribute_ind, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const;

    std::map<std::string, size_t> attribute_index_by_name;
    Columns attributes;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    mutable std::atomic<size_t> query_count{0};

    /** Extracts a list of points from two columns representing their x and y coordinates. */
    static std::vector<Point> extractPoints(const Columns &key_columns);

    /** Converts an array containing two Float64s to a point. */
    static Point fieldToPoint(const Field & field);

    /** Converts an array of arrays of points to a polygon. The first array represents the outer ring and zero or more
     * following arrays represent the rings that are excluded from the polygon.
     */
    static Polygon fieldToPolygon(const Field & field);

    /** Converts an array of polygons (see above) to a multi-polygon. */
    static MultiPolygon fieldToMultiPolygon(const Field & field);

    /** The number of dimensions used. Change with great caution. */
    static constexpr size_t DIM = 2;
};

/** Simple implementation of the polygon dictionary. Doesn't generate anything during its construction.
 *  Iterates over all stored polygons for each query, checking each of them in linear time.
 *  Retrieves the first polygon in the dictionary containing a given point.
 */
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

