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
  * Polygons are read and stored as multi_polygons from boost::geometry in Euclidean coordinates.
  * An implementation should inherit from this base class and preprocess the data upon construction if needed.
  * It must override the find method of this class which retrieves the polygon containing a single point.
  */
class IPolygonDictionary : public IDictionaryBase
{
public:
    /** Controls the different types of polygons allowed as input.
     *  The structure of a multi-polygon is as follows:
     *      - A multi-polygon is represented by a nonempty array of polygons.
     *      - A polygon is represented by a nonempty array of rings. The first element represents the outer ring. Zero
     *        or more following rings are cut out from the polygon.
     *      - A ring is represented by a nonempty array of points.
     *      - A point is represented by its coordinates stored in an according structure (see below).
     *  A simple polygon is represented by an one-dimensional array of points, stored in the according structure.
     */
    enum class InputType
    {
        MultiPolygon,
        SimplePolygon
    };
    /** Controls the different types allowed for providing the coordinates of points.
      * Right now a point can be represented by either an array or a tuple of two Float64 values.
      */
    enum class PointType
    {
        Array,
        Tuple,
    };
    IPolygonDictionary(
            const std::string & database_,
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            InputType input_type_,
            PointType point_type_);

    const std::string & getDatabase() const override;
    const std::string & getName() const override;
    const std::string & getFullName() const override;

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

    /** Single coordinate type. */
    using Coord = Float32;
    /** A two-dimensional point in Euclidean coordinates. */
    using Point = bg::model::d2::point_xy<Coord, bg::cs::cartesian>;
    /** A polygon in boost is a an outer ring of points with zero or more cut out inner rings. */
    using Polygon = bg::model::polygon<Point>;
    /** A ring in boost used for describing the polygons. */
    using Ring = bg::model::ring<Point>;

protected:
    /** Returns true if the given point can be found in the polygon dictionary.
     *  If true id is set to the index of a polygon containing the given point.
     *  Overridden in different implementations of this interface.
     */
    virtual bool find(const Point & point, size_t & id) const = 0;

    std::vector<Polygon> polygons;
    /** Since the original data may have been in the form of multi-polygons, an id is stored for each single polygon
     *  corresponding to the row in which any other attributes for this entry are located.
     */
    std::vector<size_t> ids;

    const std::string database;
    const std::string name;
    const std::string full_name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    const InputType input_type;
    const PointType point_type;

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

    /** Helper functions to retrieve and instantiate the provided null value of an attribute.
     *  Since a null value is obligatory for every attribute they are simply appended to null_values defined below.
     */
    template <typename T>
    void appendNullValueImpl(const Field & null_value);
    void appendNullValue(AttributeUnderlyingType type, const Field & value);

    /** Helper function for retrieving the value of an attribute by key. */
    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsImpl(size_t attribute_ind, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const;

    /** A mapping from the names of the attributes to their index in the two vectors defined below. */
    std::map<std::string, size_t> attribute_index_by_name;
    /** A vector of columns storing the values of each attribute. */
    Columns attributes;
    /** A vector of null values corresponding to each attribute. */
    std::vector<std::variant<
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        UInt128,
        Int8,
        Int16,
        Int32,
        Int64,
        Decimal32,
        Decimal64,
        Decimal128,
        Float32,
        Float64,
        String>> null_values;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    mutable std::atomic<size_t> query_count{0};

    /** Extracts a list of polygons from a column according to input_type and point_type.
     *  The polygons are appended to the dictionary with the corresponding ids.
     */
    void extractPolygons(const ColumnPtr & column);

    /** Extracts a list of points from two columns representing their x and y coordinates. */
    static std::vector<Point> extractPoints(const Columns &key_columns);
};

}

