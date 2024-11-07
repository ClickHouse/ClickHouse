#pragma once

#include <atomic>
#include <variant>
#include <Core/Block.h>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/multi_polygon.hpp>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>


namespace DB
{

namespace bg = boost::geometry;

/** An interface for polygon dictionaries.
  * Polygons are read and stored as multi_polygons from boost::geometry in Euclidean coordinates.
  * An implementation should inherit from this base class and preprocess the data upon construction if needed.
  * It must override the find method of this class which retrieves the polygon containing a single point.
  */
class IPolygonDictionary : public IDictionary
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
    enum class InputType : uint8_t
    {
        MultiPolygon,
        SimplePolygon
    };
    /** Controls the different types allowed for providing the coordinates of points.
      * Right now a point can be represented by either an array or a tuple of two Float64 values.
      */
    enum class PointType : uint8_t
    {
        Array,
        Tuple,
    };

    struct Configuration
    {
        InputType input_type = InputType::MultiPolygon;

        PointType point_type = PointType::Array;

        /// Store polygon key column. That will allow to read columns from polygon dictionary.
        bool store_polygon_key_column = false;

        bool use_async_executor = false;
    };

    IPolygonDictionary(
            const StorageID & dict_id_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            Configuration configuration_);

    std::string getTypeName() const override { return "Polygon"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load();
        if (!queries)
            return 0;
        return std::min(1.0, static_cast<double>(found_count.load()) / queries);
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return attributes_columns.empty() ? 0 : attributes_columns.front()->size(); }

    double getLoadFactor() const override { return 1.0; }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    const DictionaryLifetime  & getLifetime() const override { return dict_lifetime; }

    bool isInjective(const std::string & attribute_name) const override { return dict_struct.getAttribute(attribute_name).injective; }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Complex; }

    void convertKeyColumns(Columns & key_columns, DataTypes & key_types) const override;

    ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultOrFilter default_or_filter) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

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
    virtual bool find(const Point & point, size_t & polygon_index) const = 0;

    std::vector<Polygon> polygons;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const Configuration configuration;

private:
    /** Helper functions for loading the data from the configuration.
     *  The polygons serving as keys are extracted into boost types.
     *  All other values are stored in one column per attribute.
     */
    void setup();
    void blockToAttributes(const Block & block);
    void loadData();

    void calculateBytesAllocated();

    /** Checks whether a given attribute exists and returns its index */
    size_t getAttributeIndex(const std::string & attribute_name) const;

    /** Helper function for retrieving the value of an attribute by key. */
    template <typename AttributeType, typename ValueGetter, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const std::vector<IPolygonDictionary::Point> & requested_key_points,
        ValueGetter && get_value,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType, typename ValueGetter, typename ValueSetter>
    void getItemsShortCircuitImpl(
        const std::vector<IPolygonDictionary::Point> & requested_key_points,
        ValueGetter && get_value,
        ValueSetter && set_value,
        IColumn::Filter & default_mask) const;

    ColumnPtr key_attribute_column;

    Columns attributes_columns;

    size_t bytes_allocated = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    /** Since the original data may have been in the form of multi-polygons, an id is stored for each single polygon
     *  corresponding to the row in which any other attributes for this entry are located.
     */
    std::vector<size_t> polygon_index_to_attribute_value_index;

    /** Extracts a list of polygons from a column according to input_type and point_type.
     *  The polygons are appended to the dictionary with the corresponding ids.
     */
    void extractPolygons(const ColumnPtr & column);

    /** Extracts a list of points from two columns representing their x and y coordinates. */
    static std::vector<Point> extractPoints(const Columns &key_columns);
};

}
