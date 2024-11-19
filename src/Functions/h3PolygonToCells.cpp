#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/IFunction.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <constants.h>
#include <h3api.h>


// TODO: from h3ToChildren, why this exact number?
static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int ILLEGAL_COLUMN;
}

class GeoPolygonContainer {
private:
    // Store the polygon data
    std::vector<LatLng> mainLoopVerts;
    std::vector<std::vector<LatLng>> holeVerts;
    
    // Temporary storage for C-style structs
    mutable GeoLoop mutableMainLoop;
    mutable GeoPolygon mutablePolygon;
    mutable std::vector<GeoLoop> mutableHoles;

public:
    // Constructor to create from C++ data
    GeoPolygonContainer(const std::vector<LatLng>& mainLoop, 
                        const std::vector<std::vector<LatLng>>& holes = {}) 
        : mainLoopVerts(mainLoop), holeVerts(holes) {}

    // Method to get C-style GeoPolygon pointer
    const GeoPolygon* unwrap() const {
        // Prepare main loop
        mutableMainLoop = {
            static_cast<int>(mainLoopVerts.size()),
            const_cast<LatLng*>(mainLoopVerts.data())
        };

        // Prepare holes
        mutableHoles.clear();
        mutableHoles.reserve(holeVerts.size());
        for (const auto& hole : holeVerts) {
            mutableHoles.push_back({
                static_cast<int>(hole.size()),
                const_cast<LatLng*>(hole.data())
            });
        }

        // Prepare full polygon
        mutablePolygon = {
            mutableMainLoop,
            static_cast<int>(mutableHoles.size()),
            mutableHoles.data()
        };

        return &mutablePolygon;
    }

    // Additional utility methods
    size_t size() const { return mainLoopVerts.size(); }
    bool empty() const { return mainLoopVerts.empty(); }
};

class FunctionH3PolygonToCells : public IFunction
{
public:
    static constexpr auto name = "h3PolygonToCells";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3PolygonToCells>(); }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_resolution = checkAndGetColumn<ColumnUInt8>(non_const_arguments[1].column.get());
        if (!col_resolution)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt8.",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_resolution = col_resolution->getData();


        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        auto current_offset = 0;


        callOnGeometryDataType<CartesianPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            // polygonToCells does not work for points and lines
            if constexpr (std::is_same_v<ColumnToPointsConverter<CartesianPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must not be Point", getName());
            else if constexpr (std::is_same_v<ColumnToLineStringsConverter<CartesianPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must not be LineString", getName());

            // all geometries will be of same kind
            auto geometries = Converter::convert(arguments[0].column);
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                // check resolution *once* before looping over polygons
                const UInt8 resolution = data_resolution[row];
                if (resolution > MAX_H3_RES)
                    throw Exception(
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                        toString(resolution), getName(), toString(MAX_H3_RES));

                // will be ring, polygon, or multipolygon (boost::geometry)
                auto geometry = geometries[row];
                boost::geometry::correct(geometry); // TODO: Is this good or bad?

                // always work on multipolygon
                CartesianMultiPolygon multi_polygon;
                if constexpr (std::is_same_v<ColumnToMultiPolygonsConverter<CartesianPoint>, Converter>)
                    multi_polygon = geometry;
                else if constexpr (std::is_same_v<ColumnToPolygonsConverter<CartesianPoint>, Converter>)
                    multi_polygon = boost::geometry::model::multi_polygon({ geometry });
                else if constexpr (std::is_same_v<ColumnToRingsConverter<CartesianPoint>, Converter>)
                    multi_polygon = boost::geometry::model::multi_polygon({ boost::geometry::model::polygon({ geometry }) });

                // will only iterate once for ring and polygon
                for (auto & polygon : multi_polygon)
                {
                    std::vector<LatLng> exterior;
                    for (auto & point : polygon.outer())
                    {
                        LatLng vert = { degsToRads(point.y()), degsToRads(point.x()) };
                        exterior.push_back(vert);
                    }

                    std::vector<std::vector<LatLng>> holes;
                    for (auto & inner : polygon.inners())
                    {
                        std::vector<LatLng> hole;
                        for (auto & point : inner)
                        {
                            LatLng vert = { degsToRads(point.y()), degsToRads(point.x()) };
                            hole.push_back(vert);
                        }
                        holes.push_back(hole);
                    }

                    GeoPolygonContainer polygon_wrapper(exterior, holes);

                    const size_t vec_size = maxPolygonToCellsSize(polygon_wrapper.unwrap(), resolution);
                    if (vec_size > MAX_ARRAY_SIZE)
                        throw Exception(
                            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "The result of function {} (array of {} elements) will be too large with resolution argument = {}",
                            getName(), vec_size, toString(resolution));

                    std::vector<H3Index> hindex_vec;
                    hindex_vec.resize(vec_size);
                    polygonToCells(polygon_wrapper.unwrap(), resolution, hindex_vec.data());

                    dst_data.reserve(dst_data.size() + vec_size);
                    for (auto hindex : hindex_vec)
                    {
                        if (hindex != 0)
                        {
                            ++current_offset;
                            dst_data.insert(hindex);
                        }
                    }

                    dst_offsets[row] = current_offset;
                }
            }
        }
        );


        return dst;
    }
};

REGISTER_FUNCTION(H3PolygonToCells)
{
    factory.registerFunction<FunctionH3PolygonToCells>();
}

}

#endif
