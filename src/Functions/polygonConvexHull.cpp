// #include <Functions/FunctionFactory.h>
// #include <Functions/geometryConverters.h>

// #include <boost/geometry.hpp>
// #include <boost/geometry/geometries/point_xy.hpp>
// #include <boost/geometry/geometries/polygon.hpp>

// #include <common/logger_useful.h>

// #include <Columns/ColumnArray.h>
// #include <Columns/ColumnTuple.h>
// #include <Columns/ColumnsNumber.h>
// #include <DataTypes/DataTypeArray.h>
// #include <DataTypes/DataTypeTuple.h>
// #include <DataTypes/DataTypeCustomGeo.h>

// #include <memory>
// #include <string>

// namespace DB
// {

// template <typename Point>
// class FunctionPolygonConvexHull : public IFunction
// {
// public:
//     static const char * name;

//     explicit FunctionPolygonConvexHull() = default;

//     static FunctionPtr create(const Context &)
//     {
//         return std::make_shared<FunctionPolygonConvexHull>();
//     }

//     String getName() const override
//     {
//         return name;
//     }

//     bool isVariadic() const override
//     {
//         return false;
//     }

//     size_t getNumberOfArguments() const override
//     {
//         return 1;
//     }

//     DataTypePtr getReturnTypeImpl(const DataTypes &) const override
//     {
//         return DataTypeCustomPolygonSerialization::nestedDataType();
//     }

//     ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
//     {
//         auto parser = getConverterBasedOnType<Point>(arguments[0]);  
//         auto figures = parseFigure(parser);

//         PolygonSerializer<Point> serializer;

//         for (size_t i = 0; i < input_rows_count; i++)
//         {
//             Polygon<Point> convex_hull{};
//             boost::geometry::convex_hull(figures[i], convex_hull);
//             serializer.add(convex_hull);
//         }

//         return serializer.finalize();
//     }

//     bool useDefaultImplementationForConstants() const override
//     {
//         return true;
//     }
// };


// template <>
// const char * FunctionPolygonConvexHull<CartesianPoint>::name = "polygonConvexHullCartesian";


// void registerFunctionPolygonConvexHull(FunctionFactory & factory)
// {
//     factory.registerFunction<FunctionPolygonConvexHull<CartesianPoint>>();
// }

// }
