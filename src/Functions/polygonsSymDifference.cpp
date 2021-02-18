// #include <Functions/FunctionFactory.h>
// #include <Functions/geometryConverters.h>

// #include <boost/geometry.hpp>
// #include <boost/geometry/geometries/point_xy.hpp>
// #include <boost/geometry/geometries/polygon.hpp>

// #include <common/logger_useful.h>

// #include <Columns/ColumnArray.h>
// #include <Columns/ColumnTuple.h>
// #include <Columns/ColumnConst.h>
// #include <DataTypes/DataTypeArray.h>
// #include <DataTypes/DataTypeTuple.h>
// #include <DataTypes/DataTypeCustomGeo.h>

// #include <memory>
// #include <utility>

// namespace DB
// {

// template <typename Point>
// class FunctionPolygonsSymDifference : public IFunction
// {
// public:
//     static const char * name;

//     explicit FunctionPolygonsSymDifference() = default;

//     static FunctionPtr create(const Context &)
//     {
//         return std::make_shared<FunctionPolygonsSymDifference>();
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
//         return 2;
//     }

//     DataTypePtr getReturnTypeImpl(const DataTypes &) const override
//     {
//         return DataTypeCustomMultiPolygonSerialization::nestedDataType();
//     }

//     ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
//     {
//         checkColumnTypeOrThrow<Point, MultiPolygon>(arguments[0]);
//         auto first_parser = MultiPolygonFromColumnParser<Point>(std::move(arguments[0].column->convertToFullColumnIfConst()));
//         MultiPolygon<Point> first_container;

//         checkColumnTypeOrThrow<Point, MultiPolygon>(arguments[1]);
//         auto second_parser = MultiPolygonFromColumnParser<Point>(std::move(arguments[1].column->convertToFullColumnIfConst()));
//         MultiPolygon<Point> second_container;

//         MultiPolygonSerializer<Point> serializer;
//         MultiPolygon<Point> sym_difference{};

//         /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
//         for (size_t i = 0; i < input_rows_count; i++)
//         {
//             first_parser.get(first_container, i);
//             second_parser.get(second_container, i);

//             boost::geometry::correct(first_container);
//             boost::geometry::correct(second_container);

//             boost::geometry::sym_difference(first_container, second_container, sym_difference);

//             serializer.add(sym_difference);
//         }

//         return serializer.finalize();
//     }

//     bool useDefaultImplementationForConstants() const override
//     {
//         return true;
//     }
// };

// template <>
// const char * FunctionPolygonsSymDifference<CartesianPoint>::name = "polygonsSymDifferenceCartesian";

// template <>
// const char * FunctionPolygonsSymDifference<GeographicPoint>::name = "polygonsSymDifferenceGeographic";

// void registerFunctionPolygonsSymDifference(FunctionFactory & factory)
// {
//     factory.registerFunction<FunctionPolygonsSymDifference<CartesianPoint>>();
//     factory.registerFunction<FunctionPolygonsSymDifference<GeographicPoint>>();
// }

// }
