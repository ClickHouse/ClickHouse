// #include <Functions/FunctionFactory.h>
// #include <Functions/geometryConverters.h>

// #include <boost/geometry.hpp>
// #include <boost/geometry/geometries/point_xy.hpp>
// #include <boost/geometry/geometries/polygon.hpp>

// #include <common/logger_useful.h>

// #include <Columns/ColumnArray.h>
// #include <Columns/ColumnTuple.h>
// #include <Columns/ColumnConst.h>
// #include <Columns/ColumnsNumber.h>
// #include <DataTypes/DataTypesNumber.h>
// #include <DataTypes/DataTypeArray.h>
// #include <DataTypes/DataTypeTuple.h>
// #include <DataTypes/DataTypeCustomGeo.h>

// #include <memory>
// #include <utility>

// namespace DB
// {

// template <typename Point>
// class FunctionPolygonsWithin : public IFunction
// {
// public:
//     static inline const char * name;

//     explicit FunctionPolygonsWithin() = default;

//     static FunctionPtr create(const Context &)
//     {
//         return std::make_shared<FunctionPolygonsWithin>();
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
//         return std::make_shared<DataTypeUInt8>();
//     }

//     ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
//     {
//         auto first_parser = getConverterBasedOnType<Point>(arguments[0]);
//         auto second_parser = getConverterBasedOnType<Point>(arguments[1]);     

//         auto first = parseFigure(first_parser);
//         auto second = parseFigure(second_parser);

//         auto res_column = ColumnUInt8::create();
//         auto & res_data = res_column->getData();

//         /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
//         for (size_t i = 0; i < input_rows_count; i++)
//         {
//             boost::geometry::correct(first[i]);
//             boost::geometry::correct(second[i]);

//             res_data.emplace_back(boost::geometry::within(first[i], second[i]));
//         }

//         return res_column;
//     }

//     bool useDefaultImplementationForConstants() const override
//     {
//         return true;
//     }
// };


// template <>
// const char * FunctionPolygonsWithin<CartesianPoint>::name = "polygonsWithinCartesian";

// template <>
// const char * FunctionPolygonsWithin<GeographicPoint>::name = "polygonsWithinGeographic";


// void registerFunctionPolygonsWithin(FunctionFactory & factory)
// {
//     factory.registerFunction<FunctionPolygonsWithin<CartesianPoint>>();
//     factory.registerFunction<FunctionPolygonsWithin<GeographicPoint>>();
// }

// }
