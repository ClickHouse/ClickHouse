#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/mercatorConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct IntersectionCartesian
{
    static inline const char * name = "polygonsIntersectionCartesian";
    using Point = CartesianPoint;
};

struct IntersectionSpherical
{
    static inline const char * name = "polygonsIntersectionSpherical";
    using Point = SphericalPoint;
};

struct IntersectionMercator
{
    static inline const char * name = "polygonsIntersectionMercator";
    using Point = CartesianPoint;
};

template <typename Holder>
class FunctionPolygonsIntersection : public IFunction
{
public:
    static inline const char * name = Holder::name;

    explicit FunctionPolygonsIntersection() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsIntersection>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        /// Intersection of each with figure with each could be easily represent as MultiPolygon.
        return DataTypeCustomMultiPolygonSerialization::nestedDataType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        using Point = typename Holder::Point;

        MultiPolygonSerializer<Point> serializer;

        callOnTwoGeometryDataTypes<Point>(arguments[0].type, arguments[1].type, [&](const auto & left_type, const auto & right_type)
        {
            using LeftConverterType = std::decay_t<decltype(left_type)>;
            using RightConverterType = std::decay_t<decltype(right_type)>;

            using LeftConverter = typename LeftConverterType::Type;
            using RightConverter = typename RightConverterType::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>)
                throw Exception(fmt::format("Any argument of function {} must not be Point", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else
            {
                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                /// We are not interested in some pitfalls in third-party libraries
                /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    /// Orient the polygons correctly.
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    if constexpr (std::is_same_v<Holder, IntersectionMercator>)
                    {
                        mercatorForward(first[i]);
                        mercatorForward(second[i]);
                    }

                    MultiPolygon<Point> intersection{};
                    /// Main work here.
                    boost::geometry::intersection(first[i], second[i], intersection);

                    if constexpr (std::is_same_v<Holder, IntersectionMercator>)
                        mercatorBackward(intersection);

                    serializer.add(intersection);
                }
            }
        });

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

void registerFunctionPolygonsIntersection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsIntersection<IntersectionCartesian>>();
    factory.registerFunction<FunctionPolygonsIntersection<IntersectionSpherical>>();
    factory.registerFunction<FunctionPolygonsIntersection<IntersectionMercator>>();
}
}
