#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/mercatorConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


struct SymDifferenceCartesian
{
    static inline const char * name = "polygonsSymDifferenceCartesian";
    using Point = CartesianPoint;
};

struct SymDifferenceSpherical
{
    static inline const char * name = "polygonsSymDifferenceSpherical";
    using Point = SphericalPoint;
};

struct SymDifferenceMercator
{
    static inline const char * name = "polygonsSymDifferenceMercator";
    using Point = CartesianPoint;
};


template <typename Holder>
class FunctionPolygonsSymDifference : public IFunction
{
public:
    static inline const char * name = Holder::name;

    explicit FunctionPolygonsSymDifference() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsSymDifference>();
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

                /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
                for (size_t i = 0; i < input_rows_count; i++)
                {
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    if constexpr (std::is_same_v<Holder, SymDifferenceMercator>)
                    {
                        mercatorForward(first[i]);
                        mercatorForward(second[i]);
                    }

                    MultiPolygon<Point> sym_difference{};
                    boost::geometry::sym_difference(first[i], second[i], sym_difference);

                    if constexpr (std::is_same_v<Holder, SymDifferenceMercator>)
                        mercatorBackward(sym_difference);

                    serializer.add(sym_difference);
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

void registerFunctionPolygonsSymDifference(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsSymDifference<SymDifferenceCartesian>>();
    factory.registerFunction<FunctionPolygonsSymDifference<SymDifferenceSpherical>>();
    factory.registerFunction<FunctionPolygonsSymDifference<SymDifferenceMercator>>();
}

}
