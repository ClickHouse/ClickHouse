#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/mercatorConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct WithinCartesian
{
    static inline const char * name = "polygonsWithinCartesian";
    using Point = CartesianPoint;
};

struct WithinSpherical
{
    static inline const char * name = "polygonsWithinSpherical";
    using Point = SphericalPoint;
};

struct WithinMercator
{
    static inline const char * name = "polygonsWithinMercator";
    using Point = CartesianPoint;
};

template <typename Holder>
class FunctionPolygonsWithin : public IFunction
{
public:
    static inline const char * name = Holder::name;

    explicit FunctionPolygonsWithin() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsWithin>();
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
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        using Point = typename Holder::Point;
        auto res_column = ColumnUInt8::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

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

                    if constexpr (std::is_same_v<Holder, WithinMercator>)
                    {
                        mercatorForward(first[i]);
                        mercatorForward(second[i]);
                    }

                    res_data.emplace_back(boost::geometry::within(first[i], second[i]));
                }
            }
        });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

void registerFunctionPolygonsWithin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsWithin<WithinCartesian>>();
    factory.registerFunction<FunctionPolygonsWithin<WithinSpherical>>();
    factory.registerFunction<FunctionPolygonsWithin<WithinMercator>>();
}

}
