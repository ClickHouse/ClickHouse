#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Common/WKB.h>

#include <memory>
#include <string>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// `wkb` serializes exactly the named geometry types it has a transform for. Returns `nullptr` for
/// anything else, including `Ring` and the anonymous structural types (`Array(Tuple(Float64,
/// Float64))` and friends) that the geometry dispatch otherwise reads: they have no WKB
/// representation. Shared by `getReturnTypeImpl` and `executeImpl` so the domain is one predicate.
std::shared_ptr<IWKBTransform> tryGetWKBTransform(const DataTypePtr & type)
{
    const auto & name = type->getName();
    if (name == WKBPointTransform::name)
        return std::make_shared<WKBPointTransform>();
    if (name == WKBLineStringTransform::name)
        return std::make_shared<WKBLineStringTransform>();
    if (name == WKBPolygonTransform::name)
        return std::make_shared<WKBPolygonTransform>();
    if (name == WKBMultiPointTransform::name)
        return std::make_shared<WKBMultiPointTransform>();
    if (name == WKBMultiLineStringTransform::name)
        return std::make_shared<WKBMultiLineStringTransform>();
    if (name == WKBMultiPolygonTransform::name)
        return std::make_shared<WKBMultiPolygonTransform>();
    return nullptr;
}

class FunctionWKB final : public IFunction
{
public:
    static inline const char * name = "wkb";

    explicit FunctionWKB() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionWKB>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// Stated here, so an argument `wkb` cannot serialize is refused during analysis rather than
        /// only once a row reaches `executeImpl`. `ILLEGAL_TYPE_OF_ARGUMENT` is the code
        /// `FunctionBaseVariantAdaptor` reads as type incompatibility, so a `Variant` alternative
        /// `wkb` refuses is skipped instead of failing the query.
        if (!tryGetWKBTransform(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} function is not supported for type {}",
                getName(),
                arguments[0]->getName());
        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeString>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnString::create();

        auto transform = tryGetWKBTransform(arguments[0].type);
        if (!transform)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} function is not supported for type {}",
                getName(),
                arguments[0].type->getName());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Field field;
            arguments[0].column->get(i, field);
            std::string serialized = transform->dumpObject(field);
            res_column->insertData(serialized.data(), serialized.size());
        }

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

}

REGISTER_FUNCTION(WKB)
{
    factory.registerFunction<FunctionWKB>(FunctionDocumentation{
        .description = R"(
    Parses a Well-Known Binary (WKB) representation of a Point geometry and returns it in the internal ClickHouse format.
    )",
        .syntax = "wkb(geometry)",
        .arguments{{"geometry", "The input geometry type to convert into WKB."}},
        .examples{
            {"first call",
             "CREATE TABLE IF NOT EXISTS geom1 (a Point) ENGINE = Memory();"
             "INSERT INTO geom1 VALUES((0, 0));"
             "SELECT hex(wkb(a)) FROM geom1;",
             R"(
┌─hex(wkb(a))────────────────────────────────┐
│ 010100000000000000000000000000000000000000 │
└────────────────────────────────────────────┘
                )"},
        },
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Geo,
    });

    factory.registerAlias("WKB", "wkb");
}

}
