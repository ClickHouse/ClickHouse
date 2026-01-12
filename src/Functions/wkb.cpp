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
extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionWkb : public IFunction
{
public:
    static inline const char * name = "wkb";

    explicit FunctionWkb() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionWkb>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeString>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /*
    * Functions like recursiveRemoveLowCardinality don't pay enough attention to custom types and just erase
    * the information about it during type conversions.
    * While it is a big problem the quick solution would be just to disable default low cardinality implementation
    * because it doesn't make a lot of sense for geo types.
    */
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnString::create();

        std::shared_ptr<IWKBTransform> transform;
        if (arguments[0].type->getName() == WKBPointTransform::name)
            transform = std::make_shared<WKBPointTransform>();
        else if (arguments[0].type->getName() == WKBLineStringTransform::name)
            transform = std::make_shared<WKBLineStringTransform>();
        else if (arguments[0].type->getName() == WKBPolygonTransform::name)
            transform = std::make_shared<WKBPolygonTransform>();
        else if (arguments[0].type->getName() == WKBMultiLineStringTransform::name)
            transform = std::make_shared<WKBMultiLineStringTransform>();
        else if (arguments[0].type->getName() == WKBMultiPolygonTransform::name)
            transform = std::make_shared<WKBMultiPolygonTransform>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wkb function is not supported for type {}", arguments[0].type->getName());

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

REGISTER_FUNCTION(Wkb)
{
    factory.registerFunction<FunctionWkb>(FunctionDocumentation{
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
    ┌─hex(wkb(a))─-----------------------------------------┐
    │ 010100000000000000000000000000000000003440           │
    └──────────────────────────────────────────────────────┘
                )"},
        },
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Geo,
    });
}

}
