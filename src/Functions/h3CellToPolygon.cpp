#include <Functions/h3Common.h>

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

bool getCellBoundary(H3Index h3index, const H3Validator & validator, CellBoundary & boundary)
{
    if (!validator.validateCell(h3index))
        return false;

    const auto err = cellToBoundary(h3index, &boundary);
    if (err)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect H3 index: {}, error: {}", h3index, err);

    return true;
}

class FunctionH3CellToPolygon final : public IFunction
{
public:
    static constexpr auto name = "h3CellToPolygon";

    explicit FunctionH3CellToPolygon(const ContextPtr & context) : validator(context) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionH3CellToPolygon>(context); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return !validator.throw_on_error; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    // Polygon cannot be nullable, so handle null rows as empty polygons ourselves.
    bool useDefaultImplementationForNulls() const override { return false; }
    String getSignatureString() const override { return "(MaybeNullable(UInt64)) -> Polygon"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto full_column = arguments[0].column->convertToFullColumnIfConst();
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(full_column.get());
        const auto * column = checkAndGetColumn<ColumnUInt64>(
            nullable_column ? &nullable_column->getNestedColumn() : full_column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument 1 of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                getName());

        auto longitude = ColumnFloat64::create();
        auto latitude = ColumnFloat64::create();
        auto ring_offsets = ColumnArray::ColumnOffsets::create();
        auto polygon_offsets = ColumnArray::ColumnOffsets::create(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (nullable_column && nullable_column->isNullAt(row))
            {
                polygon_offsets->getData()[row] = ring_offsets->size();
                continue;
            }

            CellBoundary boundary{};
            if (getCellBoundary(column->getData()[row], validator, boundary))
            {
                for (int vertex = 0; vertex <= boundary.numVerts; ++vertex)
                {
                    const auto & point = boundary.verts[vertex == boundary.numVerts ? 0 : vertex];
                    longitude->insert(radsToDegs(point.lng));
                    latitude->insert(radsToDegs(point.lat));
                }
                ring_offsets->getData().push_back(longitude->size());
            }

            polygon_offsets->getData()[row] = ring_offsets->size();
        }

        return ColumnArray::create(
            ColumnArray::create(
                ColumnTuple::create(Columns{std::move(longitude), std::move(latitude)}),
                std::move(ring_offsets)),
            std::move(polygon_offsets));
    }

private:
    H3Validator validator;
};

class FunctionH3CellToBoundaryWKT final : public IFunction
{
public:
    static constexpr auto name = "h3CellToBoundaryWKT";

    explicit FunctionH3CellToBoundaryWKT(const ContextPtr & context) : validator(context) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionH3CellToBoundaryWKT>(context); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return !validator.throw_on_error; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    String getSignatureString() const override { return "(UInt64) -> String"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto full_column = arguments[0].column->convertToFullColumnIfConst();
        const auto * column = checkAndGetColumn<ColumnUInt64>(full_column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument 1 of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                getName());

        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            CellBoundary boundary{};
            if (!getCellBoundary(column->getData()[row], validator, boundary))
            {
                result->insertDefault();
                continue;
            }

            String wkt = "POLYGON((";
            for (int vertex = 0; vertex <= boundary.numVerts; ++vertex)
            {
                const auto & point = boundary.verts[vertex == boundary.numVerts ? 0 : vertex];
                if (vertex)
                    wkt += ", ";
                wkt += fmt::format("{:.17g} {:.17g}", radsToDegs(point.lng), radsToDegs(point.lat));
            }
            wkt += "))";
            result->insertData(wkt.data(), wkt.size());
        }

        return result;
    }

private:
    H3Validator validator;
};

}

REGISTER_FUNCTION(H3CellToPolygon)
{
    factory.registerFunction<FunctionH3CellToPolygon>(FunctionDocumentation{
        .description = "Returns a closed H3 cell boundary as a Polygon with (longitude, latitude) coordinates.",
        .syntax = "h3CellToPolygon(h3Index)",
        .arguments = {{"h3Index", "H3 cell index.", {"UInt64"}}},
        .returned_value = {"A Polygon containing a single closed boundary ring.", {"Polygon"}},
        .examples = {{"H3 cell polygon type", "SELECT toTypeName(h3CellToPolygon(599686042433355775))", "Polygon"}},
        .introduced_in = {26, 11},
        .category = FunctionDocumentation::Category::Geo});

    factory.registerFunction<FunctionH3CellToBoundaryWKT>(FunctionDocumentation{
        .description = "Returns the closed H3 cell boundary as a WKT Polygon string with (longitude, latitude) coordinates.",
        .syntax = "h3CellToBoundaryWKT(h3Index)",
        .arguments = {{"h3Index", "H3 cell index.", {"UInt64"}}},
        .returned_value = {"A WKT Polygon string.", {"String"}},
        .examples = {{"H3 cell WKT", "SELECT startsWith(h3CellToBoundaryWKT(599686042433355775), 'POLYGON((')", "1"}},
        .introduced_in = {26, 11},
        .category = FunctionDocumentation::Category::Geo});
    factory.registerAlias("h3_cell_to_boundary_wkt", FunctionH3CellToBoundaryWKT::name, FunctionFactory::Case::Insensitive);
}

}

#endif
