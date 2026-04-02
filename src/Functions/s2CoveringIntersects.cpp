#include "config.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>

#include <Functions/s2_fwd.h>
#include <s2/s2cell_union.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Internal function used by S2 projection index pruning.
/// Checks whether a single S2 cell intersects any cell in a covering (array of cell IDs).
///
/// __s2CoveringIntersects(cell_id: UInt64, cells: Array(UInt64)) → UInt8
///
/// Returns 1 if the cell identified by `cell_id` intersects at least one cell
/// in the `cells` array, 0 otherwise. Two S2 cells intersect if and only if
/// one contains the other or they are the same cell.
class FunctionS2CoveringIntersects : public IFunction
{
public:
    static constexpr auto name = "__s2CoveringIntersects";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2CoveringIntersects>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 1 of function {}. Must be UInt64",
                arguments[0]->getName(), getName());

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[1].get());
        if (!array_type || !WhichDataType(array_type->getNestedType()).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 2 of function {}. Must be Array(UInt64)",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto dst = ColumnUInt8::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        if (input_rows_count == 0)
            return dst;

        /// Unwrap cell_id (arg0) — must always be a non-const UInt64 column.
        auto col_cell_id_holder = arguments[0].column->convertToFullColumnIfConst();
        const auto * col_cell_id = checkAndGetColumn<ColumnUInt64>(col_cell_id_holder.get());
        if (!col_cell_id)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument 1 of function {}. Must be UInt64",
                arguments[0].type->getName(), getName());

        const auto & cell_id_data = col_cell_id->getData();

        /// Fast path: when the covering array (arg1) is a constant, parse it
        /// once into a sorted S2CellUnion and use binary search per row.
        /// This avoids expanding the constant into N identical rows.
        ///
        /// Since cell_id is the projection's sorting key, input rows are ordered
        /// by Hilbert curve. We cache the last matched covering cell: consecutive
        /// rows often fall within the same covering cell's [range_min, range_max],
        /// turning those lookups into O(1) instead of O(log N).
        if (isColumnConst(*arguments[1].column))
        {
            auto covering = parseCoveringFromConst(arguments[1].column);

            /// Cached iterator pointing to the last covering cell that matched.
            /// Valid when cached_valid is true.
            auto cached_it = covering.begin();
            bool cached_valid = false;

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                S2CellId cell(cell_id_data[row]);
                if (!cell.is_valid())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cell (id {}) is not valid in function {}", cell_id_data[row], getName());

                /// Face cells (level 0) intersect every covering cell in the same
                /// face. Their huge Hilbert range confuses the binary search, and
                /// caching them would pollute subsequent rows. Handle them directly.
                if (cell.level() == 0)
                {
                    /// A face cell intersects the covering if any covering cell
                    /// is on the same face.
                    bool found = false;
                    for (auto cit = covering.begin(); cit != covering.end(); ++cit)
                    {
                        if (cit->face() == cell.face())
                        {
                            found = true;
                            break;
                        }
                    }
                    dst_data[row] = found ? 1 : 0;
                    cached_valid = false;
                    continue;
                }

                /// Try cached covering cell first: if the current cell still
                /// falls within its Hilbert range, skip the binary search.
                if (cached_valid
                    && cell.range_min() <= cached_it->range_max()
                    && cell.range_max() >= cached_it->range_min())
                {
                    dst_data[row] = 1;
                    continue;
                }

                auto it = coveringIntersectsCell(covering, cell);
                if (it != covering.end())
                {
                    dst_data[row] = 1;
                    cached_it = it;
                    cached_valid = true;
                }
                else
                {
                    dst_data[row] = 0;
                    cached_valid = false;
                }
            }
            return dst;
        }

        /// This function is only emitted by ProjectionIndexS2 with a constant
        /// covering array. A non-const second argument should never occur.
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Function {} expects a constant Array(UInt64) as argument 2", getName());
    }

private:
    /// Parse a constant Array(UInt64) column into a normalized S2CellUnion.
    /// The resulting covering is sorted by cell ID, enabling binary search.
    static S2CellUnion parseCoveringFromConst(const ColumnPtr & column)
    {
        const auto & const_col = assert_cast<const ColumnConst &>(*column);
        const auto & nested = const_col.getDataColumn();
        const auto * col_array = checkAndGetColumn<ColumnArray>(&nested);
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type of argument 2 of function {}. Must be Array(UInt64)", name);

        const auto * col_data = checkAndGetColumn<ColumnUInt64>(&col_array->getData());
        if (!col_data)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal nested type of argument 2 of function {}. Must be Array(UInt64)", name);

        const auto & data = col_data->getData();
        size_t arr_end = col_array->getOffsets()[0];

        std::vector<S2CellId> cells;
        cells.reserve(arr_end);
        for (size_t i = 0; i < arr_end; ++i)
        {
            S2CellId cell(data[i]);
            if (!cell.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Covering cell (id {}) is not valid in function {}", data[i], name);
            cells.push_back(cell);
        }

        /// S2CellUnion normalizes: sorts and merges sibling cells.
        S2CellUnion covering(std::move(cells));
        return covering;
    }

    /// Binary-search the sorted covering for any cell whose Hilbert range
    /// overlaps the query cell's range. Returns an iterator to the matching
    /// covering cell, or covering.end() if no intersection is found.
    /// Same algorithm as `coveringIntersectsRange` in `KeyConditionS2.cpp`.
    static decltype(std::declval<const S2CellUnion &>().begin()) coveringIntersectsCell(const S2CellUnion & covering, S2CellId cell)
    {
        /// Find the first covering cell whose range_max >= cell.range_min.
        auto it = std::lower_bound(
            covering.begin(), covering.end(), cell,
            [](S2CellId a, S2CellId b) { return a.range_max() < b.range_min(); });

        if (it != covering.end() && it->range_min() <= cell.range_max())
            return it;
        return covering.end();
    }
};

}

REGISTER_FUNCTION(S2CoveringIntersects)
{
    FunctionDocumentation::Description description = R"(
Internal function used by S2 projection index pruning.
Checks whether a single S2 cell intersects any cell in a covering (array of cell IDs).
    )";
    FunctionDocumentation::Syntax syntax = "__s2CoveringIntersects(cell_id, covering_cells)";
    FunctionDocumentation::Arguments function_arguments = {
        {"cell_id", "S2 cell identifier.", {"UInt64"}},
        {"covering_cells", "Array of S2 cell identifiers representing a covering.", {"Array(UInt64)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1 if the cell intersects any cell in the covering, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{"Basic usage", "SELECT __s2CoveringIntersects(9926595209846587392, [9926594385212866560])", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, function_arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionS2CoveringIntersects>(documentation);
}


}

#endif
