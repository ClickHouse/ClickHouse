#if WITH_COVERAGE_DEPTH

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/coverage.h>

#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <Common/CoverageCollection.h>
#endif


namespace DB
{

namespace
{

enum class Kind : uint8_t
{
    Files,
    LineStarts,
    LineEnds,
};

/** If ClickHouse is built with coverage instrumentation (WITH_COVERAGE_DEPTH=1), returns arrays
  * of source files / line start numbers / line end numbers covered since the last reset.
  */
class FunctionCoverageLines : public IFunction
{
private:
    Kind kind;

public:
    explicit FunctionCoverageLines(Kind kind_) : kind(kind_) {}

    String getName() const override
    {
        if (kind == Kind::Files) return "coverageCurrentFiles";
        if (kind == Kind::LineStarts) return "coverageCurrentLineStarts";
        return "coverageCurrentLineEnds";
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        if (kind == Kind::Files)
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
#if defined(__ELF__) && !defined(OS_FREEBSD)
        const DB::CurrentCoverageRegions regions = DB::getCurrentCoverageRegions();

        if (kind == Kind::Files)
        {
            auto column = ColumnString::create();
            for (const auto & f : regions.files)
                column->insertData(f.data(), f.size());
            auto offsets = ColumnArray::ColumnOffsets::create(1, regions.files.size());
            auto array = ColumnArray::create(std::move(column), std::move(offsets));
            return ColumnConst::create(std::move(array), input_rows_count);
        }
        if (kind == Kind::LineStarts)
        {
            auto column = ColumnUInt32::create();
            column->getData().insert(regions.line_starts.begin(), regions.line_starts.end());
            auto offsets = ColumnArray::ColumnOffsets::create(1, regions.line_starts.size());
            auto array = ColumnArray::create(std::move(column), std::move(offsets));
            return ColumnConst::create(std::move(array), input_rows_count);
        }
        {
            auto column = ColumnUInt32::create();
            column->getData().insert(regions.line_ends.begin(), regions.line_ends.end());
            auto offsets = ColumnArray::ColumnOffsets::create(1, regions.line_ends.size());
            auto array = ColumnArray::create(std::move(column), std::move(offsets));
            return ColumnConst::create(std::move(array), input_rows_count);
        }
#else
        if (kind == Kind::Files)
        {
            auto column = ColumnString::create();
            auto offsets = ColumnArray::ColumnOffsets::create(1, 0);
            auto array = ColumnArray::create(std::move(column), std::move(offsets));
            return ColumnConst::create(std::move(array), input_rows_count);
        }
        auto column = ColumnUInt32::create();
        auto offsets = ColumnArray::ColumnOffsets::create(1, 0);
        auto array = ColumnArray::create(std::move(column), std::move(offsets));
        return ColumnConst::create(std::move(array), input_rows_count);
#endif
    }
};

}

/// Returns diagnostic info: (profile_data_records, covered_name_refs, coverage_map_size)
class FunctionCoverageDiag : public IFunction
{
public:
    String getName() const override { return "coverageDiag"; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column = ColumnUInt64::create();
        auto & data = column->getData();
#if defined(__ELF__) && !defined(OS_FREEBSD)
        const auto name_refs = getCurrentCoveredNameRefs();
        size_t map_size = DB::getCoverageMapSize();
        size_t matches = DB::countCoverageMatches(name_refs);
        auto [non_empty, zero_line, first_info] = DB::diagCoverageRegions(name_refs);
        data.push_back(static_cast<UInt64>(name_refs.size()));
        data.push_back(static_cast<UInt64>(map_size));
        data.push_back(static_cast<UInt64>(matches));
        data.push_back(static_cast<UInt64>(non_empty));   // regions with non-empty file
        data.push_back(static_cast<UInt64>(zero_line));   // regions with line_start==0
        data.push_back(static_cast<UInt64>(first_info));  // file_len<<32|line_start of first match
#else
        data.push_back(0);
        data.push_back(0);
        data.push_back(0);
        data.push_back(0);
        data.push_back(0);
        data.push_back(0);
#endif
        auto offsets = ColumnArray::ColumnOffsets::create(1, data.size());
        auto array = ColumnArray::create(std::move(column), std::move(offsets));
        return ColumnConst::create(std::move(array), input_rows_count);
    }
};

REGISTER_FUNCTION(CoverageLines)
{
    factory.registerFunction("coverageDiag", [](ContextPtr){ return std::make_shared<FunctionCoverageDiag>(); },
        FunctionDocumentation
        {
            .description = R"(Returns diagnostic counters for the LLVM coverage system as an `Array(UInt64)`: `[name_refs_count, coverage_map_size, matches, non_empty_file_regions, zero_line_regions, first_file_info]`. Only available in `WITH_COVERAGE_DEPTH=1` builds.)",
            .syntax = "coverageDiag()",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });

    factory.registerFunction("coverageCurrentFiles", [](ContextPtr){ return std::make_shared<FunctionCoverageLines>(Kind::Files); },
        FunctionDocumentation
        {
            .description = R"(
This function is only available if ClickHouse was built with the `WITH_COVERAGE_DEPTH=1` option.

Returns an `Array(String)` of source file paths covered since the last `SYSTEM SET COVERAGE TEST` call.

Use together with `coverageCurrentLineStarts` and `coverageCurrentLineEnds` to get the covered line ranges.
)",
            .syntax = "coverageCurrentFiles()",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });

    factory.registerFunction("coverageCurrentLineStarts", [](ContextPtr){ return std::make_shared<FunctionCoverageLines>(Kind::LineStarts); },
        FunctionDocumentation
        {
            .description = R"(
This function is only available if ClickHouse was built with the `WITH_COVERAGE_DEPTH=1` option.

Returns an `Array(UInt32)` of line start numbers parallel to `coverageCurrentFiles`.
)",
            .syntax = "coverageCurrentLineStarts()",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });

    factory.registerFunction("coverageCurrentLineEnds", [](ContextPtr){ return std::make_shared<FunctionCoverageLines>(Kind::LineEnds); },
        FunctionDocumentation
        {
            .description = R"(
This function is only available if ClickHouse was built with the `WITH_COVERAGE_DEPTH=1` option.

Returns an `Array(UInt32)` of line end numbers parallel to `coverageCurrentFiles`.
)",
            .syntax = "coverageCurrentLineEnds()",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });
}

}

#endif
