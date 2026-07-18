#pragma once

#include "config.h"

#if USE_PARQUET

#include <Processors/ISimpleTransform.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <vector>

namespace DB
{

/// Replaces (or appends) whole columns with constants. Used for DuckLake hive partition
/// columns of name-mapped files, whose values live in the file path (recorded in the
/// catalog) rather than in the parquet content — the reader emits defaults which this
/// transform replaces, before subcolumn extraction.
class DuckLakePartitionConstantsTransform final : public ISimpleTransform
{
public:
    struct ConstantColumn
    {
        String name;
        DataTypePtr type;
        Field value;
    };

    DuckLakePartitionConstantsTransform(
        const SharedHeader & input_header_,
        std::vector<ConstantColumn> constants_);

    String getName() const override { return "DuckLakePartitionConstants"; }

protected:
    void transform(Chunk & chunk) override;

private:
    struct ResolvedConstant
    {
        /// Position of the column in the input header, or npos when it must be appended.
        size_t input_position;
        DataTypePtr type;
        Field value;
    };

    std::vector<ResolvedConstant> constants;

    static SharedHeader makeOutputHeader(
        const SharedHeader & input_header_,
        const std::vector<ConstantColumn> & constants_);
};

}

#endif
