#include <Processors/Transforms/NestedElementsValidationTransform.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

NestedElementsValidationTransform::NestedElementsValidationTransform(SharedHeader header) : ISimpleTransform(header, header, false)
{
}

void NestedElementsValidationTransform::transform(Chunk & chunk)
{
    Nested::validateArraySizes(getOutputPort().getHeader().cloneWithColumns(chunk.getColumns()));
}

NestedSizesValidationTransform::NestedSizesValidationTransform(SharedHeader header, VectorWithMemoryTracking<NestedSizesCheck> checks_)
    : ISimpleTransform(header, header, false), checks(std::move(checks_))
{
}

namespace
{

/// The array size of every row: the row lengths of an `Array` column or the values of its `size0` subcolumn.
struct ArraySizes
{
    ArraySizes(const IColumn & column, const String & name)
    {
        if (const auto * array = typeid_cast<const ColumnArray *>(&column))
            offsets = &array->getOffsets();
        else if (const auto * column_sizes = typeid_cast<const ColumnUInt64 *>(&column))
            sizes = &column_sizes->getData();
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is neither an array nor array sizes: {}", name, column.dumpStructure());
    }

    UInt64 operator[](size_t row) const { return offsets ? (*offsets)[row] - (*offsets)[row - 1] : (*sizes)[row]; }

    const ColumnArray::Offsets * offsets = nullptr;
    const ColumnUInt64::Container * sizes = nullptr;
};

}

void NestedSizesValidationTransform::transform(Chunk & chunk)
{
    const auto & header = getOutputPort().getHeader();
    const auto & columns = chunk.getColumns();
    const size_t num_rows = chunk.getNumRows();

    auto get_column = [&](const String & name) { return columns[header.getPositionByName(name)]->convertToFullIfWrapped(); };

    for (const auto & check : checks)
    {
        const String & reference_name = check.reference ? *check.reference : check.columns.front();
        const auto reference_column = get_column(reference_name);
        const ArraySizes reference_sizes(*reference_column, reference_name);

        String reference_member = reference_name;
        if (reference_sizes.sizes && reference_member.ends_with(".size0"))
            reference_member.resize(reference_member.size() - strlen(".size0"));

        for (const auto & name : check.columns)
        {
            const auto column = get_column(name);
            const ArraySizes column_sizes(*column, name);

            for (size_t row = 0; row < num_rows; ++row)
            {
                if (column_sizes[row] != reference_sizes[row])
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "Elements '{}' and '{}' of Nested data structure '{}' (Array columns) "
                        "have different array sizes ({} and {} respectively) on row {}",
                        name, reference_member, Nested::splitName(name).first, column_sizes[row], reference_sizes[row], row);
            }
        }
    }
}

}
