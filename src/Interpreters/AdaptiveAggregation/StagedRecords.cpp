#include <Interpreters/AdaptiveAggregation/StagedRecords.h>

#include <algorithm>

#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

std::unique_ptr<const AdaptiveArgumentLayout>
AdaptiveArgumentLayout::build(const Block & header, const ColumnNumbers & keys_positions, const ColumnNumbersList & aggregates_positions)
{
    auto layout = std::make_unique<AdaptiveArgumentLayout>();
    std::vector<UInt8> staged;
    for (const auto & positions : aggregates_positions)
        for (const auto position : positions)
        {
            if (position >= staged.size())
                staged.resize(position + 1);
            if (staged[position])
                continue;
            staged[position] = 1;
            layout->num_positions = std::max(layout->num_positions, position + 1);

            auto type = recursiveRemoveLowCardinality(header.getByPosition(position).type);
            const auto column = type->createColumn();
            const IColumn * values = column.get();
            if (const auto * nullable = typeid_cast<const ColumnNullable *>(values))
                values = &nullable->getNestedColumn();

            if (values->isFixedAndContiguous())
            {
                /// A single numeric key stores the argument's exact bytes. Other key layouts may pack
                /// several fields or null markers, so their arguments need separate storage.
                if (keys_positions.size() == 1 && position == keys_positions.front() && type->isValueRepresentedByNumber())
                {
                    layout->key_field = AdaptiveArgumentLayout::Field{.position = position, .type = std::move(type)};
                    continue;
                }
                const size_t size = column->sizeOfValueIfFixed();
                layout->fixed_fields.push_back({.position = position, .type = std::move(type), .offset = layout->fixed_bytes, .size = size});
                layout->fixed_bytes += size;
            }
            else
                layout->variable_fields.push_back({.position = position, .type = std::move(type)});
        }
    layout->variable_fields_are_strings = std::ranges::all_of(layout->variable_fields, isStagedString);
    return layout;
}

}
